use std::{
    fmt::{self, Debug, Formatter},
    future::Future,
    hash::Hash,
    mem,
    num::NonZeroUsize,
    pin::Pin,
    sync::Mutex,
    sync::{Arc, Weak},
    task::{Context, Poll},
};

use crate::{
    data::{KeySet, Token as KeyToken, ValueSet},
    wakerset::{Token as WakerToken, WakerSet},
};

struct AccumulatingState<'a, Key: Eq + Hash, Batcher, Delay> {
    keys: KeySet<Key>,
    batcher: &'a Batcher,
    delay: Option<Delay>,
    wakers: WakerSet,
}

impl<'a, Key, Batcher, Delay> Debug for AccumulatingState<'a, Key, Batcher, Delay>
where
    Key: Debug + Hash + Eq,
    Delay: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("AccumulatingState")
            .field("keys", &self.keys)
            .field("batcher", &"<closure>")
            .field("delay", &self.delay)
            .field("wakers", &self.wakers)
            .finish()
    }
}

#[derive(Debug)]
struct RunningState<Fut> {
    fut: Fut,
    wakers: WakerSet,
    dropped_tokens: Vec<KeyToken>,
}

enum State<'a, Key: Hash + Eq, Value, Error, Fut, Batcher, Delay> {
    Accum(AccumulatingState<'a, Key, Batcher, Delay>),
    Running(RunningState<Fut>),
    Done(Result<ValueSet<Value>, Error>),
}

// TODO: impl Debug for State

#[derive(Debug, Clone, Default)]
pub struct BatchRules<Batcher, Delayer> {
    pub batcher: Batcher,
    pub window: Delayer,
    pub max_keys: Option<NonZeroUsize>,
}

pub struct BatchController<'a, Key: Hash + Eq, Value, Error, Fut, Batcher, Delay, Delayer> {
    rules: &'a BatchRules<Batcher, Delayer>,

    // TODO: find a good way to rewrite this type so that this lint passes
    // TODO: use arc_swap instead of Mutex<Weak<...>>. The inner mutex should ensure
    // that we can respect our invariants, so it seems like it's mostly a
    // matter of a retry loop?
    #[allow(clippy::type_complexity)]
    state: Mutex<Weak<Mutex<State<'a, Key, Value, Error, Fut, Batcher, Delay>>>>,
}

impl<'a, Key, Value, Error, Fut, Batcher, Delay, Delayer>
    BatchController<'a, Key, Value, Error, Fut, Batcher, Delay, Delayer>
where
    Key: Eq + Hash,
    Value: Clone,
    Error: Clone,
    Delayer: Fn() -> Delay,
    Delay: Future<Output = ()>,
    Batcher: Clone + Fn(KeySet<Key>) -> Fut,
    Fut: Future<Output = Result<ValueSet<Value>, Error>>,
{
    pub fn new(rules: &'a BatchRules<Batcher, Delayer>) -> Self {
        Self {
            rules,
            state: Mutex::new(Weak::new()),
        }
    }

    pub fn load(&self, key: Key) -> BatchFuture<'a, Key, Value, Error, Fut, Batcher, Delay> {
        let mut guard = self.state.lock().unwrap();

        // If there is an existing state, and it's still in the accum state,
        // add a new key to it. Note that at no point do we check the timing;
        // we assume that if our delay window has closed, a future poll will
        // advance the state to Running.
        //
        // If any of these conditions are not true, we instead create a brand
        // new state.

        // Get the current state
        if let Some(state_handle) = guard.upgrade() {
            // Is the current state poisoned?
            let mut state_guard_result = state_handle.lock();
            if let Ok(ref mut state_guard) = state_guard_result {
                // Are we in the accumulating state?
                if let State::Accum(ref mut state) = **state_guard {
                    let key_token = state.keys.add_key(key);

                    // If we've hit the key limit:
                    // - Clear the timer
                    // - Initiate a poll
                    // - Detach the shared state from the controller
                    match self.rules.max_keys {
                        Some(max_keys) if state.keys.len() >= max_keys.get() => {
                            state.delay = None;
                            state.wakers.wake_driver();
                            drop(state_guard_result);
                            *guard = Weak::new();
                        }
                        _ => drop(state_guard_result),
                    }

                    return BatchFuture {
                        key_token,
                        state: Some(state_handle),
                        waker_token: None,
                    };
                }
            }
        }

        let mut keys = KeySet::new();
        let key_token = keys.add_key(key);

        let state = match self.rules.max_keys {
            Some(max_keys) if max_keys.get() <= 1 => {
                Arc::new(Mutex::new(State::Accum(AccumulatingState {
                    keys,
                    batcher: &self.rules.batcher,
                    delay: None,
                    wakers: WakerSet::default(),
                })))
            }
            _ => {
                let state = Arc::new(Mutex::new(State::Accum(AccumulatingState {
                    keys,
                    batcher: &self.rules.batcher,
                    delay: Some((self.rules.window)()),
                    wakers: WakerSet::default(),
                })));

                *guard = Arc::downgrade(&state);
                state
            }
        };

        BatchFuture {
            key_token,
            waker_token: None,
            state: Some(state),
        }
    }
}

pub struct BatchFuture<'a, Key: Hash + Eq, Value, Error, Fut, Batcher, Delay> {
    key_token: KeyToken,
    waker_token: Option<WakerToken>,

    // TODO: find a good way to rewrite this type so that this lint passes
    #[allow(clippy::type_complexity)]
    state: Option<Arc<Mutex<State<'a, Key, Value, Error, Fut, Batcher, Delay>>>>,
}

impl<'a, Key, Value, Error, Fut, Batcher, Delay> Future
    for BatchFuture<'a, Key, Value, Error, Fut, Batcher, Delay>
where
    Key: Eq + Hash,
    Value: Clone,
    Error: Clone,
    Delay: Future<Output = ()>,
    Batcher: Clone + Fn(KeySet<Key>) -> Fut,
    Fut: Future<Output = Result<ValueSet<Value>, Error>>,
{
    type Output = Result<Value, Error>;

    fn poll(self: Pin<&mut Self>, ctx: &mut Context<'_>) -> std::task::Poll<Self::Output> {
        // TODO: find a way to make all of this into an async function. The major friction points
        // are:
        //
        // - Only one future needs to drive this to completion in the Accumulating and Running
        //   states, but all futures need to be notified during the Done state
        // - We need to "leak" the KeySet to the BatchController so that new futures can add
        //   themselves to it. This is challenging if it lives in the stack of an async function.
        let unpinned = Pin::into_inner(self);

        // Note about this mutex: it (should be) safe to use this in an async context, because
        // the lock is released when poll returns (it isn't held between async polls).
        let mut guard = unpinned
            .state
            .as_mut()
            .expect("Can't re-poll a completed BatchFuture")
            .lock()
            // This is where panic propogation happens. If a *different* call to
            // poll (in a different future) resulted in a panic (in particular,
            // if calling batcher or fut.poll panicked), the mutex will be
            // poisoned, which ensures that other polls also panic.
            .unwrap();

        if let State::Accum(ref mut state) = *guard {
            // Check the delay
            if let Some(ref mut delay) = state.delay {
                // Safety: the delay is inside an arc and we don't pull it out.
                // It is destructed in-place at the end of this block if the
                // delay doesn't return Pending.
                let pinned_delay = unsafe { Pin::new_unchecked(delay) };
                if let Poll::Pending = pinned_delay.poll(ctx) {
                    // This waker is now the driving waker for the Delay
                    // future. Update the wakerset.
                    match unpinned.waker_token {
                        Some(token) => state.wakers.replace_waker(token, ctx.waker()),
                        None => {
                            let token = state.wakers.add_waker(ctx.waker().clone());
                            unpinned.waker_token = Some(token);
                        }
                    }
                    return Poll::Pending;
                }
            }

            // Delay is complete. Transition to the Running state.
            let wakers = mem::take(&mut state.wakers);
            let keyset = state.keys.take();

            // This is one of the two places we're most worried about a panic,
            // the other being fut.poll.
            // Safety note: at this point, the future has not yet been pinned
            // and is safe to move around.
            let fut = (state.batcher)(keyset);

            // Safety note: this is where the delay is destructed in place,
            // ensuring the pin contract is upheld.
            *guard = State::Running(RunningState {
                fut,
                wakers,
                dropped_tokens: Vec::new(),
            });
        }

        if let State::Running(ref mut state) = *guard {
            // Check the future
            // Safety: we don't ever move this reference, which is behind an
            // arc
            let fut = unsafe { Pin::new_unchecked(&mut state.fut) };

            // This is the place where we're most afraid of a panic. Right now,
            // this panic is handled by poisoning the shared mutex.
            let mut result = match fut.poll(ctx) {
                Poll::Pending => {
                    // This is now the driving waker for the batch future.
                    // Update the wakerset.
                    match unpinned.waker_token {
                        Some(token) => state.wakers.replace_waker(token, ctx.waker()),
                        None => {
                            let token = state.wakers.add_waker(ctx.waker().clone());
                            unpinned.waker_token = Some(token);
                        }
                    }

                    return Poll::Pending;
                }
                Poll::Ready(result) => result,
            };

            // Some futures may have lost interest while we were in the Running
            // state. Remove those tokens from the ValueSet.
            if let Ok(values) = &mut result {
                state
                    .dropped_tokens
                    .iter()
                    .for_each(move |&token| values.discard(token));
            }

            // Now that we have a result, signal all the waiting futures to
            // wake up so they can get their results.
            match unpinned.waker_token.take() {
                // We're about to grab our result, so we don't need to wake
                // ourself. It's also entirely possible that we never had a token
                // to begin with.
                Some(token) => state.wakers.discard_wake_all(token),
                None => state.wakers.wake_all(),
            }

            // Cleanup is all done; transition the state.
            // Safety note: this is where the future is destructed in place,
            // ensuring the pin contract is upheld.
            *guard = State::Done(result);
        }

        // Take care to prevent mutex poisoning in these cases by explicitly
        // dropping the guard
        if let State::Done(Ok(ref mut values)) = *guard {
            match values.take(unpinned.key_token) {
                None => {
                    drop(guard);
                    panic!("Unknown logic error: no value in ValueSet associated with Token");
                }
                Some(value) => {
                    drop(guard);
                    unpinned.state = None;
                    return Poll::Ready(Ok(value));
                }
            }
        }

        if let State::Done(Err(ref err)) = *guard {
            let err = err.clone();
            drop(guard);
            unpinned.state = None;
            return Poll::Ready(Err(err));
        }

        unreachable!("BatchFuture contained invalid state");
    }
}

impl<'a, Key: Hash + Eq, Value, Error, Fut, Batcher, Delay> Drop
    for BatchFuture<'a, Key, Value, Error, Fut, Batcher, Delay>
{
    fn drop(&mut self) {
        // An important thing to remember when dropping a BatchFuture:
        // the shared futures used by a collection of BatchFutures are only
        // ever being driven by a single task. Therefore, we have to ensure
        // that another task is awoken to "take over", in case this one was
        // the driver. This logic is mostly handled by the WakerSet type.

        // Currently, we don't do any cleanup if the mutex is poisoned. The
        // main issue here is that we don't propogate our WakerSet state
        // correctly; if the driving future panics while being polled, none
        // of the other futures will be notified. There are a few ways to
        // address this:
        // - in the short term, add an extra case here for cleanup if the
        //   mutex is panicked that simply awakens all the tasks (so that they
        //   will propogate the panics)
        // - in the medium term, add a "panicked" state and prevent the
        //   mutex from being poisoned in the first place
        // - alternatively, in the medium term, dispense with the notion of
        //   a "driving future" and just awaken every task every time.
        // For now, we require panic=abort, meaning mutex poisoning shouldn't
        // be possible
        if let Some(state) = self.state.as_mut() {
            if let Ok(mut guard) = state.lock() {
                match *guard {
                    State::Accum(ref mut state) => {
                        if let Some(waker_token) = self.waker_token.take() {
                            // discard_and_wake ensures that if we were the driving
                            // future, another future will be selected to progress the
                            // shared batch job.
                            state.wakers.discard_and_wake(waker_token);
                        }

                        state.keys.discard_token(self.key_token);
                    }
                    State::Running(ref mut state) => {
                        if let Some(waker_token) = self.waker_token.take() {
                            // discard_and_wake ensures that if we were the driving
                            // future, another future will be selected to progress the
                            // shared batch job.
                            state.wakers.discard_and_wake(waker_token);
                        }

                        // We're in the running state, which means that the KeySet is
                        // frozen (owned by the executing future). Add our token to
                        // the list of dropped tokens so that it can be discared from
                        // the ValueSet when it's ready.
                        state.dropped_tokens.push(self.key_token);
                    }

                    State::Done(Ok(ref mut values)) => {
                        // Drop our token from the ValueSet
                        values.discard(self.key_token);
                    }
                    State::Done(Err(..)) => {}
                }
            }
        }
    }
}

// TODO: Make BatchFuture cloneable. This requires making tokens cloneable,
// which isn't the worst thing, but it does break our ownership model a bit.
