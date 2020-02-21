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

struct AccumulatingState<Key: Eq + Hash, Batcher, Delay> {
    keys: KeySet<Key>,
    batcher: Batcher,
    // If None, we're trying to force an immediate start (duration == 0).
    // TODO: Change this away from an Option if futures-timer#56 is resolved.
    delay: Option<Delay>,
    wakers: WakerSet,
}

impl<Key, Batcher, Delay> Debug for AccumulatingState<Key, Batcher, Delay>
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

// Design notes:
//
// We only need one task to "drive" this future. However, there are two cases
// we need to handle:
// - If a future is dropped, we need to arrange for a different task to
//   continue driving this batch
// - When the batch completes, we need to wake ALL the tasks
#[derive(Debug)]
struct RunningState<Fut> {
    fut: Fut,
    wakers: WakerSet,
    dropped_tokens: Vec<KeyToken>,
}

enum State<Key: Hash + Eq, Value, Error, Fut, Batcher, Delay> {
    Accum(AccumulatingState<Key, Batcher, Delay>),
    Running(RunningState<Fut>),
    Done(Result<ValueSet<Value>, Error>),
}

#[derive(Debug, Clone, Default)]
pub struct BatchRules<Batcher, Delayer> {
    pub batcher: Batcher,
    pub window: Delayer,
    pub max_keys: Option<NonZeroUsize>,
}

pub struct BatchController<Key: Hash + Eq, Value, Error, Fut, Batcher, Delay, Delayer> {
    rules: BatchRules<Batcher, Delayer>,

    // TODO: find a good way to rewrite this type so that this lint passes
    #[allow(clippy::type_complexity)]
    state: Mutex<Weak<Mutex<State<Key, Value, Error, Fut, Batcher, Delay>>>>,
}

impl<'a, Key, Value, Error, Fut, Batcher, Delay, Delayer>
    BatchController<Key, Value, Error, Fut, Batcher, Delay, Delayer>
where
    Key: Eq + Hash,
    Value: Clone,
    Error: Clone,
    Fut: Future<Output = Result<ValueSet<Value>, Error>>,
    Batcher: Clone + Fn(KeySet<Key>) -> Fut,
    Delay: Future<Output = ()>,
    Delayer: Fn() -> Delay,
{
    pub fn new(rules: BatchRules<Batcher, Delayer>) -> Self {
        Self {
            rules,
            state: Mutex::new(Weak::new()),
        }
    }

    pub fn load(&self, key: Key) -> BatchFuture<Key, Value, Error, Fut, Batcher, Delay> {
        let mut guard = self.state.lock().unwrap();

        // If there is an existing state, and it's still in the accum state,
        // add a new key to it. Note that at no point do we check the timing

        if let Some(state_handle) = guard.upgrade() {
            let mut state_guard = state_handle.lock().unwrap();
            if let State::Accum(ref mut state) = *state_guard {
                let key_token = state.keys.add_key(key);

                // If we've hit the key limit:
                // - Clear the timer
                // - Initiate a poll
                // - Detach the shared state from the controller
                match self.rules.max_keys {
                    Some(max_keys) if state.keys.len() >= max_keys.get() => {
                        state.delay = None;
                        // Note: currently we explicitly choose not to do a wake
                        // here. This is because `load` is about to return a
                        // future, and we assume that either the future is about
                        // to be polled, or it will be dropped. In the latter
                        // case it will then wake a random other future in our
                        // wakerset.
                        //
                        // In the event that this implementation changes to
                        // explicitly track the "driving" future, to reduce
                        // spurious wakeups under certain drop orderings,
                        // we will have to add a wakers.wake_any() call in
                        // here.
                        drop(state_guard);
                        *guard = Weak::new();
                    }
                    _ => drop(state_guard),
                }

                return BatchFuture {
                    key_token: Some(key_token),
                    state: state_handle,
                    waker_token: None,
                };
            }
        }

        let mut keys = KeySet::new();
        let key_token = keys.add_key(key);

        let state = Arc::new(Mutex::new(State::Accum(AccumulatingState {
            keys,
            batcher: self.rules.batcher.clone(),
            delay: Some((self.rules.window)()),
            wakers: WakerSet::default(),
        })));

        *guard = Arc::downgrade(&state);

        BatchFuture {
            key_token: Some(key_token),
            waker_token: None,
            state,
        }
    }
}

// Invariant: in order for this future to exist, its key must have been added
// to the state.
pub struct BatchFuture<Key: Hash + Eq, Value, Error, Fut, Batcher, Delay> {
    key_token: Option<KeyToken>,
    waker_token: Option<WakerToken>,

    // TODO: find a good way to rewrite this type so that this lint passes
    #[allow(clippy::type_complexity)]
    state: Arc<Mutex<State<Key, Value, Error, Fut, Batcher, Delay>>>,
}

impl<Key, Value, Error, Fut, Batcher, Delay> Future
    for BatchFuture<Key, Value, Error, Fut, Batcher, Delay>
where
    Key: Eq + Hash,
    Value: Clone,
    Error: Clone,
    Fut: Future<Output = Result<ValueSet<Value>, Error>>,
    Batcher: Clone + Fn(KeySet<Key>) -> Fut,
    Delay: Future<Output = ()>,
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
        let mut guard = unpinned.state.lock().unwrap();

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
                    match unpinned.waker_token.as_ref() {
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
            let mut result = match fut.poll(ctx) {
                Poll::Pending => {
                    // This is now the driving waker for the batch future.
                    // Update the wakerset.
                    match unpinned.waker_token.as_ref() {
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
            let dropped_tokens = mem::take(&mut state.dropped_tokens);
            if let Ok(values) = &mut result {
                dropped_tokens
                    .into_iter()
                    .for_each(move |token| values.discard(token));
            }

            let mut all_wakers = mem::take(&mut state.wakers);

            // We're about to grab our result, so we don't need to wake
            // ourself. It's also entirely possible that we never had a token
            // to begin with
            if let Some(waker_token) = unpinned.waker_token.take() {
                // Don't use discard_and_wake because we're about to wake_all
                all_wakers.discard_waker(waker_token);
            }

            // Now that we have a result, signal all the waiting futures to
            // wake up so they can get their results.
            all_wakers.wake_all();

            // Cleanup is all done; transition the state.
            // Safety note: this is where the future is destructed in place,
            // ensuring the pin contract is upheld.
            *guard = State::Done(result);
        }

        if let State::Done(Ok(ref mut values)) = *guard {
            let token = unpinned.key_token.take().unwrap();
            return Poll::Ready(Ok(values.take(token).unwrap()));
        }

        if let State::Done(Err(ref err)) = *guard {
            return Poll::Ready(Err(err.clone()));
        }

        panic!("BatchFuture contained invalid state");
    }
}

impl<Key: Hash + Eq, Value, Error, Fut, Batcher, Delay> Drop
    for BatchFuture<Key, Value, Error, Fut, Batcher, Delay>
{
    fn drop(&mut self) {
        // An important thing to remember when dropping a BatchFuture:
        // the shared futures used by a collection of BatchFutures are only
        // ever being driven by a single future. Therefore, we have to ensure
        // that another task is awoken to "take over", in case this one was
        // the driver. We store all the wakers of the associated futures, so
        // that the
        let mut guard = self.state.lock().unwrap();

        // TODO: right now, we unconditionally wake another future when we
        // drop. We should track if we're known to be the "active" future
        // to prevent spurious wakeups.
        match *guard {
            State::Accum(ref mut state) => {
                // Deregister ourselves from the KeySet and WakerSet.
                if let Some(waker_token) = self.waker_token.take() {
                    // discard_and_wake ensures that if we were the driving
                    // future, another future will be selected to progress the
                    // shared batch job.
                    state.wakers.discard_and_wake(waker_token);
                }

                if let Some(key_token) = self.key_token.take() {
                    state.keys.discard_token(key_token);
                }
            }
            State::Running(ref mut state) => {
                // Deregister ourselves from the WakerSet.
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
                if let Some(key_token) = self.key_token.take() {
                    state.dropped_tokens.push(key_token)
                }
            }
            State::Done(Ok(ref mut values)) => {
                // Drop our token from the ValueSet
                if let Some(key_token) = self.key_token.take() {
                    values.discard(key_token);
                }
            }
            State::Done(Err(..)) => {}
        }
    }
}

// TODO: Make BatchFuture cloneable. This requires making tokens cloneable,
// which isn't the worst thing, but it does break our ownership model a bit.
