use std::{
    fmt::{self, Debug, Formatter},
    future::Future,
    hash::Hash,
    mem,
    num::NonZeroUsize,
    pin::Pin,
    sync::Arc,
    sync::Mutex,
    task::{Context, Poll},
};

use arc_swap::{ArcSwap, ArcSwapAny};

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

impl<'a, Key: Eq + Hash, Batcher, Delay> AccumulatingState<'a, Key, Batcher, Delay> {
    /// Check if we're under the key limit
    #[inline]
    fn can_add_key(&self, max_keys: Option<NonZeroUsize>) -> bool {
        match max_keys {
            Some(max_keys) => self.keys.len() < max_keys.get(),
            None => true,
        }
    }
}

impl<'a, Key, Batcher, Delay> Debug for AccumulatingState<'a, Key, Batcher, Delay>
where
    Key: Debug + Hash + Eq,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("AccumulatingState")
            .field("keys", &self.keys)
            .field("batcher", &"<async fn>")
            .field("delay", &"<Future>")
            .field("wakers", &self.wakers)
            .finish()
    }
}

struct RunningState<Fut> {
    fut: Fut,
    wakers: WakerSet,
    dropped_tokens: Vec<KeyToken>,
}

impl<Fut> Debug for RunningState<Fut> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("RunningState")
            .field("fut", &"<Future>")
            .field("wakers", &self.wakers)
            .field("dropped_tokens", &self.dropped_tokens)
            .finish()
    }
}

enum State<'a, Key: Hash + Eq, Value, Error, Fut, Batcher, Delay> {
    Accum(AccumulatingState<'a, Key, Batcher, Delay>),
    Running(RunningState<Fut>),
    Done(Result<ValueSet<Value>, Error>),
}

impl<'a, Key: Hash + Eq, Value, Error, Fut, Batcher, Delay>
    State<'a, Key, Value, Error, Fut, Batcher, Delay>
{
    /// Call this method when a BatchFuture is being dropped to ensure its
    /// waker is correctly discarded
    fn discard_waker_token(&mut self, token: WakerToken) {
        match self {
            State::Accum(state) => state.wakers.discard_and_wake(token),
            State::Running(state) => state.wakers.discard_and_wake(token),
            State::Done(..) => {}
        }
    }

    /// Call this method when a BatchFuture is being dropped to ensure its
    /// key token is correctly discarded
    fn discard_key_token(&mut self, token: KeyToken) {
        match self {
            State::Accum(state) => state.keys.discard_token(token),
            State::Running(state) => state.dropped_tokens.push(token),
            State::Done(Ok(values)) => values.discard(token),
            State::Done(Err(..)) => {}
        }
    }
}

impl<'a, Key, Value, Error, Fut, Batcher, Delay> Debug
    for State<'a, Key, Value, Error, Fut, Batcher, Delay>
where
    Key: Hash + Eq + Debug,
    Value: Debug,
    Error: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        use State::*;

        match self {
            Accum(state) => f.debug_tuple("State::Accum").field(state).finish(),
            Running(state) => f.debug_tuple("State::Running").field(state).finish(),
            Done(result) => f.debug_tuple("State::Done").field(result).finish(),
        }
    }
}

// TODO: impl Debug for State

type StateHandle<'a, Key, Value, Error, Fut, Batcher, Delay> =
    Arc<Mutex<State<'a, Key, Value, Error, Fut, Batcher, Delay>>>;

fn new_state_handle<'a, Key, Value, Error, Fut, Batcher, Delay>(
    batcher: &'a Batcher,
) -> StateHandle<'a, Key, Value, Error, Fut, Batcher, Delay>
where
    Key: Hash + Eq,
{
    Arc::new(Mutex::new(State::Accum(AccumulatingState {
        keys: KeySet::new(),
        batcher,
        delay: None,
        wakers: WakerSet::new(),
    })))
}

/// BatchRules is a simple struct that configures a [`BatchController`]; see
/// its docs for more details. Because this struct is taken by reference, it
/// can be intialized as a const static in order to support async frameworks
/// that require 'static Futures.
#[derive(Clone, Copy, Default)]
pub struct BatchRules<Batcher, Delayer> {
    /// An async function that performs the batch work, transforming a set of
    /// keys into a set of values
    pub batcher: Batcher,
    /// An async function that defines the window during which keys can be
    /// added to the shared state of a BatchController. This should be a short
    /// timer, or a function that waits for a small number of frames in the
    /// runtime. Alternatively, if it is a future that never completes, the
    /// controller will continue to accumulate keys until max_keys is reached.
    pub window: Delayer,
    /// The maximum number of keys that will be requested. If None, unlimited
    /// keys will be accepted.
    pub max_keys: Option<NonZeroUsize>,
}

impl<Batcher, Delayer> Debug for BatchRules<Batcher, Delayer> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("BatchRules")
            .field("batcher", &"<async fn>")
            .field("window", &"<async fn>")
            .field("max_keys", &self.max_keys)
            .finish()
    }
}

pub struct BatchController<'a, Key: Hash + Eq, Value, Error, Fut, Batcher, Delay, Delayer> {
    rules: &'a BatchRules<Batcher, Delayer>,

    // TODO: Make this a Weak ptr. Currently Weak ptr support in ArcSwap is
    // unstable, and atomic weaks are a bit more awkward to work with,
    state: ArcSwapAny<StateHandle<'a, Key, Value, Error, Fut, Batcher, Delay>>,
}

impl<'a, Key, Value, Error, Fut, Batcher, Delay, Delayer> Debug
    for BatchController<'a, Key, Value, Error, Fut, Batcher, Delay, Delayer>
where
    Key: Hash + Eq + Debug,
    Value: Debug,
    Error: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("BatchController")
            .field("rules", self.rules)
            .field("state", &self.state)
            .finish()
    }
}

impl<'a, Key, Value, Error, Fut, Batcher, Delay, Delayer>
    BatchController<'a, Key, Value, Error, Fut, Batcher, Delay, Delayer>
where
    Key: Eq + Hash,
    Value: Clone,
    Error: Clone,
    Delayer: Fn() -> Delay,
    Delay: Future<Output = ()>,
    Batcher: Fn(KeySet<Key>) -> Fut,
    Fut: Future<Output = Result<ValueSet<Value>, Error>>,
{
    pub fn new(rules: &'a BatchRules<Batcher, Delayer>) -> Self {
        Self {
            rules,
            state: ArcSwap::new(new_state_handle(&rules.batcher)),
        }
    }

    /// Add a key to the current batch, and return a Future that will be
    /// completed with the result for that specific key. A new batch will
    /// automatically be created if necessary.
    ///
    /// If that key is the *first* key to be added to the current batch, the
    /// `window` function in this controller's `BatchRules` will be called to
    /// create a delay controlling the window during which more keys may be
    /// added to this batch. This delay is a future and is polled by the
    /// future returned by this function.
    ///
    /// If that key is the *last* key to be added to the current batch (as
    /// determined by the `max_key` field in the `BatchRules`), the delay
    /// window will be cleared, which will cause the next poll to immediately
    /// initiate the batch.
    ///
    /// See [`BatchFuture`] for more details on how polling the individual
    /// futures controls the execution of the batch, and see the top level
    /// [`batchloader`][crate] documentation for a high-level overview of running
    /// batch jobs.
    pub fn load(&self, key: Key) -> BatchFuture<'a, Key, Value, Error, Fut, Batcher, Delay> {
        loop {
            let current_state = self.state.load();

            // If the current state is:
            // - not poisoned
            // - in the Accumulating state
            // - not at the max keys
            // then add a key to it.
            // otherwise, create a new state and retry.
            {
                let mut state_guard_result = current_state.lock();
                if let Ok(ref mut state_guard) = state_guard_result {
                    if let State::Accum(ref mut state) = **state_guard {
                        // Make sure we're within the key limit. If we're at or over the key limit,
                        // that means that a previous load call with the same current_state inserted
                        // the final key. This happened after we loaded the state pointer but
                        // before we locked the mutex, and before a poll transitioned the state out
                        // of accumulating.
                        if state.can_add_key(self.rules.max_keys) {
                            // If this is the very first key, set the window for this batch of keys
                            if state.keys.len() == 0 {
                                // Panic safety note: if this panics, it will poison the mutex.
                                // This is fine because no other futures are currently sharing this
                                // state.
                                state.delay = Some((self.rules.window)());
                            }

                            // Insert the key.
                            let key_token = state.keys.add_key(key);

                            // If we are now at the key limit, reset the delay so that the future
                            // will execute the batch immediately when polled.
                            if !state.can_add_key(self.rules.max_keys) {
                                // Pin safety note: If there is a future here,
                                // it is dropped in-place.
                                state.delay = None;

                                // We need to signal the current driving waker,
                                // because the new future we're about to create
                                // doesn't yet have a waker token, which means
                                // if it is dropped before it is polled, it
                                // won't signal another waker that the delay
                                // has been changed.
                                state.wakers.wake_driver();
                            }

                            drop(state_guard_result);

                            return BatchFuture {
                                key_token,
                                state: Some(arc_swap::Guard::into_inner(current_state)),
                                waker_token: None,
                            };
                        }
                    }
                }
            }

            // At this point, the current self.state is not eligible to have a
            // new key added to it for whatever reason, so we need a new one.
            // We create an empty one in the Accumulating state, then loop
            // back to the beginning to add a key to it.

            self.state
                .compare_and_swap(current_state, new_state_handle(&self.rules.batcher));

            // It actually isn't necessary to compare current and prev. There
            // are two possibilities:
            // - If they match, that means the insertion was a success. We
            //   redo the loop to actually insert this key into the state.
            //   we do it this way so that we can use the state's mutex to
            //   ensure the key is inserted without retries (so that we don't
            //   need cloneable keys).
            // - If they do not match, there was a replacement in another
            //   thread, which means we need to redo the loop anyway with
            //   the new state.
        }
    }
}

pub struct BatchFuture<'a, Key: Hash + Eq, Value, Error, Fut, Batcher, Delay> {
    key_token: KeyToken,
    state: Option<StateHandle<'a, Key, Value, Error, Fut, Batcher, Delay>>,
    waker_token: Option<WakerToken>,
}

impl<'a, Key, Value, Error, Fut, Batcher, Delay> Debug
    for BatchFuture<'a, Key, Value, Error, Fut, Batcher, Delay>
where
    Key: Hash + Eq + Debug,
    Value: Debug,
    Error: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("BatchFuture")
            .field("key_token", &self.key_token)
            .field("waker_token", &self.waker_token)
            .field("state", &self.state)
            .finish()
    }
}

impl<'a, Key, Value, Error, Fut, Batcher, Delay> Future
    for BatchFuture<'a, Key, Value, Error, Fut, Batcher, Delay>
where
    Key: Eq + Hash,
    Value: Clone,
    Error: Clone,
    Delay: Future<Output = ()>,
    Batcher: Fn(KeySet<Key>) -> Fut,
    Fut: Future<Output = Result<ValueSet<Value>, Error>>,
{
    type Output = Result<Value, Error>;

    fn poll(self: Pin<&mut Self>, ctx: &mut Context<'_>) -> std::task::Poll<Self::Output> {
        // TODO: see if we can move this to a more lock-free design; see
        // futures::Shared for inspiration. This would allow us to move away
        // from the "driving future" concept, because several simultaneous polls
        // would all skip doing any work with only one actually accessing the
        // critical section.

        // TODO: find a way to make all of this into an async function. The major friction points
        // are:
        //
        // - Only one future needs to drive this to completion in the Accumulating and Running
        //   states, but all futures need to be notified during the Done state. This (probably)
        //   means we need manual control over the context.
        // - We need to "leak" the KeySet to the BatchController so that new futures can add
        //   themselves to it. This is challenging if it lives in the stack of an async function.
        let unpinned = Pin::into_inner(self);

        let mut guard = unpinned
            .state
            .as_mut()
            .expect("Can't re-poll a completed BatchFuture")
            .lock()
            .unwrap();

        // Technically, the logic here is better expressed as a series of waterfalling ifs:
        //
        // if state1 => { maybe return pending; else set state2}
        // if state2 => { maybe return pending; else set state3}
        // if state3 => { return ready }
        //
        // However, it's preferable to have the compile-time guarantees of exhaustiveness, so we
        // use this loop over a match instead.
        loop {
            match *guard {
                State::Accum(ref mut state) => {
                    // Check the delay
                    if let Some(ref mut delay) = state.delay {
                        // Safety: the delay is inside an arc and we don't pull it out.
                        // It is destructed in-place at the end of this block (or by
                        // the BatchController) if the delay doesn't return Pending.
                        // TODO: use pin_utils
                        let pinned_delay = unsafe { Pin::new_unchecked(delay) };
                        // TODO: Panic check here. Right now we just require abort-on-panic.
                        if let Poll::Pending = pinned_delay.poll(ctx) {
                            // This waker is now the driving waker for the Delay
                            // future. Update the wakerset.
                            state
                                .wakers
                                .update_waker(ctx.waker(), &mut unpinned.waker_token);
                            break Poll::Pending;
                        }
                    }

                    // Delay is complete. Transition to the Running state.
                    let wakers = mem::take(&mut state.wakers);
                    let keyset = state.keys.take();

                    // Safety note: at this point, the future has not yet been pinned
                    // and is safe to move around.
                    // TODO: Panic check here. Right now we just require abort-on-panic.
                    let fut = (state.batcher)(keyset);

                    // Safety note: this is where the delay is destructed in place,
                    // ensuring the pin contract is upheld.
                    // Additionally, this is where fut is moved to the location where
                    // it will later be pinned.
                    *guard = State::Running(RunningState {
                        fut,
                        wakers,
                        dropped_tokens: Vec::new(),
                    });
                }

                State::Running(ref mut state) => {
                    // Check the future
                    // Safety: we don't ever move this reference, which is behind an
                    // arc
                    let fut = unsafe { Pin::new_unchecked(&mut state.fut) };

                    // TODO: Panic check here. Right now we just require abort-on-panic.
                    let mut result = match fut.poll(ctx) {
                        Poll::Pending => {
                            // This is now the driving waker for the batch future.
                            // Update the wakerset.
                            state
                                .wakers
                                .update_waker(ctx.waker(), &mut unpinned.waker_token);
                            break Poll::Pending;
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
                    match unpinned.waker_token {
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

                State::Done(Ok(ref mut values)) => {
                    // TODO: Panic check here. Right now we just require abort-on-panic.
                    let success_value = values.take(unpinned.key_token);

                    // Take care to prevent mutex poisoning by explicitly dropping the
                    // mutex guard before checking the option
                    drop(guard);
                    unpinned.state = None;

                    break Poll::Ready(Ok(success_value.expect(
                        "Unknown logic error: no value in ValueSet associated with Token",
                    )));
                }

                State::Done(Err(ref err)) => {
                    let err = err.clone();
                    drop(guard);
                    unpinned.state = None;
                    break Poll::Ready(Err(err));
                }
            }
        }
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
        //   a "driving future" and just awaken every task every time. We'd
        //   like to avoid this if possible because it over-polls the batch
        //   job and probably results in mutex contention as well.
        // For now, we require panic=abort, meaning mutex poisoning shouldn't
        // be possible
        if let Some(state) = self.state.as_mut() {
            if let Ok(mut guard) = state.lock() {
                if let Some(waker_token) = self.waker_token.take() {
                    guard.discard_waker_token(waker_token);
                }
                guard.discard_key_token(self.key_token);
            }
        }
    }
}
