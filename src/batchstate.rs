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
    time::Duration,
};

use futures::{ready, FutureExt};
use futures_timer::Delay;

use crate::{
    data::{KeySet, Token as KeyToken, ValueSet},
    wakerset::{Token as WakerToken, WakerSet},
};

struct AccumulatingState<Key: Eq + Hash, Batcher> {
    keys: KeySet<Key>,
    batcher: Batcher,
    delay: Delay,
    wakers: WakerSet,
}

impl<Key: Debug + Hash + Eq, Batcher> Debug for AccumulatingState<Key, Batcher> {
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

enum State<Key: Hash + Eq, Value, Error, Fut, Batcher> {
    Accum(AccumulatingState<Key, Batcher>),
    Running(RunningState<Fut>),
    Done(Result<ValueSet<Value>, Error>),
}

pub struct BatchController<Key: Hash + Eq, Value, Error, Fut, Batcher> {
    batcher: Batcher,
    window: Duration,
    max_keys: Option<NonZeroUsize>,
    state: Mutex<Weak<Mutex<State<Key, Value, Error, Fut, Batcher>>>>,
}

impl<Key, Value, Error, Fut, Batcher> BatchController<Key, Value, Error, Fut, Batcher>
where
    Key: Eq + Hash,
    Value: Clone,
    Error: Clone,
    Fut: Future<Output = Result<ValueSet<Value>, Error>>,
    Batcher: Clone + Fn(KeySet<Key>) -> Fut,
{
    pub fn new(max_keys: Option<usize>, window: Duration, batcher: Batcher) -> Self {
        let max_keys = max_keys.map(|max| {
            if max < 2 {
                panic!("Max keys for a BatchController must be at least 2");
            }

            // We could use unsafe new_unchecked here, but why bother when the
            // compiler will fix it anyway
            NonZeroUsize::new(max).unwrap()
        });

        Self {
            batcher,
            window,
            max_keys,
            state: Mutex::new(Weak::new()),
        }
    }

    pub fn load(&self, key: Key) -> BatchFuture<Key, Value, Error, Fut, Batcher> {
        let mut guard = self.state.lock().unwrap();

        // If there is an existing state, and it's still in the accum state,
        // add a new key to it. Note that at no point do we check the timing

        if let Some(state_handle) = guard.upgrade() {
            let mut state_guard = state_handle.lock().unwrap();
            if let State::Accum(ref mut state) = *state_guard {
                let key_token = state.keys.add_key(key);

                // If we've hit the key limit, reset the timer so that the
                // batch is issued immediately, then detach the shared state
                // from the controller.
                match self.max_keys {
                    Some(max_keys) if state.keys.len() >= max_keys.get() => {
                        state.delay.reset(Duration::from_secs(0));
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
            batcher: self.batcher.clone(),
            delay: Delay::new(self.window),
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

// TODO: Ideally we can make this work with just a single Arc. Several arcs
// makes our implementation simpler but is unnecessary overhead.
// Invariant: in order for this future to exist, its key must have been added
// to the state.
pub struct BatchFuture<Key: Hash + Eq, Value, Error, Fut, Batcher> {
    key_token: Option<KeyToken>,
    waker_token: Option<WakerToken>,
    state: Arc<Mutex<State<Key, Value, Error, Fut, Batcher>>>,
}

impl<Key, Value, Error, Fut, Batcher> Future for BatchFuture<Key, Value, Error, Fut, Batcher>
where
    Key: Eq + Hash,
    Value: Clone,
    Error: Clone,
    Fut: Future<Output = Result<ValueSet<Value>, Error>>,
    Batcher: Fn(KeySet<Key>) -> Fut,
{
    type Output = Result<Value, Error>;

    fn poll(self: Pin<&mut Self>, ctx: &mut Context<'_>) -> std::task::Poll<Self::Output> {
        let unpinned = Pin::into_inner(self);
        let mut guard = unpinned.state.lock().unwrap();

        if let State::Accum(ref mut state) = *guard {
            // Add this waker to the WakerSet
            match unpinned.waker_token.as_ref() {
                Some(token) => state.wakers.replace_waker(token, ctx.waker()),
                None => {
                    let token = state.wakers.add_waker(ctx.waker().clone());
                    unpinned.waker_token = Some(token);
                }
            }

            // Check the delay
            ready!(state.delay.poll_unpin(ctx));

            // Delay is complete. Transition to the Running state.
            let wakers = mem::take(&mut state.wakers);
            let keyset = state.keys.take();

            let fut = (state.batcher)(keyset);

            *guard = State::Running(RunningState {
                fut,
                wakers,
                dropped_tokens: Vec::new(),
            });
        }

        if let State::Running(ref mut state) = *guard {
            // Add this waker to the WakerSet
            match unpinned.waker_token.as_ref() {
                Some(token) => state.wakers.replace_waker(token, ctx.waker()),
                None => {
                    let token = state.wakers.add_waker(ctx.waker().clone());
                    unpinned.waker_token = Some(token);
                }
            }

            // Check the future
            // Safety: we don't ever move this reference, which is behind an
            // arc
            let fut = unsafe { Pin::new_unchecked(&mut state.fut) };
            let mut result: Result<ValueSet<Value>, Error> = ready!(fut.poll(ctx));

            // Some futures may have lost interest while we were in the Running
            // state. Remove those tokens from the ValueSet.
            let dropped_tokens = mem::take(&mut state.dropped_tokens);
            if let Ok(values) = &mut result {
                dropped_tokens
                    .into_iter()
                    .for_each(move |token| values.discard(token));
            }

            // Now that we have a result, signal all the waiting futures to
            // wake up so they can get their results.
            let mut all_wakers = mem::take(&mut state.wakers);
            if let Some(waker_token) = unpinned.waker_token.take() {
                // We're about to grab our result, so we don't need to wake
                // ourself
                all_wakers.discard_waker(waker_token);
            }

            all_wakers.wake_all();

            // Cleanup is all done; transition the state
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

impl<Key: Hash + Eq, Value, Error, Fut, Batcher> Drop
    for BatchFuture<Key, Value, Error, Fut, Batcher>
{
    fn drop(&mut self) {
        // An important thing to remember when dropping a BatchFuture:
        // the shared futures used by a collection of BatchFutures are only
        // ever being driven by a single future. Therefore, we have to ensure
        // that another task is awoken to "take over", in case this one was
        // the driver. We store all the wakers of the associated futures, so
        // that the
        let mut guard = self.state.lock().unwrap();

        match *guard {
            State::Accum(ref mut state) => {
                // Deregister ourselves from the KeySet and WakerSet.
                // Additionally, we might have been the "driving" future of
                // the shared state, so wake up another task at random to keep
                // progressing the state.
                if let Some(key_token) = self.key_token.take() {
                    state.keys.discard_token(key_token);
                }

                if let Some(waker_token) = self.waker_token.take() {
                    state.wakers.discard_waker(waker_token);
                    state.wakers.wake_any();
                }
            }
            State::Running(ref mut state) => {
                // Deregister ourselves from the WakerSet.  Additionally, we
                // might have been the "driving" future of the shared state,
                // so wake up another task at random to keep progressing the
                // state.
                if let Some(waker_token) = self.waker_token.take() {
                    state.wakers.discard_waker(waker_token);
                    state.wakers.wake_any();
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
