use std::{
    future::Future,
    hash::Hash,
    mem,
    pin::Pin,
    sync::Mutex,
    sync::{Arc, Weak},
    task::{Context, Poll, Waker},
};

use futures::{ready, FutureExt, TryFuture, TryFutureExt};
use futures_timer::Delay;

use crate::{
    data::{KeySet, Token as KeyToken, ValueSet},
    wakerset::{Token as WakerToken, WakerSet},
};

struct AccumulatingState<Key, Batcher> {
    keys: KeySet<Key>,
    batcher: Batcher,
    delay: Delay,
    wakers: WakerSet,
}

// Design notes:
//
// We only need one task to "drive" this future. However, there are two cases
// we need to handle:
// - If a future is dropped, we need to arrange for a different task to
//   continue driving this batch
// - When the batch completes, we need to wake ALL the tasks
struct RunningState<Fut> {
    fut: Fut,
    wakers: WakerSet,
    dropped_tokens: Vec<KeyToken>,
}

enum BatchState<Key, Value, Error, Fut, Batcher> {
    Accum(AccumulatingState<Key, Batcher>),
    Running(RunningState<Fut>),
    Done(Result<ValueSet<Value>, Error>),
}

// TODO: Ideally we can make this work with just a single Arc. Several arcs
// makes our implementation simpler but is unnecessary overhead.
struct BatchFuture<Key, Value, Error, Fut, Batcher> {
    key_token: Option<Token>,
    waker_token: Option<Token>,
    state: Arc<Mutex<BatchState<Key, Value, Error, Fut, Batcher>>>,
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

        if let BatchState::Accum(ref mut state) = *guard {
            unpinned.waker = state.wakers.add_waker(ctx.waker().clone());
            ready!(state.delay.poll_unpin(ctx));

            // Delay is complete. Transition to the Running state.
            let wakers = mem::take(&mut state.wakers);
            let keyset = state.keys.take();

            let fut = (state.batcher)(keyset);

            *guard = BatchState::Running(RunningState {
                fut,
                wakers,
                dropped_tokens: Vec::new(),
            });
        }

        if let BatchState::Running(ref mut state) = *guard {}

        if let BatchState::Done(Ok(ref mut values)) = *guard {
            let token = unpinned.token.take().unwrap();
            return Poll::Ready(Ok(values.take(token).unwrap()));
        }

        if let BatchState::Done(Err(ref err)) = *guard {
            return Poll::Ready(Err(err.clone()));
        }

        panic!("BatchFuture contained invalid state");
    }
}
