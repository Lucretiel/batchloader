//! A homegrown dataloader. This was created because the one in crates.io
//! has a proliferation of boxes that makes in unsuitable for references and
//! so on. No caching for now.

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::Debug;
use std::future::Future;
use std::hash::Hash;
use std::mem;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use futures_timer::Delay;

use Poll::{Pending, Ready};

/// An shared pointer to a BatchState (specifically, an Option<Arc<BatchState>>).
/// Contains various helper methods that forward to BatchState.
struct SharedBatchState<
    'a,
    Key: Hash + Eq,
    Value: Clone,
    Error: Clone,
    Load: Fn(KeySet<Key>) -> Fut,
    Fut: Future<Output = Result<ValueSet<Value>, Error>>,
> {
    state: Option<Arc<Mutex<BatchState<'a, Key, Value, Error, Load, Fut>>>>,
}

impl<
        'a,
        Key: Hash + Eq,
        Value: Clone,
        Error: Clone,
        Load: Fn(KeySet<Key>) -> Fut,
        Fut: Future<Output = Result<ValueSet<Value>, Error>>,
    > SharedBatchState<'a, Key, Value, Error, Load, Fut>
{
    // TODO: several different concerns are represented among the methods here.
    // Split up SharedBatchState into several types, each with their own
    // correct and minimal method set. In particular, we have:
    //
    // - Methods related to the correct operation of the future, which doesn't
    // need the ability to add keys to the state
    // - Methods related to the correct creation of new futures, which doesn't
    // need the ability to do polling
    //
    // Relatedly, the BatchState held by Dispatcher should probably be a Weak
    // pointer, anyway.

    /// Poll this state. Panics if the pointer has been nulled. If the poll
    /// returns ready; the state is discarded; this ensures we don't
    /// attempt to poll the ValueSet with a key it doesn't have, which in turn
    /// means that that method is allowed to assume that all requested keys
    /// definitely exist.
    fn poll_token(&mut self, ctx: &mut Context, token: Token) -> Poll<Result<Value, Error>> {
        // Note that this lock only exists for the duration of a poll, not an
        // entire await, and polls by definition are very quick (so as to be
        // nonblocking). We assume that whatever async runtime we're using
        // doesn't have a lot of threads, and mutexes are generally very fast
        // in a low contention environment, so this should be fine.
        //
        // The main way this goes bad is if there are any panics. All the
        // panics this library can emit are well-defined as logic errors– for
        // instance, polling a completed future, trying to send too many key
        // into a BatchState, etc.
        let state_lock = self
            .state
            .expect("Can't poll a completed BatchFuture")
            .lock()
            .unwrap();

        match state_lock.poll_token(ctx, token) {
            Pending => Pending,
            Ready(result) => {
                self.state = None;
                Ready(result)
            }
        }
    }

    /// Unconditionally create a new BatchState from a key. The future is
    /// returned, and this SharedBatchState is updated to share that future's
    /// state.
    fn add_key_new_state(
        &mut self,
        key: Key,
        load: &'a Load,
        window: Duration,
    ) -> BatchFuture<'a, Key, Value, Error, Load, Fut> {
        let keys = KeySet::new();
        let token = keys.add(key);
        let state = BatchState::new(load, window, keys);
        let arc = Arc::new(Mutex::new(state));

        self.state = Some(arc.clone());
        BatchFuture::new(token, arc)
    }

    /// Attempt to add a key to this Batch State. Several things can happen
    /// here:
    ///
    /// - The state is Accumulating. Add this key to the set of interesting
    ///   keys. If this makes the set full, dispatch it immediately (by
    ///   resetting its local timer).
    /// - The state is either null, or not accumulating. Add create a new state.
    ///
    /// The new SharedBatchState is returned, and can be used to create a new
    /// BatchFuture.
    ///
    /// This function does need to take &mut self, because it will change
    /// the local pointer as needed. In the future hopefully this can be
    /// managed with an Atomic.
    fn add_key(
        &mut self,
        key: Key,
        rules: &'a BatchRules<Key, Value, Error, Load, Fut>,
    ) -> BatchFuture<'a, Key, Value, Error, Load, Fut> {
        use AddKeyResult::*;

        // This take is very imporant when combined with the mutex block
        // futher down. We need to make sure that it's not possible to
        // accidentally add too many keys to BatchState, which can result in
        // panics and widespread mutex poisonings.
        match self.state.take() {
            None => self.add_key_new_state(key, rules.load, rules.max_keys),
            Some(arc) => {
                let state_lock = arc.lock().unwrap();
                match state_lock.add_key(key, rules.max_keys) {
                    Added(token) => {
                        self.state = Some(arc.clone());
                        BatchFuture::new(token, arc)
                    }
                    AddedLast(token) => BatchFuture::new(token, arc),
                    Fail(key) => self.add_key_new_state(key, rules.load, rules.max_keys),
                }
            }
        }
    }
}

/// A batch future is a request for a single Key-Value lookup, which shares
/// its request with several other batch futures such that the request can
/// be Batched as [Keys] -> [Values]. It is created from a `Dispatcher`,
/// and when awaited, it will wait along with its other group of futures until
/// the window has passed, then execute the request and return the Value for
/// the specific key.
pub struct BatchFuture<'a, Key, Value, Error, Load, Fut> {
    token: Token,
    state: SharedBatchState<'a, Key, Value, Error, Load, Fut>,
}

impl<'a, Key, Value, Error, Load, Fut> BatchFuture<'a, Key, Value, Error, Load, Fut>
where
    Key: Hash + Eq,
    Value: Clone,
    Error: Clone,
    Load: Fn(KeySet<Key>) -> Fut,
    Fut: Future<Output = Result<ValueSet<Value>, Error>>,
{
    /// Note: make sure the BatchState invariants are upheld before calling
    /// this method. In paricular, each BatchFuture is guaranteed by the
    /// contract of this library to have an associated key in the BatchState.
    fn new(token: Token, state: Arc<Mutex<BatchState<'a, Key, Value, Error, Load, Fut>>>) {
        Self {
            token,
            state: Some(state),
        }
    }
}

impl<'a, Key, Value, Error, Load, Fut> Future for BatchFuture<'a, Key, Value, Error, Load, Fut>
where
    Key: Hash + Eq,
    Value: Clone,
    Error: Clone,
    Load: Fn(KeySet<Key>) -> Fut,
    Fut: Future<Output = Result<ValueSet<Value>, Error>>,
{
    type Output = Result<Value, Error>;

    fn poll(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
        match self.state.poll_token(self.token) {
            Pending => Pending,
            Ready(result) => {
                self.state.reset();
                result
            }
        }
    }
}

/// A set of configuration rules for a batcher. This defines the batch loading
/// async fn, as well as the durating of time to wait for keys
pub struct BatchRules<
    Key: Hash,
    Value: Clone,
    Error: Clone,
    Load: Fn(HashMap<Key, u32>) -> Fut,
    Fut: Future<Output = Result<HashMap<Key, Value>, Error>>,
> {
    max_keys: u32,
    window: Duration,
    load: Load,
}

impl<
        Key: Hash,
        Value: Clone,
        Error: Clone,
        Load: Fn(HashMap<Key, u32>) -> Fut,
        Fut: Future<Output = Result<HashMap<Key, Value>, Error>>,
    > BatchRules<Key, Value, Error, Load, Fut>
{
    fn new(max_keys: usize, window: Duration, load: Load) -> Self {
        Self {
            max_keys,
            window,
            load,
        }
    }

    fn dispatcher<'a>(&'a self) -> Dispatcher<'a, Key, Value, Error, Load, Fut> {
        Dispatcher {
            rules: self,
            state: Mutex::new(SharedBatchState { state: None }),
        }
    }
}

/// A dispatcher is the entry point for creating BatchFutures. It maintains
/// a "currently accumulating" state, and each time you call Dispatcher::load,
/// the key is added to that state, until:
///
/// - the states accumulation timer runs out (this is usually very short)
/// - the state reaches its maxumum keys
///
/// At this point the state will be "launched"– that is, it will be detached
/// from this dispatcher and replaced with a fresh ones. The futures associated
/// with the old state share ownership of it and drive it to completion,
/// independent of the dispatcher.
pub struct Dispatcher<
    'a,
    Key: Hash + Eq,
    Value: Clone,
    Error: Clone,
    Load: Fn(HashMap<Key, u32>) -> Fut,
    Fut: Future<Output = Result<HashMap<Key, Value>, Error>>,
> {
    rules: &'a BatchRules<Key, Value, Error, Load, Fut>,

    // TODO: replace this with an atomic pointer. Also, probably make it weak?
    // If all the futures drop their state references, there's no reason for
    // dispatcher to keep it around.
    state: Mutex<SharedBatchState<'a, Key, Value, Error, Load, Fut>>,
}

impl<
        'a,
        Key: Hash + Eq,
        Value: Clone,
        Error: Clone,
        Load: Fn(HashMap<Key, u32>) -> Fut,
        Fut: Future<Output = Result<HashMap<Key, Value>, Error>>,
    > Dispatcher<'a, Key, Value, Error, Load, Fut>
{
    fn load(&self, key: Key) -> BatchFuture<'a, Key, Value, Error, Load, Fut> {
        let state_lock = self.state.lock().unwrap();

        state_lock.add_key(key, self.rules)
    }
}
