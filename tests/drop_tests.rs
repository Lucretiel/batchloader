//! These tests ensure that dropped futures correctly update the shared state
use batchloader::{BatchController, BatchRules, KeySet, ValueSet};
use cooked_waker::{IntoWaker, Wake, WakeRef};
use futures::{executor, future};
use futures_timer::Delay;
use std::{
    future::Future,
    hash::Hash,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
    time::Duration,
};

/// A Waker that does nothing. Used for when we're manually calling poll.
#[derive(Debug, Default, Copy, Clone, IntoWaker)]
struct NoOpWaker;

impl WakeRef for NoOpWaker {
    fn wake_by_ref(&self) {}
}

impl Wake for NoOpWaker {
    fn wake(self) {}
}

/// Testing async function: put a copy of each key in an Rc in the result.
/// This lets us
async fn put_keys_in_rc<T: Copy + Eq + Hash>(keys: KeySet<T>) -> Result<ValueSet<Rc<T>>, ()> {
    Ok(keys.into_values(|key| Rc::new(*key)))
}

/// This test establishes a baseline behavior for our clone counters
#[test]
fn test_simple_drop_after_resolution() {
    let rules = BatchRules {
        batcher: put_keys_in_rc,
        window: || future::ready(()),
        max_keys: None,
    };

    let controller = BatchController::new(&rules);

    let fut1 = controller.load(1);
    let fut2 = controller.load(1);
    let fut3 = controller.load(1);
    let fut4 = controller.load(1);

    let res1 = executor::block_on(fut1).unwrap();

    // At this point, the shared result and our local fut1 result should both
    // have an Rc
    assert_eq!(Rc::strong_count(&res1), 2);

    // Resolving fut2 simply clones the underlying Rc
    let res2 = executor::block_on(fut2).unwrap();
    assert_eq!(Rc::strong_count(&res2), 3);

    // Dropping this future shouldn't change anything
    drop(fut3);
    assert_eq!(Rc::strong_count(&res1), 3);

    // However, dropping our last remaining handle to the state should cause
    // the shared state to be dropped
    drop(fut4);
    assert_eq!(Rc::strong_count(&res1), 2);
}

#[test]
fn test_drop_during_delay() {
    // This controller asserts that precisely the keys 1 and 2 are present in
    // the key set
    let rules = BatchRules {
        batcher: |keys: KeySet<i32>| async {
            assert_eq!(keys.len(), 2);

            let keys_vec: Vec<&i32> = keys.keys().collect();
            assert!(keys_vec.contains(&&1));
            assert!(keys_vec.contains(&&2));

            if false {
                // Needed for the type annotation
                Err(())
            } else {
                Ok(keys.into_values(|key| *key))
            }
        },
        window: || Delay::new(Duration::from_millis(10)),
        max_keys: None,
    };
    let controller = BatchController::new(&rules);

    let waker = NoOpWaker;
    let waker = waker.into_waker();
    let mut ctx = Context::from_waker(&waker);

    let mut fut1 = controller.load(1);
    let fut11 = controller.load(1);
    let fut2 = controller.load(2);
    let fut3 = controller.load(3);

    // This poll initiates the delay. We'll drop futures in this phase, then
    // confirm that the dropped keys weren't in the batched set.
    let poll = Pin::new(&mut fut1).poll(&mut ctx);
    assert_eq!(poll, Poll::Pending);

    drop(fut11);
    drop(fut3);

    let result: i32 = executor::block_on(fut1).unwrap();
    assert_eq!(result, 1);

    let result: i32 = executor::block_on(fut2).unwrap();
    assert_eq!(result, 2);
}

// TODO: Test drop while batcher is running
