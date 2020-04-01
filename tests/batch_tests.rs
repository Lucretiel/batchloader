//! These tests are intended to ensure that a batch function is called the
//! correct number of times for different configurations

use batchloader::{BatchController, BatchRules, KeySet, ValueSet};
use cooked_waker::{IntoWaker, Wake, WakeRef};
use crossbeam;
use futures::{executor, future, FutureExt};
use futures_timer::Delay;
use std::{
    num::NonZeroUsize,
    sync::atomic::{AtomicUsize, Ordering},
    task::{Context, Poll},
    thread,
    time::Duration,
};

async fn stringify(keys: KeySet<usize>) -> Result<ValueSet<String>, ()> {
    Ok(keys.into_values(|value| value.to_string()))
}

fn call_counter<'a, T, R>(
    counter: &'a AtomicUsize,
    function: impl Clone + Fn(T) -> R + 'a,
) -> impl Clone + Fn(T) -> R + 'a {
    move |argument| {
        counter.fetch_add(1, Ordering::SeqCst);
        function(argument)
    }
}

#[test]
fn simple_test() {
    let counter = AtomicUsize::new(0);

    let rules = BatchRules {
        window: move || future::ready(()),
        max_keys: None,
        batcher: call_counter(&counter, stringify),
    };

    let controller = BatchController::new(&rules);

    let fut1 = controller.load(10);
    let fut2 = controller.load(20);

    let res1 = executor::block_on(fut1);
    let res2 = executor::block_on(fut2);

    assert_eq!(res1.unwrap(), "10");
    assert_eq!(res2.unwrap(), "20");
    assert_eq!(counter.load(Ordering::SeqCst), 1);
}

#[test]
fn low_key_test() {
    let counter = AtomicUsize::new(0);

    let rules = BatchRules {
        window: || future::ready(()),
        max_keys: NonZeroUsize::new(2),
        batcher: call_counter(&counter, stringify),
    };

    let controller = BatchController::new(&rules);

    let fut1 = controller.load(10);
    let fut2 = controller.load(20);
    let fut3 = controller.load(30);

    let res1 = executor::block_on(fut1);
    let res2 = executor::block_on(fut2);
    let res3 = executor::block_on(fut3);

    assert_eq!(res1.unwrap(), "10");
    assert_eq!(res2.unwrap(), "20");
    assert_eq!(res3.unwrap(), "30");
    assert_eq!(counter.load(Ordering::SeqCst), 2);
}

#[test]
fn test_duplicate_keys() {
    let counter = AtomicUsize::new(0);

    let rules = BatchRules {
        window: || future::ready(()),
        max_keys: None,
        batcher: call_counter(&counter, stringify),
    };

    let controller = BatchController::new(&rules);

    let fut1 = controller.load(10);
    let fut2 = controller.load(10);
    let fut3 = controller.load(10);

    // Note that the task is launched *immediately* when the max_keys is
    // reached; there's no opportunity to add additional duplicates.
    let fut4 = controller.load(20);

    let res1 = executor::block_on(fut1);
    let res2 = executor::block_on(fut2);
    let res3 = executor::block_on(fut3);
    let res4 = executor::block_on(fut4);

    assert_eq!(res1.unwrap(), "10");
    assert_eq!(res2.unwrap(), "10");
    assert_eq!(res3.unwrap(), "10");
    assert_eq!(res4.unwrap(), "20");
    assert_eq!(counter.load(Ordering::SeqCst), 1);
}

/// Spawn several batch futures in different threads, and confirm that a single
/// batch call was made fulfilling all of them
#[test]
fn test_threaded() {
    let counter = AtomicUsize::new(0);

    let rules = BatchRules {
        window: || Delay::new(Duration::from_millis(10)),
        max_keys: None,
        batcher: call_counter(&counter, stringify),
    };

    let controller = BatchController::new(&rules);
    let controller_ref = &controller;

    let result: Vec<String> = crossbeam::scope(move |s| {
        let threads: Vec<_> = (0..4)
            .map(move |i| {
                s.spawn(move |_s| {
                    thread::sleep(Duration::from_millis(i + 2));
                    let fut = controller_ref.load(i as usize);
                    let result = executor::block_on(fut);
                    result.unwrap()
                })
            })
            .collect();

        let result: Vec<String> = threads.into_iter().map(|t| t.join().unwrap()).collect();
        result
    })
    .unwrap();

    assert_eq!(result, &["0", "1", "2", "3"]);
    assert_eq!(counter.load(Ordering::SeqCst), 1);
}

/// A Waker that does nothing. Used for when we're manually calling poll.
#[derive(Debug, Default, Copy, Clone, IntoWaker)]
struct NoOpWaker;

impl WakeRef for NoOpWaker {
    fn wake_by_ref(&self) {}
}

impl Wake for NoOpWaker {
    fn wake(self) {}
}

#[test]
fn test_key_limit_instant_trigger() {
    let rules = BatchRules {
        batcher: |keys: KeySet<usize>| stringify(keys),
        window: || future::pending(),
        max_keys: NonZeroUsize::new(3),
    };

    let controller = BatchController::new(&rules);

    let waker = NoOpWaker;
    let waker = waker.into_waker();
    let mut ctx = Context::from_waker(&waker);

    let mut fut1 = controller.load(1);
    assert_eq!(fut1.poll_unpin(&mut ctx), Poll::Pending);

    let mut fut2 = controller.load(2);
    assert_eq!(fut2.poll_unpin(&mut ctx), Poll::Pending);

    // Reusing a key means we won't yet be at the key limit
    let mut fut11 = controller.load(1);
    assert_eq!(fut11.poll_unpin(&mut ctx), Poll::Pending);

    let mut fut3 = controller.load(3);

    assert_eq!(
        fut3.poll_unpin(&mut ctx),
        Poll::Ready(Ok(String::from("3"))),
    );
    assert_eq!(
        fut1.poll_unpin(&mut ctx),
        Poll::Ready(Ok(String::from("1"))),
    );
    assert_eq!(
        fut11.poll_unpin(&mut ctx),
        Poll::Ready(Ok(String::from("1"))),
    );
    assert_eq!(
        fut2.poll_unpin(&mut ctx),
        Poll::Ready(Ok(String::from("2"))),
    );
}
