//! These tests ensure that, when a driving future is dropped, another future
//! is notified.

use batchloader::{BatchController, BatchRules, KeySet};
use cooked_waker::{IntoWaker, Wake, WakeRef};
use futures::FutureExt;
use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    task::{Context, Poll, Waker},
    thread::sleep,
    time::Duration,
};

/// A waker that stores true if it has been awoken
#[derive(Debug, Clone, Default, IntoWaker)]
struct BoolWaker {
    cell: Arc<AtomicBool>,
}

impl BoolWaker {
    fn reset(&self) {
        self.cell.store(false, Ordering::SeqCst)
    }

    fn is_signaled(&self) -> bool {
        self.cell.load(Ordering::SeqCst)
    }
}

impl WakeRef for BoolWaker {
    fn wake_by_ref(&self) {
        self.cell.store(true, Ordering::SeqCst)
    }
}

impl Wake for BoolWaker {}

/// A future wrapper that returns pending the first N times it is polled, then
/// returns Ready. We use it to test different control flow variations.
///
/// It immediately calls Wake when it's polled in the pending state, but the
/// intended use of this struct is for a "manually" polled future so that we
/// can test differet sequences of futures being added, dropped, and polled
#[derive(Debug, Clone)]
struct Skipper {
    remaining_skips: usize,
}

impl Skipper {
    fn new(count: usize) -> Self {
        Skipper {
            remaining_skips: count,
        }
    }
}

impl Future for Skipper {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        match &mut self.get_mut().remaining_skips {
            0 => Poll::Ready(()),
            skips => {
                *skips -= 1;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}

struct Task<F: Future + Unpin> {
    fut: F,
    signal: BoolWaker,
    waker: Waker,
}

impl<F: Future + Unpin> Task<F> {
    fn new(fut: F) -> Self {
        let signal = BoolWaker::default();

        Task {
            fut,
            waker: signal.clone().into_waker(),
            signal,
        }
    }

    fn poll(&mut self) -> Poll<F::Output> {
        self.signal.reset();
        self.fut.poll_unpin(&mut Context::from_waker(&self.waker))
    }

    fn is_signaled(&self) -> bool {
        self.signal.is_signaled()
    }
}

#[test]
fn test_notify_lifecycle() {
    let controller = BatchController::new(BatchRules {
        window: Duration::from_millis(1),
        max_keys: None,
        batcher: |keys: KeySet<i32>| async {
            Skipper::new(1).await;

            if false {
                Err(())
            } else {
                Ok(keys.into_values(|key| *key))
            }
        },
    });

    let mut task1 = Task::new(controller.load(1));
    let mut task2 = Task::new(controller.load(2));
    let mut task3 = Task::new(controller.load(3));

    // Polling the futures initiates the timer
    assert_eq!(task3.poll(), Poll::Pending);
    assert_eq!(task2.poll(), Poll::Pending);
    assert_eq!(task1.poll(), Poll::Pending);

    // At this pointer, the timer has started, and should still be running.
    // None of the futures have been signaled. After 1 ms, signal1 (and ONLY
    // signal 1) should have been signaled
    assert!(!task1.is_signaled());
    assert!(!task2.is_signaled());
    assert!(!task3.is_signaled());

    sleep(Duration::from_millis(10));

    assert!(task1.is_signaled());
    assert!(!task2.is_signaled());
    assert!(!task3.is_signaled());

    // We re-poll fut1. This triggers the Skipper, which should immediately
    // notifiy signal1. A second poll should finish the task, which should
    // notify ALL signals.
    assert_eq!(task1.poll(), Poll::Pending);

    assert!(task1.is_signaled());
    assert!(!task2.is_signaled());
    assert!(!task3.is_signaled());

    assert_eq!(task1.poll(), Poll::Ready(Ok(1)));

    assert!(task2.is_signaled());
    assert!(task3.is_signaled());

    assert_eq!(task2.poll(), Poll::Ready(Ok(2)));
    assert_eq!(task3.poll(), Poll::Ready(Ok(3)));
}

#[test]
fn test_notify_lifecycle_drops() {
    let controller = BatchController::new(BatchRules {
        window: Duration::from_millis(1),
        max_keys: None,
        batcher: |keys: KeySet<i32>| async {
            Skipper::new(1).await;

            if false {
                Err(())
            } else {
                Ok(keys.into_values(|key| *key))
            }
        },
    });

    let mut tasks: HashMap<i32, _> = (1..=5)
        .map(|key| (key, Task::new(controller.load(key))))
        .collect();

    // Poll all the tasks. At this point, task #5 is our driver.
    for i in 1..=5 {
        assert_eq!(tasks.get_mut(&i).unwrap().poll(), Poll::Pending);
    }

    // At this pointer, the timer has started, and should still be running.
    // None of the futures have been signaled.
    assert!(tasks.values().all(|task| !task.is_signaled()));

    // We immediately drop the driving task. This should cause another one to
    // be awoken, so that it can do a poll and become the driving task.
    tasks.remove(&5);
    let mut driving_task = None;
    for (&i, task) in tasks.iter() {
        if task.is_signaled() {
            match driving_task {
                None => driving_task = Some(i),
                Some(..) => panic!("Test failure: multiple tasks awoken after drop"),
            }
        }
    }

    let driving_task = driving_task.expect("Test failure: no task was awakened after a drop");

    sleep(Duration::from_millis(10));

    // At this point, the delay has finished, and the driving task should have
    // been signaled.
    for (&i, task) in tasks.iter() {
        if i == driving_task {
            assert!(task.is_signaled());
        } else {
            assert!(!task.is_signaled());
        }
    }

    // Poll the task to initiate the closure. This advances us to the first
    // Skipper.
    assert_eq!(tasks.get_mut(&driving_task).unwrap().poll(), Poll::Pending);

    // Once again, drop that task. This should cause yet another task to be
    // signaled.
    tasks.remove(&driving_task);
    let mut driving_task = None;
    for (&i, task) in tasks.iter() {
        if task.is_signaled() {
            match driving_task {
                None => driving_task = Some(i),
                Some(..) => panic!("Test failure: multiple tasks awoken after drop"),
            }
        }
    }

    let driving_task = driving_task.expect("Test failure: no task was awakened after a drop");

    // Poll that task. This should cause a completion, meaning all other tasks
    // are also done.

    assert_eq!(
        tasks.get_mut(&driving_task).unwrap().poll(),
        Poll::Ready(Ok(driving_task))
    );

    // All other tasks should have been signaled. driving task, having been
    // completed, should NOT have been re-signaled.
    for (&i, task) in tasks.iter() {
        if i == driving_task {
            assert!(!task.is_signaled())
        } else {
            assert!(task.is_signaled())
        }
    }

    tasks.remove(&driving_task);

    for (&i, task) in tasks.iter_mut() {
        assert_eq!(task.poll(), Poll::Ready(Ok(i)));
    }
}
