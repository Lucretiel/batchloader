#![cfg(test)]

use crate::{BatchController, KeySet, ValueSet};
use core::cell::Cell;
use std::time::Duration;

use futures::executor;

async fn stringify(keys: KeySet<usize>) -> Result<ValueSet<String>, ()> {
    Ok(keys.into_values(|value| value.to_string()))
}

fn call_counter<'a, T, R>(
    counter: &'a Cell<usize>,
    function: impl Fn(T) -> R + 'a,
) -> impl Fn(T) -> R + 'a {
    move |argument| {
        counter.set(counter.get() + 1);
        function(argument)
    }
}

#[test]
fn simple_test() {
    let counter = Cell::new(0);
    let f = call_counter(&counter, stringify);

    let controller = BatchController::new(Some(10), Duration::from_secs(0), &f);

    let fut1 = controller.load(10);
    let fut2 = controller.load(20);

    let res1 = executor::block_on(fut1);
    let res2 = executor::block_on(fut2);

    assert_eq!(res1.unwrap(), "10");
    assert_eq!(res2.unwrap(), "20");
    assert_eq!(counter.get(), 1);
}

#[test]
fn low_key_test() {
    let counter = Cell::new(0);
    let f = call_counter(&counter, stringify);

    let controller = BatchController::new(Some(2), Duration::from_secs(0), &f);

    let fut1 = controller.load(10);
    let fut2 = controller.load(20);
    let fut3 = controller.load(30);

    let res1 = executor::block_on(fut1);
    let res2 = executor::block_on(fut2);
    let res3 = executor::block_on(fut3);

    assert_eq!(res1.unwrap(), "10");
    assert_eq!(res2.unwrap(), "20");
    assert_eq!(res3.unwrap(), "30");
    assert_eq!(counter.get(), 2);
}

#[test]
fn test_duplicate_keys() {
    let counter = Cell::new(0);
    let f = call_counter(&counter, stringify);

    let controller = BatchController::new(Some(2), Duration::from_secs(0), &f);

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
    assert_eq!(counter.get(), 1);
}
