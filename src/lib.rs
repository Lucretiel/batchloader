//! Batchloader is a Rust implementation of the
//! [dataloader pattern](https://github.com/graphql/dataloader) originally
//! created by Facebook. It provides an interface to batch multiple similar
//! requests into a single batch operation, then distribute the results to
//! each requester, without the requester knowing anything about the batch
//! operation. The common use case for this is batching requests to an API,
//! but it can be used for any situation where it'd be useful to hide batching
//! logic.
//!
//! ## Overview
//!
//! As a simple example, suppose you had an API to fetch user data by username.
//! The API supports batching; that is, you can supply multiple usernames in
//! the same request and it will return results for all of them. The entry
//! point might look like this:
//!
//! ```
//! #[derive(Debug, Clone)]
//! struct UserData {
//!     username: String,
//!     name: String,
//!     age: u16,
//! }
//!
//! #[derive(Debug, Clone)]
//! struct APIError {}
//!
//! async fn get_users(usernames: impl Iterator<Item=String>) ->
//!     Result<Vec<UserData>, APIError>
//! {
//!     todo!()
//! }
//! ```
//!
//! However, your application only needs to get infomation for a single user
//! in a given request. You still want to be able to batch these requests, to
//! reduce API pressure; this is where batchloader comes in.
//!
//! First, your batching function should be adapted to the batchloader API.
//! A batchloader batching function takes a `KeySet<K>`, which contains all the
//! keys to be batched, and returns a `Result<ValueSet<V>, E>`. A `ValueSet`
//! contains all the resulting values, matched with their original keys. In
//! order to ensure that all keys have a matching value, the [`KeySet`] has
//! methods that create a [`ValueSet`] and ensure that all of the keys are
//! correct.
//!
//!
//! ```
//! # use std::collections::HashMap;
//! # use std::error::Error;
//! use batchloader::{KeySet, ValueSet, KeyedEntry, OnDuplicate};
//! # struct UserData { username: String, name: String, age: u16 }
//! # #[derive(Debug, Clone)] struct APIError {}
//! # async fn get_users(usernames: impl Iterator<Item=String>) ->
//! #    Result<Vec<UserData>, APIError> {Ok(vec![])}
//!
//! impl KeyedEntry<str> for UserData {
//!     fn get_key(&self) -> &str {
//!         &self.username
//!     }
//! }
//!
//! async fn batch_get_users(usernames: KeySet<String>) ->
//!     Result<ValueSet<UserData>, APIError>
//! {
//!     // perform the API request
//!     let users = get_users(usernames
//!         .keys()
//!         .map(|username| username.clone())
//!     ).await?;
//!
//!     // This method returns an error if the `users` Vec contains any
//!     // unrecognized keys, or if any keys are missing.
//!     let users = usernames.values_from_iter(OnDuplicate::Ignore, users)?
//!
//!     Ok(users)
//! }
//! ```
//!
//! In this example, we use [`KeySet::values_from_iter`] and the
//! [`KeyedEntry`] trait. There are a few other methods available to create a
//! [`ValueSet`], depending on the shape of your data; see the [`KeySet`]
//! documentation for details.
//!
//! Once you have a batch function compatible with `batchloader`, create a
//! [`BatchController`]. A `BatchController` coordinates the batching logic,
//! pooling different requests together. It can be shared by reference to your
//! request handlers, such that many independent async tasks (even on different
//! threads) can batch individual key lookups into the same request.
//!
//! The [`BatchController`] is configured with 3 fields:
//! - The `batcher` has already been described; it's the asynchronous function
//!   that actually does the work.
//! - `window` is another async function that defines the window during which
//!   several keys can be collected. For a typical web application, this would
//!   do something as simple as (asynchronously) sleep for a few milliseconds,
//!   during which other requests can come in and be added to the set.
//! - `max_keys` is an optional maximum number of keys per batch. If set,
//!   the batch will be dispatched immediately when this number of keys is
//!   reached, regardless of the window.
//!
//! ```
//! # use std::error::Error;
//! # use std::time::Duration;
//! use batchloader::{BatchController, BatchRules};
//! use futures_timer::Delay; // a runtime-agnostic async sleep
//! # use batchloader::{KeySet, ValueSet};
//! # #[derive(Debug, Clone)] struct UserData { username: String }
//! # #[derive(Debug, Clone)] struct APIError {}
//! # async fn batch_get_users(usernames: KeySet<String>) ->
//! #     Result<ValueSet<UserData>, APIError> { todo! {} }
//!
//! // BatchRules defines the behavior for a BatchController. It has to be
//! // passed by reference so that the `batcher` function can be called by
//! // reference.
//! let rules = BatchRules {
//!     batcher: batch_get_users,
//!     window: || Delay::new(Duration::from_millis(1)),
//!     max_keys: None,
//! };
//! let controller = BatchController::new(&rules);
//! ```
//!
//! Once you have a [`BatchController`], you can begin loading batched data with
//! the [`load`] method. [`load`] is called with a single key, and creates
//! a [`BatchFuture`] associated with that key. The [`BatchController`] will
//! collect all of the keys that were requested in its window, up to the
//! configured `max_keys`. Once the window expires, or the `max_keys` are
//! reached, polling the [`BatchFuture`] will drive the batch function, and
//! once it completes, each [`BatchFuture`] will complete with the value
//! associated with its individual key.
//!
//! ```
//! # use std::error::Error;
//! # use std::time::Duration;
//! # use batchloader::{BatchController, BatchRules};
//! # use futures_timer::Delay; // a runtime-agnostic async sleep
//! # use batchloader::{KeySet, ValueSet};
//! # #[derive(Debug, Clone)] struct UserData { username: String }
//! # #[derive(Debug, Clone)] struct APIError {}
//! # async fn batch_get_users(usernames: KeySet<String>) ->
//! #     Result<ValueSet<UserData>, APIError> { todo! {} }
//! # let rules = BatchRules {
//! #    batcher: batch_get_users,
//! #    window: || Delay::new(Duration::from_millis(1)),
//! #    max_keys: None,
//! # };
//! # let controller = BatchController::new(&rules);
//! let request_handler = |username: String| async {
//!     let user_result = controller.load(username);
//!
//!     match user_result {
//!         Ok(user_data) => {}
//!         Err(api_error) => {}
//!     }
//! }
//! ```
//!
//! ## Design notes
//!
//! ### `KeySet` and `ValueSet`
//!
//! Traditional dataloader implementations, including the original javascript
//! dataloader, use batching functions that simply take a list of keys and
//! return a same-length list of values that are paired with those keys. While
//! it maximizes simplicity, there are a few problems with this approach:
//!
//! - The interface has no way to guarantee that the returned values list is
//!   the same length as the input keys list. This is a logic error in the
//!   implementation, but there isn't really a good way to report this error
//!   to the developer.
//! - The interface doesn't account at all for duplicate keys; it assumes that
//!   each incoming request is unique.
//!
//! To solve both of these problems, batchloader uses the [`KeySet`] type for
//! incoming keys to a batch function, and the [`ValueSet`] type for outgoing
//! values. The key design element here is that the only way to create a
//! [`ValueSet`] is with one of the methods on [`KeySet`]; these methods all
//! ensure that the [`ValueSet`] has precisely the set of data matching the
//! keys.
//!
//! In addition, the [`KeySet`] deduplicates incoming keys, and only passes
//! unique keys to the batch function. The [`ValueSet`] internally stores how
//! many requests for a key have been made, and uses value cloning to give
//! out values to different futures (past the first) associated with the same
//! key. In the common case of no key duplicates, no clones will occur.
//!
//! ### Poll-driven design.
//!
//! In keeping with the Rust's polling async design, all of the asynchronous
//! work in batchloader is driven through polling [`BatchFuture`]. The batch
//! function is not scheduled in any runtime or run in the background; it is
//! driven directly in the foreground by `BatchFuture`.
//!
//! The polling is designed to be as lazy as possible. Because the batch
//! function only needs to be driven by a single task, BatchFuture tracks and
//! notifies only a single task to drive the shared batch job forward. The
//! other futures are woken only when data is available (or when the driving
//! future is dropped)
//!
//! When the batched work is complete, every future is notified. However, the
//! values are not distributed "into" the futures until the futures are polled;
//! each polled future will extract its particular value from the ValueSet
//!
//! [`load`]: BatchController::load

mod batch;
mod data;
mod wakerset;

pub use batch::{BatchController, BatchFuture, BatchRules};
pub use data::{IntoValuesError, KeySet, KeyValueEntry, KeyedEntry, OnDuplicate, ValueSet};
