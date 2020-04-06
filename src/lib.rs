//! Batchloader is a Rust implementation of the
//! [dataloader pattern](https://github.com/graphql/dataloader) originally
//! created by Facebook. It provides an interface to batch multiple similar
//! requests into a single batch operation, then distribute the results to
//! each requester, without the requester knowing anything about the batch
//! operation. The common use case for this is batching requests to an API,
//! but it can be used for any situation where it'd be useful to hide batching
//! logic.
//!
//! As a simple example, suppose you had an API to fetch user data by username.
//! The API supports batching; that is, you can supply multiple usernames in
//! the same request and it will return results for all of them. The entry
//! point might look like this:
//!
//! ```ignore
//! #[derive(Debug, Clone)]
//! struct UserData {
//!     username: String,
//!     name: String,
//!     age: u16,
//! }
//!
//! #[derive(Debug, Clone)] struct APIError {}
//!
//! async fn get_users(usernames: impl Iterator<Item=String>) ->
//!     Result<Vec<UserData>, APIError>
//! {
//!     // ...
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
//! order to statically ensure that all keys have a matching value, a [`KeySet`]
//! is directly converted into a [`ValueSet`] after the request is completed.
//! Using the `get_users` function, a batcher might look like this:
//!
//! ```
//! # use std::collections::HashMap;
//! # use std::error::Error;
//! use batchloader::{KeySet, ValueSet};
//! # struct UserData { username: String }
//! # #[derive(Debug, Clone)] struct APIError {}
//! # async fn get_users(usernames: impl Iterator<Item=String>) ->
//! #    Result<Vec<UserData>, APIError> {Ok(vec![])}
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
//!     // map the result back to a ValueSet
//!     let mut users: HashMap<String, UserData> = users
//!         .into_iter()
//!         .map(|user| (user.username.clone(), user))
//!         .collect();
//!
//!     // a ValueSet is created key-by-key from a KeySet. This ensures that
//!     // every key is accounted for.
//!     Ok(usernames.into_values(move |username| {
//!         users.remove(username).unwrap()
//!     }))
//! }
//! ```
//!
//! Then, create a [`BatchController`]. A `BatchController` coordinates the
//! batching logic, pooling different requests together. It can be shared by
//! reference to your request handlers, such that many independent async tasks
//! (even on different threads) can batch individual key lookups into the same
//! request.
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
//! Outline:
//! - Finally, create individual futures with load
//! - Note that the controller does no work; there is not backing "task" driving
//!   the shared future to completion. All the work is done by polling.

mod batch;
mod data;
mod wakerset;

pub use batch::{BatchController, BatchFuture, BatchRules};
pub use data::{KeySet, ValueSet};
