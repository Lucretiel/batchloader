use std::{collections::HashMap, default::Default, num::NonZeroUsize, task::Waker};

#[repr(transparent)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct Token(NonZeroUsize);

impl Token {
    /// Make a brand new token from scratch. This should only be used when
    /// initializing a new Wakerset.
    fn new() -> Self {
        Token(NonZeroUsize::new(1).unwrap())
    }

    fn copy(&self) -> Self {
        Token(self.0)
    }

    /// Use this token as a generator; return the next token in the sequence
    /// and increment this one.
    fn next_token(&mut self) -> Token {
        let current = self.copy();

        // Interestingly, we could use wrapping_add and rely on the
        // NonZero check to panic
        *self = self
            .0
            .get()
            .checked_add(1)
            .and_then(NonZeroUsize::new)
            .map(Token)
            .expect("Overflow when creating a Waker token");

        current
    }
}

/// Data structure for managing a collection of wakers that are all interested
/// in a single shared computation. In particular, it is designed so that only
/// a single task needs to actually do the work of driving the future to
/// completion, but other tasks can take its place if that one is dropped.
///
/// Wakers can be added to a wakerset; when added, a token associated with the
/// Waker is returned. This token should be associated with a running future
/// and can be used to:
/// - replace the waker on subsequent polls
/// - discard the waker from the wakerset.
///
/// The WakerSet maintains the notion of the "driving waker"; this is the
/// waker that most recently polled the relevant future. This is always the
/// most recently added or replaced waker in the set. This design is such
/// that, after a shared future is polled, that context should be immediately
/// added to this wakerset. If the driving waker is discarded from the set,
/// another can be selected as the driving waker. In this way, we create an
/// ambiguous but nevertheless unbroken chain of wakers. So long as futures
/// take care to discard their stored tokens when dropped, the shared
/// computation will always have a "path forward".
///
/// Note that, right now, this idea of a "driving waker" is considered an
/// optimization. If there are event orderings we haven't considered that
/// result in deadlocks or no woken wakers, we may fall back to a more
/// robust design.
#[derive(Debug)]
pub(crate) struct WakerSet {
    // Invariant: There must always be a driving waker if the waker set is
    // non-empty. This is because we must ensure that there is always a task
    // driving the shared computation to completion, and if that task drops
    // the relevant future, it must be sure to awaken another one to continue
    // working. Additionally, if there is a series of drops in a row without
    // an intervening poll, we must ensure that at the end of a drop chain,
    // a non-dropped future has been awakened (or the WakerSet is empty)
    // TODO: Replace HashMap with Slab
    wakers: HashMap<Token, Waker>,
    driving_waker: Option<Token>,

    // Tokens are an ever-increasing integer. We assume that WakerSets are
    // relatively short-lived and that there's no chance of running out of
    // these. We use a nonzero usize so that we can have a cheap Option<Token>
    next_token: Token,
}

impl WakerSet {
    #[must_use]
    #[inline]
    pub fn new() -> Self {
        Self {
            wakers: HashMap::new(),
            next_token: Token::new(),
            driving_waker: None,
        }
    }

    /// Add a new waker to this set. Return the token associated with this
    /// waker's entry in the set. This token should be associated with the
    /// future, and when the future is re-polled, replace_waker should be
    /// used.
    ///
    /// This waker is set as the current driving waker, on the assumption that
    /// it has just been used to poll a future.
    #[must_use]
    pub fn add_waker(&mut self, waker: Waker) -> Token {
        let token = self.next_token.next_token();
        self.wakers.insert(token, waker);
        self.driving_waker = Some(token);
        token
    }

    /// Set a waker with an existing token in this set, replacing the existing
    /// waker. Panics if there is no such token.
    ///
    /// This waker is set as the current driving waker, on the assumption that
    /// it has just been used to poll a future.
    pub fn replace_waker(&mut self, token: &Token, waker: &Waker) {
        let current_waker = self
            .wakers
            .get_mut(&token)
            .expect("No matching token in wakerset");

        if !current_waker.will_wake(&waker) {
            current_waker.clone_from(waker)
        }

        self.driving_waker = Some(token.copy());
    }

    /// Either add a new waker (if the token is None) or replace an existing
    /// waker (if it is not). If a new waker was added, the token is updated
    /// in place. That waker is made the current driving waker, on the assumption
    /// that it has just been used to poll a future.
    pub fn update_waker(&mut self, waker: &Waker, token: &mut Option<Token>) {
        match *token {
            Some(token) => self.replace_waker(token, waker),
            None => *token = Some(self.add_waker(waker.clone())),
        }
    }

    /// Wake the current driving waker, if it exists. No-op if there are no
    /// wakers in the set.
    pub fn wake_driver(&self) {
        if let Some(driver) = self.driving_waker {
            self.wakers
                .get(&driver)
                .expect("Error: driving waker not present in Wakerset")
                .wake_by_ref();
        }
    }

    /// Wake every Waker in this wakerset
    pub fn wake_all(&self) {
        self.wakers.values().for_each(|waker| waker.wake_by_ref());
    }

    /// Discard a waker from this set. If that waker was the current driving
    /// waker (or there is currently no driving waker), an arbitrary waker is
    /// made the current driving waker and awoken.
    ///
    /// We create a new driving waker immediately because if a series of drops
    /// happen at the same time we need to ensure that at least one non-dropped
    /// waker is awoken.
    ///
    /// TODO: consider panicking if the token isn't in our WakerSet; this
    /// probably indicates a logic error in the library (not a user error)
    pub fn discard_and_wake(&mut self, token: Token) {
        self.wakers.remove(&token);

        if self.driving_waker == Some(token) || self.driving_waker.is_none() {
            match self.wakers.iter().next() {
                None => self.driving_waker = None,
                Some((&token, waker)) => {
                    self.driving_waker = Some(token);
                    waker.wake_by_ref();
                }
            }
        }
    }

    /// Discard a waker from this set, then wake all remaining wakers in the
    /// set. This is used when a shared future is about to complete itself, and
    /// it wants to awaken its siblings but doesn't want to spuriously awaken
    /// itself.
    pub fn discard_wake_all(&mut self, token: Token) {
        self.wakers.remove(&token);
        self.wake_all()
    }
}

impl Default for WakerSet {
    fn default() -> Self {
        Self::new()
    }
}

// TODO: unit tests. Our integration tests pass so this is a low priority.
// notify_tests.rs specifically tests that the functionality that wakerset
// enables is being used correctly.
