use std::{collections::HashMap, default::Default, num::NonZeroUsize, task::Waker};

#[derive(Debug, Eq, PartialEq, Hash)]
pub(crate) struct Token(NonZeroUsize);

impl Token {
    fn duplicate(&self) -> Token {
        Token(self.0)
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
/// These tokens cannot be cloned or otherwise duplicated; this helps to ensure
/// that their lifespan are correctly associated with a particular task.
///
/// The WakerSet maintains the notion of the "driving waker"; this is the
/// waker that most recently polled the relevant future. This is always the
/// most recently added or replaced waker in the set; it is assumed that a
/// waker will be upserted into the set, and then used to poll the underlying
/// future. If the driving waker is discarded from the set, another can be
/// selected as the driving waker. In this way, we create an ambiguous but
/// nevertheless unbroken chain of wakers. So long as futures take care to
/// discard their stored tokens when dropped, the shared computation will
/// always have a "path forward".
#[derive(Debug)]
pub(crate) struct WakerSet {
    // Invariant: There must always be a driving waker if the waker set is
    // non-empty. This is because we must ensure that there is always a task
    // driving the shared computation to completion, and if that task drops
    // the relevant future, it must be sure to awaken another one to continue
    // working. Additionally, if there is a series of drops in a row without
    // an intervening poll, we must ensure that at the end of a drop chain,
    // a non-dropped future has been awakened (or the WakerSet is empty)
    wakers: HashMap<Token, Waker>,
    driving_waker: Option<Token>,

    // Tokens are an ever-increasing integer. We assume that WakerSets are
    // relatively short-lived and that there's no chance of running out of
    // these.
    next_token: NonZeroUsize,
}

impl WakerSet {
    #[must_use]
    #[inline]
    pub fn new() -> Self {
        Self {
            wakers: HashMap::new(),
            next_token: NonZeroUsize::new(1).unwrap(),
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
        let token = Token(self.next_token);
        self.next_token = self
            .next_token
            .get()
            .checked_add(1)
            .and_then(NonZeroUsize::new)
            .expect("Overflow when creating token");

        self.wakers.insert(token.duplicate(), waker);
        self.driving_waker = Some(token.duplicate());
        token
    }

    /// Set a waker with an existing token in this set, replacing the existing
    /// waker. Panics if the token is not present in the set. The waker is
    /// passed by reference and is set with clone_from because we assume that
    /// it comes from a Context and will need to be cloned anyway.
    ///
    /// This waker is set as the current driving waker, on the assumption that
    /// it has just been used to poll a future.
    pub fn replace_waker(&mut self, token: &Token, waker: &Waker) {
        self.wakers
            .get_mut(token)
            .expect("Attempted to add Waker to WakerSet with an invalid token")
            .clone_from(waker);

        self.driving_waker = Some(token.duplicate());
    }

    /// Discard a waker from this set. If that waker was the current driving
    /// waker (or there is currently no driving waker), an arbitrary waker is
    /// made the current driving waker and awoken.
    ///
    /// We create a new driving waker immediately because if a series of drops
    /// happen at the same time we need to ensure that at least one non-dropped
    /// waker is awoken.
    pub fn discard_and_wake(&mut self, token: Token) {
        // TODO: current we panic in replace_waker if the Token doesn't exist
        // in the set. Should we do the same thing here?
        self.wakers.remove(&token);
        if self.driving_waker == Some(token) || self.driving_waker.is_none() {
            match self.wakers.iter().next() {
                None => self.driving_waker = None,
                Some((token, waker)) => {
                    self.driving_waker = Some(token.duplicate());
                    waker.wake_by_ref();
                }
            }
        }
    }

    /// Wake the currently driving waker for this WakerSet. No-op if there is
    /// no current driver.
    pub fn wake_driver(&self) {
        // note: one of the invariants of this data structure is that there is
        // ALWAYS a driving waker if wakers is non-empty. Therefore, we can
        // assume that if this is None, there's no need to try to create a new
        // driving waker.
        if let Some(ref token) = self.driving_waker {
            self.wakers
                .get(token)
                .expect("Driving waker is somehow not present in waker set")
                .wake_by_ref();
        }
    }

    /// Consume this WakerSet and awaken each waker it contains.
    pub fn wake_all(self) {
        self.wakers
            .into_iter()
            .for_each(|(_token, waker)| waker.wake());
    }

    /// Discard a waker from this set, then wake all remaining wakers in the
    /// set. This is used when a shared future is about to complete itself, and
    /// it wants to awaken its siblings but doesn't want to spuriously awaken
    /// itself.
    pub fn discard_wake_all(mut self, token: Token) {
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
