use std::{collections::HashMap, default::Default, num::NonZeroUsize, task::Waker};

#[derive(Debug, Eq, PartialEq, Hash)]
pub(crate) struct Token(NonZeroUsize);

impl Token {
    fn duplicate(&self) -> Token {
        Token(self.0)
    }
}

#[derive(Debug)]
pub(crate) struct WakerSet {
    wakers: HashMap<Token, Waker>,
    next_token: NonZeroUsize,
}

impl Default for WakerSet {
    fn default() -> Self {
        Self {
            wakers: HashMap::with_capacity(1),
            next_token: NonZeroUsize::new(1).unwrap(),
        }
    }
}

impl WakerSet {
    /// Add a new waker to this set. Return the token associated with this
    /// waker's entry in the set. This token should be associated with the
    /// future, and when the future is re-polled, replace_waker should be
    /// used.
    #[must_use]
    pub(crate) fn add_waker(&mut self, waker: Waker) -> Token {
        let token = Token(self.next_token);
        self.next_token = NonZeroUsize::new(self.next_token.get() + 1).unwrap();

        self.wakers.insert(token.duplicate(), waker);
        token
    }

    /// Set a waker with an existing token in this set. Panics if the token
    /// is not present in the set. The waker is passed by reference and is set
    /// with clone_from because we assume that it comes from a Context and
    /// will need to be cloned anyway.
    pub(crate) fn replace_waker(&mut self, token: &Token, waker: &Waker) {
        self.wakers
            .get_mut(token)
            .expect("Attempted to add Waker to WakerSet with an invalid token")
            .clone_from(waker);
    }

    pub(crate) fn discard_waker(&mut self, token: Token) {
        self.wakers.remove(&token);
    }

    pub(crate) fn wake_any(&self) {
        if let Some(waker) = self.wakers.values().next() {
            waker.wake_by_ref();
        }
    }

    pub(crate) fn wake_all(self) {
        self.wakers
            .into_iter()
            .for_each(|(_token, waker)| waker.wake());
    }

    pub(crate) fn wake_all_by_ref(&self) {
        self.wakers.values().for_each(|waker| waker.wake_by_ref());
    }
}
