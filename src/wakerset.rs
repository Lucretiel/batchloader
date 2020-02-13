use std::{
    collections::{hash_map::Entry, HashMap},
    task::Waker,
};

#[derive(Debug, Eq, PartialEq, Hash)]
pub(crate) struct Token(usize);

impl Token {
    fn duplicate(&self) -> Token {
        Token(self.0)
    }
}

#[derive(Debug, Default)]
pub(crate) struct WakerSet {
    wakers: HashMap<Token, Waker>,
    next_token: usize,
}

impl WakerSet {
    /// Add a new waker to this set. Return the token associated with this
    /// waker's entry in the set. This token should be associated with the
    /// future, and when the future is re-polled, replace_waker should be
    /// used.
    #[must_use]
    fn add_waker(&mut self, waker: Waker) -> Token {
        let token = Token(self.next_token);
        self.next_token += 1;

        self.wakers.insert(token.duplicate(), waker);
        token
    }

    /// Set a waker with an existing token in this set. Panics if the token
    /// is not present in the set. The waker is passed by reference and is set
    /// with clone_from because we assume that it comes from a Context and
    /// will need to be cloned anyway.
    fn replace_waker(&mut self, token: &Token, waker: &Waker) {
        self.wakers
            .get_mut(token)
            .expect("Attempted to add Waker to WakerSet with an invalid token")
            .clone_from(waker);
    }

    fn discard_waker(&mut self, token: Token) {
        self.wakers.remove(&token);
    }

    fn wake_any(&self) {
        if let Some(waker) = self.wakers.values().next() {
            waker.wake_by_ref();
        }
    }

    fn wake_all(self) {
        self.wakers
            .into_iter()
            .for_each(|(_token, waker)| waker.wake());
    }

    fn wake_all_by_ref(&self) {
        self.wakers.values().for_each(|waker| waker.wake_by_ref());
    }
}
