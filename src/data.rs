//! Data structures for passing keys and values into and out of a BatchState.
// TODO: remove Copy + Clone from Token, so that we can use ownership semantics
// to enforce that each token can be used for at-most one extraction from a
// ValueSet

use std::collections::hash_map::{Entry, HashMap};
use std::fmt::{self, Debug, Formatter};
use std::hash::Hash;
use std::mem;

#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct Token(usize);

impl Token {
    fn duplicate(&self) -> Self {
        Token(self.0)
    }
}

/// A set of keys passed into a batch loader function. Use the `keys` method
/// to get the set of keys, all of which will be unique, so that you can
/// execute your request. Then, use the `into_key_values` function to
/// transform your response data into a ValueSet, which is handed back to the
/// batch loader.
pub struct KeySet<Key> {
    // In order to not require hashable keys, this structure associates each
    // key with two pieces of information:
    //
    // - a Token, which is held by each future. The token is uniquely associated
    // with a key for a given KeySet.
    // - A count of how many futures are requesting the same key. This count
    // specifically is the number of futures *past the first* that are awaiting
    // the key; in other words, it's the number of times the value will need
    // to be cloned.
    keys: HashMap<Key, (Token, u32)>,
}

impl<Key> KeySet<Key> {
    /// Get the number of unique keys in this keyset.
    #[inline]
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// Get an iterator over all the keys in this keyset. These are guaranteed
    /// to be:
    ///
    /// - Unique
    /// - Between 1 and the configured max_keys of the related BatchRules
    /// - In an arbitrary order
    #[inline]
    pub fn keys(&self) -> impl Iterator<Item = &Key> + Clone {
        self.keys.keys()
    }

    /// After you've completed your request, use this method to pair each value
    /// in your result with its key. This is the only way to create a ValueSet,
    /// which is then returned from your batch function.
    #[inline]
    pub fn into_values<Value>(self, mut get_value: impl FnMut(&Key) -> Value) -> ValueSet<Value> {
        enum Never {}

        match self.try_into_values(move |key| -> Result<Value, Never> { Ok(get_value(key)) }) {
            Ok(values) => values,
            Err(_) => unreachable!(),
        }
    }

    /// Fallible version of into_values. Same as into_values, but will return
    /// an error the first time `get_value` returns an error.
    #[inline]
    pub fn try_into_values<Value, Error>(
        self,
        mut get_value: impl FnMut(&Key) -> Result<Value, Error>,
    ) -> Result<ValueSet<Value>, Error> {
        let result: Result<HashMap<Token, (Value, u32)>, Error> = self
            .keys
            .into_iter()
            .map(move |(key, (token, count))| Ok((token, (get_value(&key)?, count))))
            .collect();

        Ok(ValueSet { values: result? })
    }
}

impl<Key: Hash + Eq> KeySet<Key> {
    #[inline]
    pub(crate) fn new() -> Self {
        Self {
            keys: HashMap::new(),
        }
    }

    /// Add a key to this KeySet, and return the token associated with that
    /// key. This token can then be used to pull a value out of the ValueSet
    /// associated with the key.
    #[inline]
    pub(crate) fn add_key(&mut self, key: Key) -> Token {
        let new_token = Token(self.keys.len());

        let (ref token, _) = self
            .keys
            .entry(key)
            .and_modify(|&mut (_, ref mut count)| *count += 1)
            .or_insert((new_token, 0));

        token.duplicate()
    }

    /// TODO: add a function to discard a token. Need a way to associate
    /// Tokens backwards with keys.

    /// Take the keyset out of this particular &mut self instance, replacing it
    /// with an empty set. Helper method for when the state transitions out
    /// of Accumulating.
    #[inline]
    pub(crate) fn take(&mut self) -> Self {
        Self {
            keys: mem::take(&mut self.keys),
        }
    }
}

impl<Key: Hash + Eq + Debug> Debug for KeySet<Key> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("KeySet").field("keys", &self.keys).finish()
    }
}

/// A value set is an opaque data structure that contains the result of a batch
/// operation. It is created with `KeySet::into_values`, and is used by the
/// Batcher to distribute the values to the correct waiting futures.
#[derive(Debug)]
pub struct ValueSet<Value> {
    values: HashMap<Token, (Value, u32)>,
}

impl<Value> ValueSet<Value> {
    pub(crate) fn new_empty() -> Self {
        Self {
            values: HashMap::new(),
        }
    }
}

impl<Value: Clone> ValueSet<Value> {
    /// Take a value assoicated with a token out of this ValueSet. If the
    /// count of this token is > 0, the value is cloned.
    ///
    /// This function takes a Token by move, to help ensure that that token
    /// cannot be reused to take the same value again by accident.
    pub(crate) fn take(&mut self, token: Token) -> Option<Value> {
        // TODO: Replace this with RawEntry
        match self.values.entry(token) {
            Entry::Vacant(..) => None,
            Entry::Occupied(mut entry) => match entry.get_mut() {
                &mut (_, 0) => Some(entry.remove().0),
                (value, count) => {
                    *count -= 1;
                    Some(value.clone())
                }
            },
        }
    }
}