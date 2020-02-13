//! Data structures for passing keys and values into and out of a BatchState.
// TODO: remove Copy + Clone from Token, so that we can use ownership semantics
// to enforce that each token can be used for at-most one extraction from a
// ValueSet

use std::collections::hash_map::{Entry, HashMap};
use std::hash::Hash;
use std::mem;
use std::num::NonZeroUsize;

#[repr(transparent)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct Token(NonZeroUsize);

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
#[derive(Debug)]
pub struct KeySet<Key: Eq + Hash> {
    // In order to not require cloneable keys, this structure associates each
    // key with two pieces of information:
    //
    // - a Token, which is held by each future. The token is uniquely associated
    // with a key for a given KeySet.
    // - A count of how many futures are requesting the same key. This count
    // specifically is the number of futures *past the first* that are awaiting
    // the key; in other words, it's the number of times the value will need
    // to be cloned.
    //
    // Note that Tokens are created with the len of keys, so keys should never
    // be removed from this table. Because KeySets tend to be short lived, this
    // shouldn't be a problem.
    keys: HashMap<Key, Token>,
    tokens: HashMap<Token, usize>,
}

impl<Key: Eq + Hash> KeySet<Key> {
    pub(crate) fn discard_token(&mut self, token: Token) {
        match self.tokens.entry(token) {
            Entry::Occupied(entry) if *entry.get() == 0 => {
                entry.remove();
            }
            Entry::Occupied(mut entry) => {
                *entry.get_mut() -= 1;
            }
            Entry::Vacant(_) => panic!("Attempted to remove nonexistent token from KeySet"),
        }
    }

    /// Check if there are any keys in this keyset
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.tokens.is_empty()
    }

    /// Get the number of unique keys in this keyset.
    #[inline]
    pub fn len(&self) -> usize {
        self.tokens.len()
    }

    /// Get an iterator over all the keys in this keyset. These are guaranteed
    /// to be:
    ///
    /// - Unique
    /// - Between 1 and the configured max_keys of the related BatchRules
    /// - In an arbitrary order
    pub fn keys(&self) -> impl Iterator<Item = &Key> + Clone {
        let tokens = &self.tokens;

        self.keys
            .iter()
            .filter(move |(_key, token)| tokens.contains_key(token))
            .map(|(key, _token)| key)
    }

    /// After you've completed your request, use this method to pair each value
    /// in your result with its key. This is the only way to create a ValueSet,
    /// which is then returned from your batch function.
    pub fn into_values<Value>(self, mut get_value: impl FnMut(&Key) -> Value) -> ValueSet<Value> {
        #[derive(Debug)]
        enum Never {}

        self.try_into_values(move |key| -> Result<Value, Never> { Ok(get_value(key)) })
            .unwrap()
    }

    /// Fallible version of into_values. Same as into_values, but will return
    /// an error the first time `get_value` returns an error.
    pub fn try_into_values<Value, Error>(
        self,
        mut get_value: impl FnMut(&Key) -> Result<Value, Error>,
    ) -> Result<ValueSet<Value>, Error> {
        let KeySet { keys, tokens } = self;

        let result: Result<HashMap<Token, ValueSetEntry<Value>>, Error> = keys
            .into_iter()
            .filter_map(move |(key, token)| {
                let count = tokens.get(&token)?;
                Some((key, token, *count))
            })
            .map(move |(key, token, count)| {
                let value = get_value(&key)?;
                Ok((token, ValueSetEntry { value, count }))
            })
            .collect();

        result.map(move |values| ValueSet { values })
    }

    pub(crate) fn new() -> Self {
        Self {
            keys: HashMap::new(),
            tokens: HashMap::new(),
        }
    }

    /// Add a key to this KeySet, and return the token associated with that
    /// key. This token can then be used to pull a value out of the ValueSet
    /// associated with the key.
    pub(crate) fn add_key(&mut self, key: Key) -> Token {
        let new_token = Token(NonZeroUsize::new(self.keys.len() + 1).unwrap());
        let token = self.keys.entry(key).or_insert(new_token).duplicate();
        self.tokens
            .entry(token.duplicate())
            .and_modify(|count| *count += 1)
            .or_insert(0);

        token
    }

    /// Take the keyset out of this particular &mut self instance, replacing it
    /// with an empty set. Helper method for when the state transitions out
    /// of Accumulating.

    pub(crate) fn take(&mut self) -> Self {
        Self {
            keys: mem::take(&mut self.keys),
            tokens: mem::take(&mut self.tokens),
        }
    }
}

#[derive(Debug)]
struct ValueSetEntry<Value> {
    count: usize,
    value: Value,
}

/// A value set is an opaque data structure that contains the result of a batch
/// operation. It is created with `KeySet::into_values`, and is used by the
/// Batcher to distribute the values to the correct waiting futures.
#[derive(Debug)]
pub struct ValueSet<Value> {
    values: HashMap<Token, ValueSetEntry<Value>>,
}

impl<Value> ValueSet<Value> {
    /// Discard a token associated with this ValueSet without getting the
    /// value. No-op if the token isn't present.
    pub(crate) fn discard(&mut self, token: Token) {
        // TODO: Replace this with RawEntry
        match self.values.entry(token) {
            Entry::Vacant(..) => {}
            Entry::Occupied(entry) if entry.get().count == 0 => {
                entry.remove();
            }
            Entry::Occupied(mut entry) => {
                entry.get_mut().count -= 1;
            }
        }
    }
}

impl<Value: Clone> ValueSet<Value> {
    // TODO: deduplicate take and discard

    /// Take a value assoicated with a token out of this ValueSet. If the
    /// count of this token is > 0, the value is cloned.
    ///
    /// This function takes a Token by move, to help ensure that that token
    /// cannot be reused to take the same value again by accident.
    pub(crate) fn take(&mut self, token: Token) -> Option<Value> {
        // TODO: Replace this with RawEntry
        match self.values.entry(token) {
            Entry::Vacant(..) => None,
            Entry::Occupied(entry) if entry.get().count == 0 => Some(entry.remove().value),
            Entry::Occupied(mut entry) => {
                let entry = entry.get_mut();
                entry.count -= 1;
                Some(entry.value.clone())
            }
        }
    }
}
