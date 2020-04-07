//! Data structures for passing keys and values into and out of a BatchState.
// TODO: remove Copy + Clone from Token, so that we can use ownership semantics
// to enforce that each token can be used for at-most one extraction from a
// ValueSet

use std::{
    borrow::Borrow,
    collections::hash_map::{Entry, HashMap},
    error::Error,
    fmt::{Debug, Display},
    hash::Hash,
    mem,
    num::NonZeroUsize,
};

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct Token(NonZeroUsize);

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
    keys: HashMap<Key, Token>,
    tokens: HashMap<Token, usize>,
}

impl<Key: Eq + Hash> KeySet<Key> {
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

    /// Get an iterator over all the keys in this KeySet. These are guaranteed
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

    /// Look up if a key is present in this KeySet.
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        Q: Hash + Eq,
        Key: Borrow<Q>,
    {
        match self.keys.get(key) {
            Some(token) => self.tokens.contains_key(token),
            None => false,
        }
    }

    /// After you've completed your batch job, use this method to pair each
    /// value in your result with its key, into a `ValueSet`. This method will
    /// iterate over each key in the KeySet, calling `get_value` on each one,
    /// to produce the `ValueSet` that will be used to distribute values to
    /// the individual futures. If you don't have an easy `get_value`function—
    /// for example, if your data is in an unordered `Vec`— consider using the
    /// [`values_from_iter`] function instead.
    pub fn into_values<'a, Value>(
        &'a self,
        mut get_value: impl FnMut(&'a Key) -> Value,
    ) -> ValueSet<Value> {
        #[derive(Debug)]
        enum Never {}

        self.try_into_values(move |key| -> Result<Value, Never> { Ok(get_value(key)) })
            .unwrap()
    }

    /// Fallible version of `into_values`. Same as `into_values`, but will return
    /// an error the first time `get_value` returns an error.
    pub fn try_into_values<'a, Value, Error>(
        &'a self,
        mut get_value: impl FnMut(&'a Key) -> Result<Value, Error>,
    ) -> Result<ValueSet<Value>, Error> {
        let result: Result<HashMap<Token, ValueSetEntry<Value>>, Error> = self
            .keys
            .iter()
            .filter_map(move |(key, token)| {
                let count = self.tokens.get(token)?;
                Some((key, *token, *count))
            })
            .map(move |(key, token, count)| {
                let value = get_value(key)?;
                Ok((token, ValueSetEntry { value, count }))
            })
            .collect();

        result.map(move |values| ValueSet { values })
    }

    /// After you've completed your request, use this method to turn your
    /// resultant values into a ValueSet. If you input data structure has a
    /// fast way to look up pairings between keys and values (such as a
    /// `HashMap`), consider using `into_values`, as it requires many fewer
    /// runtime checks to ensure that the data is valid.
    ///
    /// Each entry in the input `entries` must implement [`KeyValueEntry`],
    /// which gets the Key and the Value for each entry. This trait is already
    /// implemented on `(Key, Value)` pairs. See also the [`KeyedEntry`] trait,
    /// if the key is simply a member field of a value (common in JSON APIs).
    ///
    /// The `on_dup` parameter controls what happens if a duplicate key is in
    /// your `entries` data: it can be skipped, replace the old data, or
    /// result in an error.
    ///
    /// If the input data is not well-formed, this method will return an error
    /// indicating what went wrong. See [`IntoValuesError`] for details.
    pub fn values_from_iter<T, Value, Q>(
        &self,
        on_dup: OnDuplicate,
        entries: impl IntoIterator<Item = T>,
    ) -> Result<ValueSet<Value>, IntoValuesError<T, Key>>
    where
        T: KeyValueEntry<Q, Value>,
        Key: Borrow<Q>,
        Q: Hash + Eq,
    {
        let KeySet { keys, tokens } = self;
        let mut values: HashMap<Token, ValueSetEntry<Value>> = HashMap::with_capacity(tokens.len());

        for entry in entries {
            let key = entry.get_key();

            match keys.get(key) {
                None => return Err(IntoValuesError::UnrecognizedKey(entry)),
                Some(&token) => match values.entry(token) {
                    Entry::Occupied(slot) => match on_dup {
                        OnDuplicate::Replace => slot.into_mut().value = entry.into_value(),
                        OnDuplicate::Skip => continue,
                        OnDuplicate::Error => return Err(IntoValuesError::DuplicateKey(entry)),
                    },
                    Entry::Vacant(slot) => match tokens.get(&token) {
                        None => return Err(IntoValuesError::UnrecognizedKey(entry)),
                        Some(&count) => {
                            slot.insert(ValueSetEntry {
                                count,
                                value: entry.into_value(),
                            });
                        }
                    },
                },
            }
        }

        for (key, token) in keys {
            if !values.contains_key(&token) {
                return Err(IntoValuesError::MissingKey(key));
            }
        }

        Ok(ValueSet { values })
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
        let token = *self.keys.entry(key).or_insert(new_token);
        self.tokens
            .entry(token)
            .and_modify(|count| *count += 1)
            .or_insert(0);

        token
    }

    /// Use a token to discard a previously added key from the keyset. Panics
    /// if the key is not present, because this indicates an error in the logic
    /// somewhere.
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

/// Trait for data values that have keys associated with themselves. Intended
/// for APIs that return arrays where the key for each entry is part of the
/// entry. This is a shortcut trait for `KeyValueEntry`, for the case where
/// your Value type has a Key as one of its member fields (common in JSON APIs
/// that return objects)
pub trait KeyedEntry<K> {
    /// Get a reference to the key associated with this entry
    fn get_key(&self) -> &K;
}

/// Trait for types that contain a key and a value. Used by
/// [`KeySet::values_from_iter`] to generically accept any data type in an
/// iterable. Implemented automatically for `(K, V)` tuples and for
/// `V: KeyedEntry<K>`.
pub trait KeyValueEntry<K, V>: Sized {
    /// Get a reference to the key associated with this entry
    fn get_key(&self) -> &K;

    /// Get the value associated with this entry.
    fn into_value(self) -> V;
}

impl<K, V> KeyValueEntry<K, V> for (K, V) {
    fn get_key(&self) -> &K {
        &self.0
    }

    fn into_value(self) -> V {
        self.1
    }
}

impl<K, V> KeyValueEntry<K, V> for V
where
    V: KeyedEntry<K>,
{
    fn get_key(&self) -> &K {
        KeyedEntry::get_key(self)
    }

    fn into_value(self) -> Self {
        self
    }
}

/// This enum controls how [`ValueSet::values_from_iter`] method handles
/// duplicate keys. See that method's documentation for details.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnDuplicate {
    /// New duplicate keys replace the old data
    Replace,
    /// Duplicate keys are skipped
    Skip,
    /// Duplicate keys cause the method to return an error
    Error,
}

/// If the `entries` input data to [`ValueSet::values_from_iter`] was not
/// well-formed, this error is returned indicating the specific error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntoValuesError<'a, Entry, Key> {
    /// This key in the `KeySet` was not present in `entries`
    MissingKey(&'a Key),

    /// The key for this entry was encountered more than once in `entries`
    DuplicateKey(Entry),

    /// The key for this entry was not present in the `KeySet`
    UnrecognizedKey(Entry),
}

impl<Key: Debug, Entry: Debug> Display for IntoValuesError<'_, Key, Entry> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use IntoValuesError::*;

        match self {
            MissingKey(key) => write!(f, "missing key: {:?}", key),
            DuplicateKey(entry) => write!(f, "entry key is a duplicate: {:?}", entry),
            UnrecognizedKey(entry) => write!(f, "unrecognized key for entry: {:?}", entry),
        }
    }
}

impl<Key: Debug, Entry: Debug> Error for IntoValuesError<'_, Key, Entry> {}

/// Inner value type for a ValueSet
#[derive(Debug)]
struct ValueSetEntry<Value> {
    /// Like with KeySet, the count of a value is specifically the number of
    /// clones we expect to need of the value
    count: usize,
    value: Value,
}

/// Helper struct for returning either a reference or a owned value. Basically
/// identical to Cow, but we don't require a ToOwned bound.
#[derive(Debug)]
enum MaybeRef<'a, T> {
    Owned(T),
    Ref(&'a T),
}

impl<'a, T: Clone> MaybeRef<'a, T> {
    fn into_owned(self) -> T {
        match self {
            MaybeRef::Owned(value) => value,
            MaybeRef::Ref(value) => value.clone(),
        }
    }
}

/// A value set is an opaque data structure that contains the result of a batch
/// operation. It is created with `KeySet::into_values`, and is used by the
/// Batcher to distribute the values to the correct waiting futures.
#[derive(Debug)]
pub struct ValueSet<Value> {
    values: HashMap<Token, ValueSetEntry<Value>>,
}

impl<Value> ValueSet<Value> {
    /// Get the value associate with the token and decrement its count.
    /// If its count is > 0, a reference is returned; otherwise the value is
    /// removed from the set and returned by value. This function provides
    /// the common logic between `discard` and `take`; it ensures that values
    /// are not cloned in the discard case.
    #[inline]
    fn extract(&mut self, token: Token) -> Option<MaybeRef<Value>> {
        // TODO: Replace this with RawEntry
        match self.values.entry(token) {
            Entry::Vacant(..) => None,
            Entry::Occupied(entry) if entry.get().count == 0 => {
                Some(MaybeRef::Owned(entry.remove().value))
            }
            Entry::Occupied(entry) => {
                let entry = entry.into_mut();
                entry.count -= 1;
                Some(MaybeRef::Ref(&entry.value))
            }
        }
    }

    /// Discard a token associated with this ValueSet without getting the
    /// value. No-op if the token isn't present.
    pub(crate) fn discard(&mut self, token: Token) {
        let _value = self.extract(token);
    }
}

impl<Value: Clone> ValueSet<Value> {
    /// Take a value assoicated with a token out of this ValueSet. If the
    /// count of this token is > 0, the value is cloned.
    ///
    /// This method uses the Key counters in the original KeySet to determine
    /// the number of times the value needs to be cloned.
    pub(crate) fn take(&mut self, token: Token) -> Option<Value> {
        self.extract(token).map(|value| value.into_owned())
    }
}

// TODO: unit tests. Our integration tests pass so this is a low priority.
