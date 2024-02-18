use anyhow::Result;

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    pointing_to_first: bool,
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > TwoMergeIterator<A, B>
{
    fn should_point_to_first(&self) -> bool {
        if !self.a.is_valid() {
            return false;
        }

        if !self.b.is_valid() {
            return true;
        }

        self.a.key() <= self.b.key()
    }

    pub fn create(a: A, b: B) -> Result<Self> {
        let mut iter = Self {
            a,
            b,
            pointing_to_first: false,
        };
        iter.pointing_to_first = iter.should_point_to_first();

        Ok(iter)
    }
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        if self.pointing_to_first {
            self.a.key()
        } else {
            self.b.key()
        }
    }

    fn value(&self) -> &[u8] {
        if self.pointing_to_first {
            self.a.value()
        } else {
            self.b.value()
        }
    }

    fn is_valid(&self) -> bool {
        if self.pointing_to_first {
            self.a.is_valid()
        } else {
            self.b.is_valid()
        }
    }

    fn next(&mut self) -> Result<()> {
        if self.pointing_to_first {
            if self.b.is_valid() && self.a.key() == self.b.key() {
                self.b.next()?;
            }
            self.a.next()?;
        } else {
            if self.a.is_valid() && self.a.key() == self.b.key() {
                self.a.next()?;
            }
            self.b.next()?;
        }

        self.pointing_to_first = self.should_point_to_first();

        Ok(())
    }
}
