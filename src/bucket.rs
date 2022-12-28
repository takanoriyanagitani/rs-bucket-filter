//! A key/value pairs container.

/// An ID of the container which can have many key/value pairs.
#[derive(Clone, PartialEq, PartialOrd, Eq, Ord)]
pub struct Bucket {
    /// The name of this bucket.
    name: String,
}

impl Bucket {
    /// Gets the name of this bucket as str.
    pub fn as_str(&self) -> &str {
        self.name.as_str()
    }

    /// Creates a bucket from a checked string.
    ///
    /// No check will be done by this library.
    pub fn new_checked(checked: String) -> Self {
        Self { name: checked }
    }
}
