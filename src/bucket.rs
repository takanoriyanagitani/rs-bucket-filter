#[derive(Clone, PartialEq, PartialOrd, Eq, Ord)]
pub struct Bucket {
    name: String,
}

impl Bucket {
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
