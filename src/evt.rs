/// A list of events(errors).
#[derive(Debug)]
pub enum Event {
    UnexpectedError(String),
    UnableToConnect(String),
}
