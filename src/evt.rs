#[derive(Debug)]
pub enum Event {
    UnexpectedError(String),
    UnableToConnect(String),
}
