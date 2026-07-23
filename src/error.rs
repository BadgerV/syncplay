use thiserror::Error;

#[derive(Error, Debug)]
pub enum SyncPlayError {
    #[error("Audio error: {0}")]
    Audio(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("mDNS error: {0}")]
    Mdns(String),

    #[error("Serialization error: {0}")]
    Serialization(#[from] Box<bincode::ErrorKind>),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Stream error: {0}")]
    Stream(#[from] cpal::StreamError),

    #[error("Backend error: {0}")]
    Backend(#[from] cpal::BackendSpecificError),

    #[error("Build stream error: {0}")]
    BuildStream(#[from] cpal::BuildStreamError),

    #[error("Play stream error: {0}")]
    PlayStream(#[from] cpal::PlayStreamError),
}

impl From<mdns_sd::Error> for SyncPlayError {
    fn from(e: mdns_sd::Error) -> Self {
        SyncPlayError::Mdns(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, SyncPlayError>;
