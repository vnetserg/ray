pub use error_chain::{bail, ChainedError};

use error_chain::error_chain;

use tonic::{Code, Status};

use std::{fmt, io};

error_chain! {
    foreign_links {
        Io(io::Error);
        ProtoEncode(prost::EncodeError);
        ProtoDecode(prost::DecodeError);
    }
}

// There is chain formatting already in error_chain, but we want to cheer it up a bit.
pub struct DisplayFancyChain<'a, T: 'a + ?Sized>(&'a T);

impl<'a, T> fmt::Display for DisplayFancyChain<'a, T>
where
    T: ChainedError,
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "↪ Error: {}", self.0)?;

        for e in self.0.iter().skip(1) {
            write!(fmt, "\n↪ Caused by: {}", e)?;
        }

        if let Some(backtrace) = ChainedError::backtrace(self.0) {
            write!(fmt, "\n{:?}", backtrace)?;
        }

        Ok(())
    }
}

pub trait WithFancyChain {
    fn display_fancy_chain(&self) -> DisplayFancyChain<'_, Error>;
}

impl WithFancyChain for Error {
    fn display_fancy_chain(&self) -> DisplayFancyChain<'_, Error> {
        DisplayFancyChain(&self)
    }
}

impl From<Error> for Status {
    fn from(err: Error) -> Self {
        let message = format!("{}", err.display_chain());
        Self::new(Code::Internal, message)
    }
}
