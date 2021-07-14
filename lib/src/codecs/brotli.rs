use std::io::{self, Write};

pub use brotli::write::BrotliDecoder;
pub use brotli::write::BrotliEncoder;

use super::Coder;

impl<W: Write> Coder<W> for BrotliDecoder<W> {
    fn get_mut(&mut self) -> &mut W {
        BrotliDecoder::get_mut(self)
    }

    fn finish(mut self) -> std::io::Result<W> {
        BrotliDecoder::finish(&mut self).map_err(|_| {
            io::Error::new(io::ErrorKind::Other, "brotli decoder failed to finalize stream")
        })
    }

    fn finish_boxed(self: Box<Self>) -> io::Result<W> {
        self.finish()
    }
}

impl<W: Write> Coder<W> for BrotliEncoder<W> {
    fn get_mut(&mut self) -> &mut W {
        BrotliEncoder::get_mut(self)
    }

    fn finish(mut self) -> std::io::Result<W> {
        self.flush()?;
        BrotliEncoder::finish(self).map_err(|_| {
            io::Error::new(io::ErrorKind::Other, "brotli encoder failed to finalize stream")
        })
    }

    fn finish_boxed(self: Box<Self>) -> io::Result<W> {
        self.finish()
    }
}
