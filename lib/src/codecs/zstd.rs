use std::io::{self, Write};

pub use zstd::stream::write::Decoder;
pub use zstd::stream::write::Encoder;

impl<W: Write> super::Coder<W> for Decoder<'static, W> {
    fn get_mut(&mut self) -> &mut W {
        Decoder::get_mut(self)
    }

    fn finish(self) -> std::io::Result<W> {
        Ok(Decoder::into_inner(self))
    }

    fn finish_boxed(self: Box<Self>) -> io::Result<W> {
        self.finish()
    }
}

impl<W: Write> super::Coder<W> for Encoder<'static, W> {
    fn get_mut(&mut self) -> &mut W {
        Encoder::get_mut(self)
    }

    fn finish(self) -> std::io::Result<W> {
        Encoder::finish(self)
    }

    fn finish_boxed(self: Box<Self>) -> io::Result<W> {
        self.finish()
    }
}
