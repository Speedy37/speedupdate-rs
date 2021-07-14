use std::io;

use super::Coder;

pub struct Writer<W: io::Write>(pub W);

impl<W: io::Write> Coder<W> for Writer<W> {
    fn get_mut(&mut self) -> &mut W {
        &mut self.0
    }

    fn finish(self) -> std::io::Result<W> {
        Ok(self.0)
    }

    fn finish_boxed(self: Box<Self>) -> io::Result<W> {
        Ok(self.0)
    }
}

impl<W: io::Write> io::Write for Writer<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write(buf)
    }

    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.0.write_all(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}
