use crate::io;

struct MapReadSlice<W> {
    inner: W,
}

impl<T: io::ReadSlice> vcdiff::ReadSlice for MapReadSlice<T> {
    fn read_slice(&mut self, pos: io::SeekFrom, buf: &mut [u8]) -> io::Result<()> {
        self.inner.read_slice(pos, buf)
    }
}

impl<T: io::Write> io::Write for MapReadSlice<T> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

pub struct DecoderWriter<R, W>
where
    R: io::Read + io::Seek,
    W: io::Write + io::ReadSlice,
{
    decoder: vcdiff::VCDiffDecoder<R, MapReadSlice<W>>,
    state: vcdiff::DecoderState,
}

impl<R, W> DecoderWriter<R, W>
where
    R: io::Read + io::Seek,
    W: io::Write + io::ReadSlice,
{
    pub fn new(original: R, target: W, buffer_size: usize) -> Self {
        Self {
            decoder: vcdiff::VCDiffDecoder::new(
                original,
                MapReadSlice { inner: target },
                buffer_size,
            ),
            state: vcdiff::DecoderState::WantMoreInputOrDone,
        }
    }
}

impl<R, W> super::Coder<W> for DecoderWriter<R, W>
where
    R: io::Read + io::Seek,
    W: io::Write + io::ReadSlice,
{
    fn get_mut(&mut self) -> &mut W {
        &mut self.decoder.get_mut().1.inner
    }

    fn finish(self) -> std::io::Result<W> {
        if self.state != vcdiff::DecoderState::WantMoreInputOrDone {
            Err(io::Error::new(io::ErrorKind::InvalidData, "vcdiff decoder wants more input"))
        } else {
            Ok(self.decoder.into_inner().1.inner)
        }
    }

    fn finish_boxed(self: Box<Self>) -> io::Result<W> {
        self.finish()
    }
}

impl<R, W> io::Write for DecoderWriter<R, W>
where
    R: io::Read + io::Seek,
    W: io::Write + io::ReadSlice,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.state = self.decoder.decode(buf)?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
