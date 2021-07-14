use xz2::stream::{Action, LzmaOptions, Status, Stream};

use super::Coder;
use crate::io;

pub struct Writer<W: io::Write> {
    w: W,
    raw: Stream,
    is_encoder: bool,
}

impl<W: io::Write> Writer<W> {
    pub fn decompressor(w: W) -> Result<Self, io::Error> {
        Ok(Self { w, raw: Stream::new_lzma_decoder(u64::max_value())?, is_encoder: false })
    }

    pub fn compressor(w: W, preset: u32) -> Result<Self, io::Error> {
        Ok(Self {
            w,
            raw: Stream::new_lzma_encoder(&LzmaOptions::new_preset(preset)?)?,
            is_encoder: true,
        })
    }
}

impl<W: io::Write> Coder<W> for Writer<W> {
    fn get_mut(&mut self) -> &mut W {
        &mut self.w
    }

    fn finish(mut self) -> io::Result<W> {
        let mut buffer = [0u8; io::BUFFER_SIZE];
        loop {
            let before_out = self.raw.total_out();
            let res = self.raw.process(&[], &mut buffer, Action::Finish)?;
            let size_out = (self.raw.total_out() - before_out) as usize;
            self.w.write_all(&buffer[..size_out])?;
            if res == Status::StreamEnd {
                return Ok(self.w);
            }
        }
    }

    fn finish_boxed(self: Box<Self>) -> io::Result<W> {
        self.finish()
    }
}

impl<W: io::Write> io::Write for Writer<W> {
    fn write(&mut self, mut input: &[u8]) -> io::Result<usize> {
        let mut buffer = [0u8; io::BUFFER_SIZE];
        let mut written = 0;
        while input.len() > 0 {
            let before_in = self.raw.total_in();
            let before_out = self.raw.total_out();
            let res = self.raw.process(input, &mut buffer, Action::Run)?;
            let size_in = (self.raw.total_in() - before_in) as usize;
            let size_out = (self.raw.total_out() - before_out) as usize;
            input = &input[size_in..];
            self.w.write_all(&buffer[0..size_out])?;
            written += size_in;
            if res == Status::StreamEnd {
                break;
            }
        }
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.is_encoder {
            let mut buffer = [0u8; io::BUFFER_SIZE];
            loop {
                let before_out = self.raw.total_out();
                let res = self.raw.process(&[], &mut buffer, Action::FullFlush)?;
                let size_out = (self.raw.total_out() - before_out) as usize;
                self.w.write_all(&buffer[..size_out])?;
                if res == Status::StreamEnd {
                    break;
                }
            }
        }
        self.w.flush()
    }
}
