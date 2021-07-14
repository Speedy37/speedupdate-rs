//! Traits, helpers, and type definitions for encoding/decoding.
use std::collections::HashMap;
use std::marker::PhantomData;
use std::ops::RangeInclusive;
use std::str::FromStr;

use byte_unit::Byte;

use crate::io;

#[cfg(feature = "brotli")]
pub mod brotli;
#[cfg(feature = "lzma")]
pub mod lzma;
pub mod raw;
#[cfg(feature = "vcdiff")]
pub mod vcdiff;
#[cfg(feature = "zstd")]
pub mod zstd;

pub trait Coder<W>: io::Write {
    /// Acquires a mutable reference to the underlying writer
    ///
    /// Note that mutation of the writer may result in surprising results if
    /// this decoder is continued to be used.
    fn get_mut(&mut self) -> &mut W;

    fn finish(self) -> io::Result<W>;

    fn finish_boxed(self: Box<Self>) -> io::Result<W>;
}

/// Coder adaptor which compute for input sha1, output sha1, count read bytes
/// and count written bytes.
pub struct CheckCoder<'a, W, C> {
    writer: io::CheckWriter<Box<dyn Coder<io::CheckWriter<W, C>> + 'a>, C>,
}

impl<'a, W, C> CheckCoder<'a, W, C>
where
    W: io::Write + 'a,
    C: io::Check + Default + 'a,
{
    pub fn decoder(decompressor_name: &str, writer: W) -> io::Result<Self> {
        let output_writer = io::CheckWriter::new(writer);
        let transform_writer = decoder(decompressor_name, output_writer)?;
        let input_writer = io::CheckWriter::new(transform_writer);
        Ok(Self { writer: input_writer })
    }

    pub fn encoder(encoder_options: &CoderOptions, writer: W) -> io::Result<Self> {
        let output_writer = io::CheckWriter::new(writer);
        let transform_writer = encoder(encoder_options, output_writer)?;
        let input_writer = io::CheckWriter::new(transform_writer);
        Ok(Self { writer: input_writer })
    }
}

impl<'a, W, C> CheckCoder<'a, W, C>
where
    W: io::Write + io::ReadSlice + 'a,
    C: io::Check + Default + 'a,
{
    pub fn patch_decoder<L>(
        patcher_name: &str,
        decompressor_name: &str,
        local: L,
        writer: W,
    ) -> io::Result<Self>
    where
        L: io::Read + io::Seek + 'a,
    {
        let output_writer = io::CheckWriter::new(writer);
        let transform_writer =
            patch_decoder(decompressor_name, patcher_name, local, output_writer)?;
        let input_writer = io::CheckWriter::new(transform_writer);
        Ok(Self { writer: input_writer })
    }

    pub fn patch_encoder<L>(patcher_options: &CoderOptions, local: L, writer: W) -> io::Result<Self>
    where
        L: io::Read + io::Seek + 'a,
    {
        let output_writer = io::CheckWriter::new(writer);
        let transform_writer = patch_encoder(patcher_options, local, output_writer)?;
        let input_writer = io::CheckWriter::new(transform_writer);
        Ok(Self { writer: input_writer })
    }
}

impl<W, C> CheckCoder<'_, W, C> {
    pub fn input_checks(&mut self) -> &mut C {
        &mut self.writer.check
    }

    pub fn output_checks(&mut self) -> &mut C {
        &mut self.writer.writer.get_mut().check
    }

    pub fn finish(self) -> io::Result<io::CheckWriter<W, C>> {
        self.writer.writer.finish_boxed()
    }
}

impl<W, C> io::Write for CheckCoder<'_, W, C>
where
    W: io::Write,
    C: io::Check,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.writer.write(buf)
    }

    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.writer.write_all(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

struct MapCoder<D, W0, W1> {
    decoder: D,
    phantom: std::marker::PhantomData<(W0, W1)>,
}

impl<D, W0, W1> MapCoder<D, W0, W1>
where
    D: Coder<W0>,
    W0: Coder<W1>,
{
    fn new(decoder: D) -> Self {
        Self { decoder, phantom: std::marker::PhantomData }
    }
}

impl<D, W0, W1> io::Write for MapCoder<D, W0, W1>
where
    D: Coder<W0>,
    W0: Coder<W1>,
{
    #[inline]
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.decoder.write(buf)
    }

    #[inline]
    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.decoder.write_all(buf)
    }

    #[inline]
    fn flush(&mut self) -> io::Result<()> {
        self.decoder.flush()
    }
}

impl<D, W0, W1> Coder<W1> for MapCoder<D, W0, W1>
where
    D: Coder<W0>,
    W0: Coder<W1>,
{
    #[inline]
    fn get_mut(&mut self) -> &mut W1 {
        self.decoder.get_mut().get_mut()
    }

    #[inline]
    fn finish(self) -> io::Result<W1> {
        self.decoder.finish()?.finish()
    }

    #[inline]
    fn finish_boxed(self: Box<Self>) -> io::Result<W1> {
        self.decoder.finish()?.finish()
    }
}

trait BoxCoder<W0, W1> {
    fn boxed<'a, D: Coder<W0> + 'a>(d: D) -> Box<dyn Coder<W1> + 'a>
    where
        W0: 'a,
        W1: 'a;
}

struct BoxCoderDirect<W>(PhantomData<W>);

impl<W> BoxCoder<W, W> for BoxCoderDirect<W> {
    fn boxed<'a, D: Coder<W> + 'a>(d: D) -> Box<dyn Coder<W> + 'a>
    where
        W: 'a,
    {
        Box::new(d)
    }
}
struct BoxCoderFlatten<W0, W1>(PhantomData<(W0, W1)>);

impl<W0, W1> BoxCoder<W0, W1> for BoxCoderFlatten<W0, W1>
where
    W0: Coder<W1>,
{
    fn boxed<'a, D: Coder<W0> + 'a>(d: D) -> Box<dyn Coder<W1> + 'a>
    where
        W0: 'a,
        W1: 'a,
    {
        Box::new(MapCoder::new(d))
    }
}

pub struct CoderOptions {
    name: String,
    options: HashMap<String, String>,
}

impl CoderOptions {
    pub fn new(name: String) -> Self {
        Self { name, options: HashMap::new() }
    }

    /// Minimum ratio (0..=100) to reach to keep this coder
    ///
    /// i.e. `ratio = (enc_size * 100) / pre_size`
    pub fn min_ratio(&self) -> io::Result<u64> {
        self.get_size(&["minratio"], 100)
    }

    /// Minimum size to reach to keep this coder
    pub fn min_size(&self) -> io::Result<u64> {
        self.get_size(&["minsize"], 0)
    }

    /// Maximum size to not reach to keep this coder
    pub fn max_size(&self) -> io::Result<u64> {
        self.get_size(&["maxsize"], u64::max_value())
    }

    pub fn from_str(s: &str) -> io::Result<Self> {
        let mut it = s.splitn(2, ":");
        let name = it
            .next()
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("bad compressor format, no name: {}", s),
                )
            })?
            .to_string();
        let options = it
            .next()
            .unwrap_or_default()
            .split(';')
            .filter(|o| !o.is_empty())
            .map(|o| {
                let mut it = o.splitn(2, "=");
                let name = it
                    .next()
                    .ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("bad option format, no name: {}", o),
                        )
                    })?
                    .to_string();
                let value = it.next();
                Ok(match (name, value) {
                    (name, Some(value)) => (name, value.to_string()),
                    (name, None) => (String::new(), name),
                })
            })
            .collect::<io::Result<HashMap<String, String>>>()?;
        Ok(CoderOptions { name, options })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn get(&self, names: &[&str]) -> Option<&str> {
        names.iter().find_map(|&name| self.options.get(name).map(|s| s.as_str()))
    }

    pub fn get_u32(&self, names: &[&str], default: u32) -> io::Result<u32> {
        match self.get(names) {
            Some(value) => Ok(u32::from_str(value).map_err(|_err| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("bad option value, not a u32: {}", value),
                )
            })?),
            None => Ok(default),
        }
    }

    pub fn get_u64(&self, names: &[&str], default: u64) -> io::Result<u64> {
        match self.get(names) {
            Some(value) => Ok(u64::from_str(value).map_err(|_err| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("bad option value, not a u64: {}", value),
                )
            })?),
            None => Ok(default),
        }
    }

    pub fn get_size(&self, names: &[&str], default: u64) -> io::Result<u64> {
        match self.get(names) {
            Some(value) => Ok(Byte::from_str(value)
                .map_err(|_err| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("bad option value, not a size: {}", value),
                    )
                })?
                .get_bytes()),
            None => Ok(default),
        }
    }

    pub fn get_bool(&self, names: &[&str], default: u32) -> io::Result<bool> {
        let v = self.get_u32_range(names, default, 0..=1)?;
        Ok(v == 1)
    }

    pub fn get_u32_range(
        &self,
        names: &[&str],
        default: u32,
        range: RangeInclusive<u32>,
    ) -> io::Result<u32> {
        let v = self.get_u32(names, default)?;
        if range.contains(&v) {
            Ok(v)
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("bad option value, not in range: {} {:?}", v, range),
            ))
        }
    }
}

pub fn encoder<'a, W>(
    encoder_options: &CoderOptions,
    output: W,
) -> io::Result<Box<dyn Coder<W> + 'a>>
where
    W: io::Write + 'a,
{
    #[cfg(feature = "brotli")]
    if encoder_options.name() == "brotli" {
        let quality = encoder_options.get_u32_range(&["", "quality"], 6, 0..=11)?;
        let lgwin = encoder_options.get_u32_range(&["lgwin", "lg_window_size"], 20, 10..=30)?;
        return Ok(BoxCoderDirect::boxed(brotli::BrotliEncoder::from_params(
            output,
            ::brotli::CompressParams::new().quality(quality).lgwin(lgwin),
        )));
    }

    #[cfg(feature = "lzma")]
    if encoder_options.name() == "lzma" {
        let mut preset = encoder_options.get_u32_range(&["", "preset"], 6, 0..=9)?;
        if encoder_options.get_bool(&["extreme"], 1)? {
            preset |= lzma_sys::LZMA_PRESET_EXTREME;
        }
        return Ok(BoxCoderDirect::boxed(lzma::Writer::compressor(output, preset)?));
    }

    #[cfg(feature = "zstd")]
    if encoder_options.name() == "zstd" {
        let level = encoder_options.get_u32_range(&["", "level"], 3, 1..=21)?;
        return Ok(BoxCoderDirect::boxed(zstd::Encoder::new(output, level as i32)?));
    }

    if encoder_options.name() == "raw" {
        return Ok(BoxCoderDirect::boxed(raw::Writer(output)));
    }

    Err(io::Error::new(
        io::ErrorKind::Other,
        format!("encoder {} isn't supported!", encoder_options.name()),
    ))
}

pub fn decoder<'a, W>(
    decompressor_name: &str,
    output: W,
) -> Result<Box<dyn Coder<W> + 'a>, io::Error>
where
    W: io::Write + 'a,
{
    decoder_flatten::<BoxCoderDirect<W>, W, W>(decompressor_name, output)
}

fn decoder_flatten<'a, B, W0, W1>(
    decompressor_name: &str,
    output: W0,
) -> Result<Box<dyn Coder<W1> + 'a>, io::Error>
where
    B: BoxCoder<W0, W1>,
    W0: io::Write + 'a,
    W1: 'a,
{
    #[cfg(feature = "brotli")]
    if decompressor_name == "brotli" {
        return Ok(B::boxed(brotli::BrotliDecoder::new(output)));
    }

    #[cfg(feature = "lzma")]
    if decompressor_name == "lzma" {
        return Ok(B::boxed(lzma::Writer::decompressor(output)?));
    }

    #[cfg(feature = "zstd")]
    if decompressor_name == "zstd" {
        return Ok(B::boxed(zstd::Decoder::new(output)?));
    }

    if decompressor_name == "raw" {
        return Ok(B::boxed(raw::Writer(output)));
    }

    Err(io::Error::new(
        io::ErrorKind::Other,
        format!("decompressor {} isn't supported!", decompressor_name),
    ))
}

pub fn patch_encoder<'a, L, W>(
    patcher_options: &CoderOptions,
    local: L,
    output: W,
) -> Result<Box<dyn Coder<W> + 'a>, io::Error>
where
    L: io::Read + io::Seek + 'a,
    W: io::Write + 'a,
{
    #[cfg(feature = "vcdiff")]
    if patcher_options.name() == "vcdiff" {
        todo!()
    }

    #[cfg(feature = "zstd")]
    if patcher_options.name() == "zstd" {
        let mut local = local;
        let level = patcher_options.get_u32_range(&["", "level"], 3, 1..=21)?;
        let mut buf = Vec::new();
        local.read_to_end(&mut buf)?;
        return Ok(BoxCoderDirect::boxed(zstd::Encoder::with_dictionary(
            output,
            level as i32,
            &buf,
        )?));
    }

    if patcher_options.name() == "raw" {
        return Ok(Box::new(raw::Writer(output)));
    }

    Err(io::Error::new(io::ErrorKind::Other, "not implemented!"))
}

pub fn patch_decoder<'a, L, W>(
    decompressor_name: &str,
    patcher_name: &str,
    local: L,
    output: W,
) -> Result<Box<dyn Coder<W> + 'a>, io::Error>
where
    L: io::Read + io::Seek + 'a,
    W: io::Write + io::ReadSlice + 'a,
{
    #[cfg(feature = "vcdiff")]
    if patcher_name == "vcdiff" {
        let patcher = vcdiff::DecoderWriter::new(local, output, io::BUFFER_SIZE);
        let decompressor =
            decoder_flatten::<BoxCoderFlatten<_, W>, _, W>(decompressor_name, patcher)?;
        return Ok(decompressor);
    }

    #[cfg(feature = "zstd")]
    if patcher_name == "zstd" {
        let mut local = local;
        let mut buf = Vec::new();
        local.read_to_end(&mut buf)?;
        return Ok(BoxCoderDirect::boxed(zstd::Decoder::with_dictionary(output, &buf)?));
    }

    if patcher_name == "raw" {
        return decoder(decompressor_name, output);
    }

    Err(io::Error::new(io::ErrorKind::Other, "not implemented!"))
}
