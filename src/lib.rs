//! This crate provides you with a Tokio codec ([`Decoder`] and
//! [`Encoder`]), which internally uses [`rmp_serde`] to serialize
//! and deserialize data in the MessagePack (msgpack) format.
//!
//! You can work with the [`Stream`] and [`Sink`] on [`Framed`] that
//! the codec provides, where the stream emits deserialized values
//! and the sink accepts values to be serialized.
//!
//! [`Decoder`]: tokio_util::codec::Decoder
//! [`Encoder`]: tokio_util::codec::Encoder
//! [`Stream`]: https://docs.rs/futures/latest/futures/trait.Stream.html
//! [`Sink`]: https://docs.rs/futures/latest/futures/sink/trait.Sink.html#
//! [`Framed`]: tokio_util::codec::Framed
//!
//! # Example
//! ```no_run
//! ```

use std::marker::PhantomData;

use bytes::{Buf, BytesMut};
use serde::{Deserialize, Serialize};
use tokio_util::codec::{Decoder, Encoder};

/// A codec that decodes message pack data.
/// The codec will discard any data that exceeds the maximum message length.
pub struct MessagePackCodec<D> {
    max_message_len: usize,
    is_discarding: Option<usize>,
    _priv: PhantomData<D>,
}

impl<D> MessagePackCodec<D> {
    pub fn new() -> Self {
        Self {
            max_message_len: usize::MAX,
            is_discarding: None,
            _priv: PhantomData,
        }
    }

    pub fn with_max_message_length(max_message_len: usize) -> Self {
        Self {
            max_message_len,
            is_discarding: None,
            _priv: PhantomData,
        }
    }

    pub fn max_message_length(&self) -> usize {
        self.max_message_len
    }

    pub fn set_max_message_length(&mut self, max_message_len: usize) {
        self.max_message_len = max_message_len;
    }
}

impl<D> Decoder for MessagePackCodec<D>
where
    for<'de> D: Deserialize<'de>,
{
    type Item = D;
    type Error = MessagePackCodecError;

    /// NOTE: regardless of the maximum message length, you must ensure that the
    /// amount of data you keep in memory (which you provide to decode) does not
    /// exceed any other limits you would like to enforce (e.g. memory usage),
    /// by, like, not reading too many bytes into `src` at once.
    ///
    /// So, if you send in a huge buffer and we're able to deserialize it, then
    /// data will not be discarded
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        loop {
            let mut deserializer =
                rmp_serde::decode::Deserializer::new(std::io::Cursor::new(src.as_ref()));
            let original_pos = deserializer.position();

            match Deserialize::deserialize(&mut deserializer) {
                Ok(val) => {
                    if let Some(discard_length) = self.is_discarding {
                        // we were discarding data, but we found a valid message so we return an
                        // error informing of data being discarded, and if the caller wants to
                        // continue decoding, they can call decode again to get the message that
                        // we just successfully decoded
                        self.is_discarding = None;
                        return Err(MessagePackCodecError::MessageLengthExceededLimit(
                            discard_length,
                            self.max_message_len,
                        ));
                    } else {
                        // return the successfully decoded message
                        let decoded_len = (deserializer.position() - original_pos) as usize;
                        src.advance(decoded_len);
                        return Ok(Some(val));
                    }
                }
                Err(rmp_serde::decode::Error::InvalidDataRead(error))
                | Err(rmp_serde::decode::Error::InvalidMarkerRead(error))
                    if error.kind() == std::io::ErrorKind::UnexpectedEof =>
                {
                    // this branch indicates that more of the buffer needs to be read in,
                    // in order to decode the message, so we return None
                    let decoded_len = (deserializer.position() - original_pos) as usize;
                    if decoded_len > self.max_message_len {
                        // indicates that the message length exceeded the limit.
                        // So, we start discarding this message, and continue
                        // to discard until we find a valid message.
                        // We still return None in order to request more data.
                        self.is_discarding = Some(decoded_len);
                        src.advance(decoded_len);
                    }
                    return Ok(None);
                }
                Err(e) => {
                    // If we get here, it's a real error, not just EOF
                    if let Some(discard_length) = &mut self.is_discarding {
                        // though, this is expected if we were discarding data,
                        // so we should continue to discard

                        // the following is a weird "careful" method of discarding, where
                        // we search for the next byte that could be a valid start of a message
                        *discard_length += 1;
                        src.advance(1);
                        continue;
                    } else {
                        // we should discard the data so that we don't let
                        // the caller try to decode it again. The caller must bail
                        let decoded_len = (deserializer.position() - original_pos) as usize;
                        src.advance(decoded_len);
                        return Err(e.into());
                    }
                }
            }
        }
    }

    fn decode_eof(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match self.decode(buf)? {
            Some(val) => Ok(Some(val)),
            None => {
                if buf.is_empty() {
                    Ok(None)
                } else {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "stream ended before messagepack data was fully read",
                    )
                    .into())
                }
            }
        }
    }
}

impl<E> Encoder<E> for MessagePackCodec<E>
where
    E: Serialize,
{
    type Error = MessagePackCodecError;

    fn encode(&mut self, item: E, dst: &mut BytesMut) -> Result<(), Self::Error> {
        rmp_serde::encode::write(&mut BytesWriter(dst), &item)?;
        Ok(())
    }
}

/// Wrapper for `&mut [BytesMut]` that provides Write.
///
/// See also:
/// * <https://github.com/vorner/tokio-serde-cbor/blob/a347107ad56f2ad8086998eb63ecb70b19f3b71d/src/lib.rs#L167-L181>
/// * <https://github.com/carllerche/bytes/issues/77>
struct BytesWriter<'a>(&'a mut BytesMut);

impl<'a> std::io::Write for BytesWriter<'a> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.extend(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// An error occurred while encoding or decoding a line.
#[derive(thiserror::Error, Debug)]
pub enum MessagePackCodecError {
    /// The maximum message length was exceeded. (message length, max length)
    #[error("message length ({0}) exceeded limit ({1})")]
    MessageLengthExceededLimit(usize, usize),
    /// A message pack decoding error occurred.
    #[error(transparent)]
    RmpDecode(#[from] rmp_serde::decode::Error),
    /// A message pack encoding error occurred.
    #[error(transparent)]
    RmpEncode(#[from] rmp_serde::encode::Error),
    /// An IO error occurred.
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

#[cfg(test)]
mod tests {
    use core::panic;

    use super::*;
    use bytes::BytesMut;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct TestVal {
        a: i32,
        b: String,
    }

    impl Default for TestVal {
        fn default() -> Self {
            Self {
                a: 42,
                b: "the answer".to_string(),
            }
        }
    }

    #[test]
    fn message_pack_codec_decode_empty() {
        let mut codec = MessagePackCodec::<TestVal>::with_max_message_length(1024);
        let mut buf = BytesMut::new();
        let decoded = codec.decode(&mut buf).unwrap();
        assert_eq!(decoded, None);
        let decoded = codec.decode(&mut buf).unwrap();
        assert_eq!(decoded, None);
        let decoded = codec.decode_eof(&mut buf).unwrap();
        assert_eq!(decoded, None);
        let decoded = codec.decode_eof(&mut buf).unwrap();
        assert_eq!(decoded, None);
        assert!(buf.is_empty());
    }

    #[test]
    fn message_pack_codec_decode_single_read() {
        let mut codec = MessagePackCodec::<TestVal>::with_max_message_length(1024);
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&rmp_serde::to_vec(&TestVal::default()).unwrap());
        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded, TestVal::default());
        assert!(buf.is_empty());
    }

    #[test]
    fn message_pack_codec_decode_partial_reads() {
        let mut codec = MessagePackCodec::<TestVal>::with_max_message_length(1024);
        let mut buf = BytesMut::new();
        let encoded = rmp_serde::to_vec(&TestVal::default()).unwrap();
        buf.extend_from_slice(&encoded[..encoded.len() - 1]);
        let decoded = codec.decode(&mut buf).unwrap();
        assert_eq!(decoded, None);
        buf.extend_from_slice(&encoded[encoded.len() - 1..]);
        let decoded = codec.decode(&mut buf).unwrap();
        assert_eq!(decoded, Some(TestVal::default()));
        assert!(buf.is_empty());
    }

    #[test]
    fn message_pack_codec_decode_multiple_reads() {
        let mut codec = MessagePackCodec::<TestVal>::with_max_message_length(1024);
        let mut buf = BytesMut::new();
        let encoded = rmp_serde::to_vec(&TestVal::default()).unwrap();
        buf.extend_from_slice(&encoded);
        buf.extend_from_slice(&encoded);
        assert_eq!(buf.len(), encoded.len() * 2);
        let decoded = codec.decode(&mut buf).unwrap();
        assert_eq!(decoded, Some(TestVal::default()));
        assert_eq!(buf.len(), encoded.len());
        let decoded = codec.decode(&mut buf).unwrap();
        assert_eq!(decoded, Some(TestVal::default()));
        assert!(buf.is_empty());
    }

    #[test]
    fn message_pack_codec_decode_multiple_partial_reads() {
        let mut codec = MessagePackCodec::<TestVal>::with_max_message_length(1024);
        let mut buf = BytesMut::new();
        let encoded = rmp_serde::to_vec(&TestVal::default()).unwrap();
        buf.extend_from_slice(&encoded[..encoded.len() - 2]);
        assert_eq!(buf.len(), encoded.len() - 2);
        let decoded = codec.decode(&mut buf).unwrap();
        assert_eq!(decoded, None,);
        buf.extend_from_slice(&encoded[encoded.len() - 2..]);
        buf.extend_from_slice(&encoded[..encoded.len() - 2]);
        assert_eq!(buf.len(), 2 * encoded.len() - 2);
        let decoded = codec.decode(&mut buf).unwrap();
        assert_eq!(decoded, Some(TestVal::default()));
        buf.extend_from_slice(&encoded[encoded.len() - 2..]);
        assert_eq!(buf.len(), encoded.len());
        let decoded = codec.decode(&mut buf).unwrap();
        assert_eq!(decoded, Some(TestVal::default()));
        assert!(buf.is_empty());
    }

    #[test]
    /// Test that the codec will discard any data that exceeds the maximum message length.
    /// It should be notes that just providing a buffer that exceeds the maximum message length
    /// will not cause the codec to discard the data. The codec will only ensure that it does not
    /// accumulate more than the maximum message length within the buffer.
    fn message_pack_codec_decode_exceed_max_length() {
        let mut codec = MessagePackCodec::<TestVal>::with_max_message_length(50);
        let mut buf = BytesMut::new();
        let test_huge = TestVal {
            a: 42,
            b: "huge".to_string().repeat(100),
        };
        let mut bytes_vec;
        let encoded_huge = rmp_serde::to_vec(&test_huge).unwrap();
        let test_small = TestVal {
            a: 42,
            b: "small".to_string(),
        };
        let encoded_small = rmp_serde::to_vec(&test_small).unwrap();
        bytes_vec = encoded_small.clone();
        bytes_vec.extend_from_slice(&encoded_huge);
        bytes_vec.extend_from_slice(&encoded_small);
        // this islike pretending to be a network stream where we get chunks of data
        // and the chunk is smaller than 50 bytes, which is like our tolerance limit
        let mut chunked_bytes = bytes_vec.chunks(10);

        // The first successful decode should be the small message
        let mut loop_count = 0;
        while loop_count < 10000 {
            match codec.decode(&mut buf) {
                Ok(Some(val)) => {
                    assert_eq!(val, test_small);
                    break;
                }
                Ok(None) => {}
                Err(error) => {
                    panic!("unexpected error: {:?}", error);
                }
            }
            buf.extend_from_slice(chunked_bytes.next().unwrap());
            loop_count += 1;
        }
        if loop_count == 100 {
            panic!("loop count exceeded");
        }
        // The second decode should end up in MessageLengthExceededLimit
        let mut loop_count = 0;
        while loop_count < 10000 {
            match codec.decode(&mut buf) {
                Ok(Some(val)) => {
                    panic!("unexpected value: {:?}", val);
                }
                Ok(None) => {}
                Err(MessagePackCodecError::MessageLengthExceededLimit(len, 50))
                    if len == encoded_huge.len() =>
                {
                    break;
                }
                Err(e) => panic!("unexpected error: {:?}", e),
            }
            buf.extend_from_slice(chunked_bytes.next().unwrap());
            loop_count += 1;
        }
        if loop_count == 100 {
            panic!("loop count exceeded");
        }
        // The second successful decode should be the small message
        let mut loop_count = 0;
        while loop_count < 10000 {
            match codec.decode(&mut buf) {
                Ok(Some(val)) => {
                    assert_eq!(val, test_small);
                    break;
                }
                Ok(None) => {}
                Err(error) => {
                    panic!("unexpected error: {:?}", error);
                }
            }
            buf.extend_from_slice(chunked_bytes.next().unwrap());
            loop_count += 1;
        }
        if loop_count == 100 {
            panic!("loop count exceeded");
        }
        assert!(buf.is_empty());
    }

    #[test]
    fn message_pack_codec_decode_unexpected_eof() {
        let mut codec = MessagePackCodec::<TestVal>::with_max_message_length(1024);
        let mut buf = BytesMut::new();
        let encoded = rmp_serde::to_vec(&TestVal::default()).unwrap();
        buf.extend_from_slice(&encoded[..encoded.len() - 2]);
        assert_eq!(buf.len(), encoded.len() - 2);
        let result = codec.decode_eof(&mut buf);
        if let Err(MessagePackCodecError::Io(error)) = result {
            assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
        } else {
            panic!("unexpected result: {:?}", result);
        }
    }

    #[test]
    fn message_pack_codec_encode_multiple() {
        let test_val_1 = TestVal {
            a: 42,
            b: "the answer".to_string(),
        };
        let test_val_2 = TestVal {
            a: 43,
            b: "the answer 2".to_string(),
        };
        let mut codec = MessagePackCodec::<TestVal>::with_max_message_length(1024);
        let mut buf = BytesMut::new();
        let mut encoded = vec![];
        encoded.extend_from_slice(&rmp_serde::to_vec(&test_val_1).unwrap());
        encoded.extend_from_slice(&rmp_serde::to_vec(&test_val_2).unwrap());
        buf.reserve(encoded.len());
        codec.encode(test_val_1.clone(), &mut buf).unwrap();
        codec.encode(test_val_2.clone(), &mut buf).unwrap();
        assert_eq!(buf, encoded);
    }
}
