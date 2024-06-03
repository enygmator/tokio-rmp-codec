# tokio-rmp-codec

This crate provides you with a Tokio codec ([`Decoder`] and [`Encoder`]), which internally uses [`rmp_serde`] to serialize and deserialize data in the MessagePack (msgpack) format.

## Installation

To use, add the following line to `Cargo.toml` under `[dependencies]`:

```toml
tokio-rmp-codec = { git = "https://github.com/enygmator/tokio-rmp-codec.git" }
```

## Usage

Refer the unit and integration tests for usage.

## License

This project is licensed under the [MIT license](LICENSE).

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in Tokio by you, shall be licensed as MIT, without any additional
terms or conditions.

[`Decoder`]: tokio_util::codec::Decoder
[`Encoder`]: tokio_util::codec::Encoder
[`rmp_serde`]: https://crates.io/crates/rmp-serde
