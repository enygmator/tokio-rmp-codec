use std::collections::HashMap;

use maplit::hashmap;
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use tokio_rmp_codec::MessagePackCodec;
use tokio_stream::StreamExt;
use tokio_util::codec::Decoder;
#[allow(unused_imports)]
use tracing::{debug, info};

/// This is a struct that is used to work with the `Ext` type in MessagePack
/// NOTE: renaming the container to [`rmp_serde::MSGPACK_EXT_STRUCT_NAME`] is a hack
/// recommended by the `rmp-serde` crate to deserialize MessagePack Extension types.
#[derive(Deserialize, Serialize, Debug, PartialEq)]
#[serde(rename = "_ExtStruct")]
// WARN: as far as I can tell, the serde deserialize and serialize derive macros
// always maintain the order of fields in the structs as defined in the source code
// since they are macros and work before compilation occurs.
// IF there is reordering, then there will be a problem with the deserialization
// as messagepack, at least the "mode" that fluent-bit uses, is not self-describing
// and the order of fields is important.
pub struct MessagePackExt((i8, serde_bytes::ByteBuf));

/// The metadata that is attached to the FluentBit message
#[derive(Deserialize, Serialize, Debug, PartialEq)]
// WARN: as far as I can tell, the serde deserialize and serialize derive macros
// always maintain the order of fields in the structs as defined in the source code
// since they are macros and work before compilation occurs.
// IF there is reordering, then there will be a problem with the deserialization
// as messagepack, at least the "mode" that fluent-bit uses, is not self-describing
// and the order of fields is important.
pub struct FLBEventMetadata {
    /// The timestamp of the event.  
    /// NOTE: we currently don't use this field, so we are not deserializing it
    timestamp: MessagePackExt,
    some_other_map: HashMap<String, String>,
}

/// The FluentBit message format that is specifically used by QQ  
/// The general format is as specified in [fluent-bit documentation],
/// though it seems to be missing a [`Map`][FLBEventMetadata] field
/// (`some_other_field`).
///
/// [fluent-bit documentation]: https://github.com/fluent/fluent-bit-docs/blob/master/development/msgpack-format.md
#[derive(Deserialize, Serialize, Debug, PartialEq)]
// WARN: as far as I can tell, the serde deserialize and serialize derive macros
// always maintain the order of fields in the structs as defined in the source code
// since they are macros and work before compilation occurs.
// IF there is reordering, then there will be a problem with the deserialization
// as messagepack, at least the "mode" that fluent-bit uses, is not self-describing
// and the order of fields is important.
pub struct FLBEventMessage {
    pub metadata: FLBEventMetadata,
    data: HashMap<String, String>,
}

#[test_log::test(tokio::test)]
async fn fluentbit_messagepack_decode() {
    let message = include_bytes!("fluentbit_msgpack_bytes.bin");
    let reader = tokio_test::io::Builder::new().read(message).build();

    let messagepack_codec = MessagePackCodec::<FLBEventMessage>::with_max_message_length(1024);
    let mut framed = messagepack_codec.framed(reader);

    let tag_string = "tag".to_string();
    let nfs_string = "nfs".to_string();
    let log_string = "log".to_string();

    assert_eq!(
        FLBEventMessage {
            metadata: FLBEventMetadata {
                timestamp: MessagePackExt((0, ByteBuf::from([102, 93, 72, 98, 57, 201, 218, 11]))),
                some_other_map: hashmap! {}
            },
            data: hashmap! {tag_string.clone() => nfs_string.clone(), log_string.clone() => "".to_string()}
        },
        framed.next().await.unwrap().unwrap()
    );
    assert_eq!(
        FLBEventMessage {
            metadata: FLBEventMetadata {
                timestamp: MessagePackExt((0, ByteBuf::from([102, 93, 72, 98, 57, 201, 246, 234]))),
                some_other_map: hashmap! {}
            },
            data: hashmap! {tag_string.clone() => nfs_string.clone(), log_string.clone() => "[2024/06/03 04:34:44] [ info] [output:tcp:tcp.0] worker #0 started".to_string()}
        },
        framed.next().await.unwrap().unwrap()
    );
    assert_eq!(
        FLBEventMessage {
            metadata: FLBEventMetadata {
                timestamp: MessagePackExt((0, ByteBuf::from([102, 93, 72, 98, 57, 201, 249, 181]))),
                some_other_map: hashmap! {}
            },
            data: hashmap! {log_string.clone() => "[2024/06/03 04:34:44] [ info] [output:tcp:tcp.0] worker #1 started".to_string(), tag_string.clone() => nfs_string.clone()}
        },
        framed.next().await.unwrap().unwrap()
    );
    assert_eq!(
        FLBEventMessage {
            metadata: FLBEventMetadata {
                timestamp: MessagePackExt((0, ByteBuf::from([102, 93, 72, 98, 57, 201, 251, 146]))),
                some_other_map: hashmap! {}
            },
            data: hashmap! {tag_string.clone() => nfs_string.clone(), log_string.clone() => "[2024/06/03 04:34:44] [ info] [output:tcp:tcp.0] worker #2 started".to_string()}
        },
        framed.next().await.unwrap().unwrap()
    );
    assert_eq!(
        FLBEventMessage {
            metadata: FLBEventMetadata {
                timestamp: MessagePackExt((0, ByteBuf::from([102, 93, 72, 98, 57, 201, 253, 111]))),
                some_other_map: hashmap! {}
            },
            data: hashmap! {tag_string.clone() => nfs_string.clone(), log_string.clone() => "[2024/06/03 04:34:44] [ info] [sp] stream processor started".to_string()}
        },
        framed.next().await.unwrap().unwrap()
    );
    assert_eq!(
        FLBEventMessage {
            metadata: FLBEventMetadata {
                timestamp: MessagePackExt((0, ByteBuf::from([102, 93, 72, 98, 57, 201, 254, 93]))),
                some_other_map: hashmap! {}
            },
            data: hashmap! {tag_string.clone() => nfs_string.clone(), log_string.clone() => "[2024/06/03 04:34:44] [ info] [output:tcp:tcp.0] worker #3 started".to_string()}
        },
        framed.next().await.unwrap().unwrap()
    );
    assert_eq!(
        FLBEventMessage {
            metadata: FLBEventMetadata {
                timestamp: MessagePackExt((0, ByteBuf::from([102, 93, 72, 98, 57, 202, 0, 58]))),
                some_other_map: hashmap! {}
            },
            data: hashmap! {tag_string.clone() => nfs_string.clone(), log_string.clone() => "[2024/06/03 04:34:44] [ info] [output:tcp:tcp.0] worker #4 started".to_string()}
        },
        framed.next().await.unwrap().unwrap()
    );
    assert_eq!(
        FLBEventMessage {
            metadata: FLBEventMetadata {
                timestamp: MessagePackExt((0, ByteBuf::from([102, 93, 72, 98, 57, 202, 2, 23]))),
                some_other_map: hashmap! {}
            },
            data: hashmap! {log_string.clone() => "[2024/06/03 04:34:44] [ info] [output:tcp:tcp.0] worker #6 started".to_string(), tag_string.clone() => nfs_string.clone()}
        },
        framed.next().await.unwrap().unwrap()
    );
    assert_eq!(
        FLBEventMessage {
            metadata: FLBEventMetadata {
                timestamp: MessagePackExt((0, ByteBuf::from([102, 93, 72, 98, 57, 202, 3, 5]))),
                some_other_map: hashmap! {}
            },
            data: hashmap! {tag_string.clone() => nfs_string.clone(), log_string.clone() => "[2024/06/03 04:34:44] [ info] [output:tcp:tcp.0] worker #9 started".to_string()}
        },
        framed.next().await.unwrap().unwrap()
    );
    assert_eq!(
        FLBEventMessage {
            metadata: FLBEventMetadata {
                timestamp: MessagePackExt((0, ByteBuf::from([102, 93, 72, 98, 57, 202, 4, 226]))),
                some_other_map: hashmap! {}
            },
            data: hashmap! {log_string.clone() => "[2024/06/03 04:34:44] [ info] [output:tcp:tcp.0] worker #8 started".to_string(), tag_string.clone() => nfs_string.clone()}
        },
        framed.next().await.unwrap().unwrap()
    );
    assert_eq!(
        FLBEventMessage {
            metadata: FLBEventMetadata {
                timestamp: MessagePackExt((0, ByteBuf::from([102, 93, 72, 98, 57, 202, 5, 208]))),
                some_other_map: hashmap! {}
            },
            data: hashmap! {tag_string.clone() => nfs_string.clone(), log_string.clone() => "^C[2024/06/03 04:34:48] [engine] caught signal (SIGINT)".to_string()}
        },
        framed.next().await.unwrap().unwrap()
    );
    assert_eq!(
        FLBEventMessage {
            metadata: FLBEventMetadata {
                timestamp: MessagePackExt((0, ByteBuf::from([102, 93, 72, 98, 57, 202, 7, 173]))),
                some_other_map: hashmap! {}
            },
            data: hashmap! {log_string.clone() => "[2024/06/03 04:34:48] [ warn] [engine] service will shutdown in max 5 seconds".to_string(), tag_string.clone() => nfs_string.clone()}
        },
        framed.next().await.unwrap().unwrap()
    );
    assert_eq!(
        FLBEventMessage {
            metadata: FLBEventMetadata {
                timestamp: MessagePackExt((0, ByteBuf::from([102, 93, 72, 98, 57, 202, 8, 156]))),
                some_other_map: hashmap! {}
            },
            data: hashmap! {tag_string.clone() => nfs_string.clone(), log_string.clone() => "[2024/06/03 04:34:48] [ info] [input] pausing tail.0".to_string()}
        },
        framed.next().await.unwrap().unwrap()
    );
    assert_eq!(
        FLBEventMessage {
            metadata: FLBEventMetadata {
                timestamp: MessagePackExt((0, ByteBuf::from([102, 93, 72, 98, 57, 202, 10, 120]))),
                some_other_map: hashmap! {}
            },
            data: hashmap! {log_string.clone() => "[2024/06/03 04:34:49] [ info] [output:tcp:tcp.0] thread worker #1 stopping...".to_string(), tag_string.clone() => nfs_string.clone()}
        },
        framed.next().await.unwrap().unwrap()
    );
    assert_eq!(
        FLBEventMessage {
            metadata: FLBEventMetadata {
                timestamp: MessagePackExt((0, ByteBuf::from([102, 93, 72, 98, 57, 202, 12, 85]))),
                some_other_map: hashmap! {}
            },
            data: hashmap! {log_string.clone() => "[2024/06/03 04:34:49] [ info] [output:tcp:tcp.0] thread worker #9 stopping...".to_string(), tag_string.clone() => nfs_string.clone()}
        },
        framed.next().await.unwrap().unwrap()
    );
    assert_eq!(
        FLBEventMessage {
            metadata: FLBEventMetadata {
                timestamp: MessagePackExt((0, ByteBuf::from([102, 93, 72, 98, 57, 202, 13, 68]))),
                some_other_map: hashmap! {}
            },
            data: hashmap! {log_string.clone() => "[2024/06/03 04:34:49] [ info] [output:tcp:tcp.0] thread worker #9 stopped".to_string(), tag_string.clone() => nfs_string.clone()}
        },
        framed.next().await.unwrap().unwrap()
    );
    assert_eq!(
        FLBEventMessage {
            metadata: FLBEventMetadata {
                timestamp: MessagePackExt((0, ByteBuf::from([102, 93, 72, 98, 57, 202, 15, 33]))),
                some_other_map: hashmap! {}
            },
            data: hashmap! {log_string.clone() => "[2024/06/03 04:34:49] [ info] [input:tail:tail.0] inotify_fs_remove(): inode=16803765 watch_fd=1".to_string(), tag_string.clone() => nfs_string.clone()}
        },
        framed.next().await.unwrap().unwrap()
    );
}
