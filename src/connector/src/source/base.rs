// Copyright 2023 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use bytes::Bytes;
use enum_as_inner::EnumAsInner;
use futures::stream::BoxStream;
use futures::{pin_mut, Stream, StreamExt};
use itertools::Itertools;
use prost::Message;
use risingwave_common::error::ErrorCode;
use risingwave_pb::connector_service::TableSchema;
use risingwave_pb::source::ConnectorSplit;
use serde::{Deserialize, Serialize};
use tokio::runtime::Runtime;
use tokio::sync::mpsc;

use super::datagen::DatagenMeta;
use super::filesystem::{FsSplit, S3FileReader, S3Properties, S3SplitEnumerator, S3_CONNECTOR};
use super::google_pubsub::GooglePubsubMeta;
use super::kafka::KafkaMeta;
use super::nexmark::source::message::NexmarkMeta;
use crate::parser::ParserConfig;
use crate::source::cdc::{
    CdcProperties, CdcSplit, CdcSplitReader, DebeziumSplitEnumerator, MYSQL_CDC_CONNECTOR,
    POSTGRES_CDC_CONNECTOR,
};
use crate::source::datagen::{
    DatagenProperties, DatagenSplit, DatagenSplitEnumerator, DatagenSplitReader, DATAGEN_CONNECTOR,
};
use crate::source::dummy_connector::DummySplitReader;
use crate::source::google_pubsub::{
    PubsubProperties, PubsubSplit, PubsubSplitEnumerator, PubsubSplitReader,
    GOOGLE_PUBSUB_CONNECTOR,
};
use crate::source::kafka::enumerator::KafkaSplitEnumerator;
use crate::source::kafka::source::KafkaSplitReader;
use crate::source::kafka::{KafkaProperties, KafkaSplit, KAFKA_CONNECTOR};
use crate::source::kinesis::enumerator::client::KinesisSplitEnumerator;
use crate::source::kinesis::source::reader::KinesisSplitReader;
use crate::source::kinesis::split::KinesisSplit;
use crate::source::kinesis::{KinesisProperties, KINESIS_CONNECTOR};
use crate::source::nexmark::source::reader::NexmarkSplitReader;
use crate::source::nexmark::{
    NexmarkProperties, NexmarkSplit, NexmarkSplitEnumerator, NEXMARK_CONNECTOR,
};
use crate::source::pulsar::source::reader::PulsarSplitReader;
use crate::source::pulsar::{
    PulsarProperties, PulsarSplit, PulsarSplitEnumerator, PULSAR_CONNECTOR,
};
use crate::{
    impl_connector_properties, impl_split, impl_split_enumerator, impl_split_reader,
    BoxSourceWithStateStream,
};

/// [`SplitEnumerator`] fetches the split metadata from the external source service.
/// NOTE: It runs in the meta server, so probably it should be moved to the `meta` crate.
#[async_trait]
pub trait SplitEnumerator: Sized {
    type Split: SplitMetaData + Send + Sync;
    type Properties;

    async fn new(properties: Self::Properties) -> Result<Self>;
    async fn list_splits(&mut self) -> Result<Vec<Self::Split>>;
}

/// [`SplitReader`] is an abstraction of the external connector read interface,
/// used to read messages from the outside and transform them into source-oriented
/// [`SourceMessage`], in order to improve throughput, it is recommended to return a batch of
/// messages at a time.
#[async_trait]
pub trait SplitReader: Sized {
    type Properties;

    async fn new(
        properties: Self::Properties,
        state: ConnectorState,
        columns: Option<Vec<Column>>,
    ) -> Result<Self>;

    fn into_stream(self) -> BoxSourceStream;
}

/// [`SplitReaderV2`] is a new abstraction of the external connector read interface which is
/// responsible for parsing, it is used to read messages from the outside and transform them into a
/// stream of parsed [`StreamChunk`]
#[async_trait]
pub trait SplitReaderV2: Sized {
    type Properties;

    async fn new(
        properties: Self::Properties,
        state: Vec<SplitImpl>,
        parser_config: ParserConfig,
    ) -> Result<Self>;

    fn into_stream(self) -> BoxSourceWithStateStream;
}

pub type BoxSourceStream = BoxStream<'static, Result<Vec<SourceMessage>>>;

/// The max size of a chunk yielded by source stream.
pub const MAX_CHUNK_SIZE: usize = 1024;

#[derive(Clone, Debug, Deserialize)]
pub enum ConnectorProperties {
    Kafka(Box<KafkaProperties>),
    Pulsar(Box<PulsarProperties>),
    Kinesis(Box<KinesisProperties>),
    Nexmark(Box<NexmarkProperties>),
    Datagen(Box<DatagenProperties>),
    S3(Box<S3Properties>),
    MySqlCdc(Box<CdcProperties>),
    PostgresCdc(Box<CdcProperties>),
    GooglePubsub(Box<PubsubProperties>),
    Dummy(Box<()>),
}

impl ConnectorProperties {
    fn new_cdc_properties(
        connector_name: &str,
        properties: HashMap<String, String>,
    ) -> Result<Self> {
        match connector_name {
            MYSQL_CDC_CONNECTOR => Ok(Self::MySqlCdc(Box::new(CdcProperties {
                props: properties,
                source_type: "mysql".to_string(),
                ..Default::default()
            }))),
            POSTGRES_CDC_CONNECTOR => Ok(Self::PostgresCdc(Box::new(CdcProperties {
                props: properties,
                source_type: "postgres".to_string(),
                ..Default::default()
            }))),
            _ => Err(anyhow!("unexpected cdc connector '{}'", connector_name,)),
        }
    }

    pub fn init_properties_for_cdc(
        &mut self,
        source_id: u32,
        rpc_addr: String,
        table_schema: Option<TableSchema>,
    ) {
        match self {
            ConnectorProperties::MySqlCdc(c) | ConnectorProperties::PostgresCdc(c) => {
                c.source_id = source_id;
                c.connector_node_addr = rpc_addr;
                c.table_schema = table_schema;
            }
            _ => {}
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, EnumAsInner, PartialEq, Hash)]
pub enum SplitImpl {
    Kafka(KafkaSplit),
    Pulsar(PulsarSplit),
    Kinesis(KinesisSplit),
    Nexmark(NexmarkSplit),
    Datagen(DatagenSplit),
    GooglePubsub(PubsubSplit),
    MySqlCdc(CdcSplit),
    PostgresCdc(CdcSplit),
    S3(FsSplit),
}

// for the `FsSourceExecutor`
impl SplitImpl {
    #[allow(clippy::result_unit_err)]
    pub fn into_fs(self) -> Result<FsSplit, ()> {
        match self {
            Self::S3(split) => Ok(split),
            _ => Err(()),
        }
    }

    pub fn as_fs(&self) -> Option<&FsSplit> {
        match self {
            Self::S3(split) => Some(split),
            _ => None,
        }
    }
}

pub enum SplitReaderImpl {
    Kinesis(Box<KinesisSplitReader>),
    Kafka(Box<KafkaSplitReader>),
    Dummy(Box<DummySplitReader>),
    Nexmark(Box<NexmarkSplitReader>),
    Pulsar(Box<PulsarSplitReader>),
    Datagen(Box<DatagenSplitReader>),
    MySqlCdc(Box<CdcSplitReader>),
    PostgresCdc(Box<CdcSplitReader>),
    GooglePubsub(Box<PubsubSplitReader>),
}

pub enum SplitReaderV2Impl {
    S3(Box<S3FileReader>),
    Dummy(Box<DummySplitReader>),
}

impl SplitReaderV2Impl {
    pub fn into_stream(self) -> BoxSourceWithStateStream {
        match self {
            Self::S3(s3_reader) => SplitReaderV2::into_stream(*s3_reader),
            Self::Dummy(dummy_reader) => SplitReaderV2::into_stream(*dummy_reader),
        }
    }

    pub async fn create(
        config: ConnectorProperties,
        state: ConnectorState,
        parser_config: ParserConfig,
        _columns: Option<Vec<Column>>,
    ) -> Result<Self> {
        if state.is_none() {
            return Ok(Self::Dummy(Box::new(DummySplitReader {})));
        }
        let state = state.unwrap();
        let reader = match config {
            ConnectorProperties::S3(s3_props) => Self::S3(Box::new(
                S3FileReader::new(*s3_props, state, parser_config).await?,
            )),
            _ => todo!(),
        };
        Ok(reader)
    }
}

pub enum SplitEnumeratorImpl {
    Kafka(KafkaSplitEnumerator),
    Pulsar(PulsarSplitEnumerator),
    Kinesis(KinesisSplitEnumerator),
    Nexmark(NexmarkSplitEnumerator),
    Datagen(DatagenSplitEnumerator),
    MySqlCdc(DebeziumSplitEnumerator),
    PostgresCdc(DebeziumSplitEnumerator),
    GooglePubsub(PubsubSplitEnumerator),
    S3(S3SplitEnumerator),
}

impl_connector_properties! {
    { Kafka, KAFKA_CONNECTOR },
    { Pulsar, PULSAR_CONNECTOR },
    { Kinesis, KINESIS_CONNECTOR },
    { Nexmark, NEXMARK_CONNECTOR },
    { Datagen, DATAGEN_CONNECTOR },
    { S3, S3_CONNECTOR },
    { MySqlCdc, MYSQL_CDC_CONNECTOR },
    { PostgresCdc, POSTGRES_CDC_CONNECTOR },
    { GooglePubsub, GOOGLE_PUBSUB_CONNECTOR}
}

impl_split_enumerator! {
    { Kafka, KafkaSplitEnumerator },
    { Pulsar, PulsarSplitEnumerator },
    { Kinesis, KinesisSplitEnumerator },
    { Nexmark, NexmarkSplitEnumerator },
    { Datagen, DatagenSplitEnumerator },
    { MySqlCdc, DebeziumSplitEnumerator },
    { PostgresCdc, DebeziumSplitEnumerator },
    { GooglePubsub, PubsubSplitEnumerator},
    { S3, S3SplitEnumerator }
}

impl_split! {
    { Kafka, KAFKA_CONNECTOR, KafkaSplit },
    { Pulsar, PULSAR_CONNECTOR, PulsarSplit },
    { Kinesis, KINESIS_CONNECTOR, KinesisSplit },
    { Nexmark, NEXMARK_CONNECTOR, NexmarkSplit },
    { Datagen, DATAGEN_CONNECTOR, DatagenSplit },
    { GooglePubsub, GOOGLE_PUBSUB_CONNECTOR, PubsubSplit },
    { MySqlCdc, MYSQL_CDC_CONNECTOR, CdcSplit },
    { PostgresCdc, POSTGRES_CDC_CONNECTOR, CdcSplit },
    { S3, S3_CONNECTOR, FsSplit }
}

impl_split_reader! {
    { Kafka, KafkaSplitReader },
    { Pulsar, PulsarSplitReader },
    { Kinesis, KinesisSplitReader },
    { Nexmark, NexmarkSplitReader },
    { Datagen, DatagenSplitReader },
    { MySqlCdc, CdcSplitReader},
    { PostgresCdc, CdcSplitReader},
    { GooglePubsub, PubsubSplitReader },
    { Dummy, DummySplitReader }
}

pub type DataType = risingwave_common::types::DataType;

#[derive(Clone, Debug)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
}

/// Split id resides in every source message, use `Arc` to avoid copying.
pub type SplitId = Arc<str>;

/// The message pumped from the external source service.
/// The third-party message structs will eventually be transformed into this struct.
#[derive(Debug, Clone)]
pub struct SourceMessage {
    pub payload: Option<Bytes>,
    pub offset: String,
    pub split_id: SplitId,

    pub meta: SourceMeta,
}

#[derive(Debug, Clone)]
pub enum SourceMeta {
    Kafka(KafkaMeta),
    Nexmark(NexmarkMeta),
    GooglePubsub(GooglePubsubMeta),
    Datagen(DatagenMeta),
    // For the source that doesn't have meta data.
    Empty,
}

/// Implement Eq manually to ignore the `meta` field.
impl PartialEq for SourceMessage {
    fn eq(&self, other: &Self) -> bool {
        self.offset == other.offset
            && self.split_id == other.split_id
            && self.payload == other.payload
    }
}
impl Eq for SourceMessage {}

/// The message pumped from the external source service.
/// The third-party message structs will eventually be transformed into this struct.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FsSourceMessage {
    pub payload: Option<Bytes>,
    pub offset: usize,
    pub split_size: usize,
    pub split_id: SplitId,
}

/// The metadata of a split.
pub trait SplitMetaData: Sized {
    fn id(&self) -> SplitId;
    fn encode_to_bytes(&self) -> Bytes;
    fn restore_from_bytes(bytes: &[u8]) -> Result<Self>;
}

/// [`ConnectorState`] maintains the consuming splits' info. In specific split readers,
/// `ConnectorState` cannot be [`None`] and only contains one [`SplitImpl`]. If no split is assigned
/// to source executor, `ConnectorState` is [`None`] and [`DummySplitReader`] is up instead of other
/// split readers.
pub type ConnectorState = Option<Vec<SplitImpl>>;

/// Spawn the data generator to a dedicated runtime, returns a channel receiver
/// for acquiring the generated data. This is used for the [`DatagenSplitReader`] and
/// [`NexmarkSplitReader`] in case that they are CPU intensive and may block the streaming actors.
pub fn spawn_data_generation_stream<T: Send + 'static>(
    stream: impl Stream<Item = T> + Send + 'static,
    buffer_size: usize,
) -> impl Stream<Item = T> + Send + 'static {
    static RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
        tokio::runtime::Builder::new_multi_thread()
            .thread_name("risingwave-data-generation")
            .enable_all()
            .build()
            .expect("failed to build data-generation runtime")
    });

    let (generation_tx, generation_rx) = mpsc::channel(buffer_size);
    RUNTIME.spawn(async move {
        pin_mut!(stream);
        while let Some(result) = stream.next().await {
            if generation_tx.send(result).await.is_err() {
                tracing::warn!("failed to send next event to reader, exit");
                break;
            }
        }
    });

    tokio_stream::wrappers::ReceiverStream::new(generation_rx)
}

#[cfg(test)]
mod tests {
    use maplit::*;
    use nexmark::event::EventType;

    use super::*;

    #[test]
    fn test_split_impl_get_fn() -> Result<()> {
        let split = KafkaSplit::new(0, Some(0), Some(0), "demo".to_string());
        let split_impl = SplitImpl::Kafka(split.clone());
        let get_value = split_impl.into_kafka().unwrap();
        println!("{:?}", get_value);
        assert_eq!(split.encode_to_bytes(), get_value.encode_to_bytes());

        Ok(())
    }

    #[test]
    fn test_cdc_split_state() -> Result<()> {
        let offset_str = "{\"sourcePartition\":{\"server\":\"RW_CDC_mydb.products\"},\"sourceOffset\":{\"transaction_id\":null,\"ts_sec\":1670407377,\"file\":\"binlog.000001\",\"pos\":98587,\"row\":2,\"server_id\":1,\"event\":2}}";
        let split_impl = SplitImpl::MySqlCdc(CdcSplit::new(1001, offset_str.to_string()));
        let encoded_split = split_impl.encode_to_bytes();
        let restored_split_impl = SplitImpl::restore_from_bytes(encoded_split.as_ref())?;
        assert_eq!(
            split_impl.encode_to_bytes(),
            restored_split_impl.encode_to_bytes()
        );
        Ok(())
    }

    #[test]
    fn test_extract_nexmark_config() {
        let props: HashMap<String, String> = convert_args!(hashmap!(
            "connector" => "nexmark",
            "nexmark.table.type" => "Person",
            "nexmark.split.num" => "1",
        ));

        let props = ConnectorProperties::extract(props).unwrap();

        if let ConnectorProperties::Nexmark(props) = props {
            assert_eq!(props.table_type, Some(EventType::Person));
            assert_eq!(props.split_num, 1);
        } else {
            panic!("extract nexmark config failed");
        }
    }

    #[test]
    fn test_extract_cdc_properties() {
        let user_props_mysql: HashMap<String, String> = convert_args!(hashmap!(
            "connector_node_addr" => "localhost",
            "connector" => "mysql-cdc",
            "database.hostname" => "127.0.0.1",
            "database.port" => "3306",
            "database.user" => "root",
            "database.password" => "123456",
            "database.name" => "mydb",
            "table.name" => "products",
        ));

        let user_props_postgres: HashMap<String, String> = convert_args!(hashmap!(
            "connector_node_addr" => "localhost",
            "connector" => "postgres-cdc",
            "database.hostname" => "127.0.0.1",
            "database.port" => "5432",
            "database.user" => "root",
            "database.password" => "654321",
            "schema.name" => "public",
            "database.name" => "mypgdb",
            "table.name" => "orders",
        ));

        let conn_props = ConnectorProperties::extract(user_props_mysql).unwrap();
        if let ConnectorProperties::MySqlCdc(c) = conn_props {
            assert_eq!(c.source_id, 0);
            assert_eq!(c.source_type, "mysql");
            assert_eq!(c.props.get("connector_node_addr").unwrap(), "localhost");
            assert_eq!(c.props.get("database.hostname").unwrap(), "127.0.0.1");
            assert_eq!(c.props.get("database.port").unwrap(), "3306");
            assert_eq!(c.props.get("database.user").unwrap(), "root");
            assert_eq!(c.props.get("database.password").unwrap(), "123456");
            assert_eq!(c.props.get("database.name").unwrap(), "mydb");
            assert_eq!(c.props.get("table.name").unwrap(), "products");
        } else {
            panic!("extract cdc config failed");
        }

        let conn_props = ConnectorProperties::extract(user_props_postgres).unwrap();
        if let ConnectorProperties::PostgresCdc(c) = conn_props {
            assert_eq!(c.source_id, 0);
            assert_eq!(c.source_type, "postgres");
            assert_eq!(c.props.get("connector_node_addr").unwrap(), "localhost");
            assert_eq!(c.props.get("database.hostname").unwrap(), "127.0.0.1");
            assert_eq!(c.props.get("database.port").unwrap(), "5432");
            assert_eq!(c.props.get("database.user").unwrap(), "root");
            assert_eq!(c.props.get("database.password").unwrap(), "654321");
            assert_eq!(c.props.get("schema.name").unwrap(), "public");
            assert_eq!(c.props.get("database.name").unwrap(), "mypgdb");
            assert_eq!(c.props.get("table.name").unwrap(), "orders");
        } else {
            panic!("extract cdc config failed");
        }
    }
}
