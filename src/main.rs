#![feature(slice_concat_ext)]

extern crate clap;
extern crate env_logger;
extern crate futures;
extern crate futures_cpupool;
#[macro_use]
extern crate log;
extern crate prost;
#[macro_use]
extern crate prost_derive;
extern crate rdkafka;
#[macro_use]
extern crate serde_derive;
extern crate tokio_core;
extern crate toml;

use std::slice::SliceConcatExt;
use clap::{App, Arg};
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext, StreamConsumer};
use rdkafka::producer::FutureProducer;
use rdkafka::message::{Message as KafkaMessage, Timestamp};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::config::ClientConfig;
use std::time::Duration;
use std::collections::HashMap;
use std::iter::FromIterator;
use std::io::Cursor;
use prost::Message;
use std::fs::File;
use std::io::Read;
use futures::Future;
use futures::future::{err, ok};
use futures::stream::Stream;
use futures_cpupool::Builder;
use tokio_core::reactor::Core;

pub mod protocol {
    include!(concat!(env!("OUT_DIR"), "/protocol.offsets.rs"));
}

pub mod config;

static TIMEOUT: u64 = 60000;

fn latest_offsets<C: ConsumerContext>(
    consumer: &BaseConsumer<C>,
    topic_name: &str,
) -> Vec<(i32, (i64, i64))> {
    let meta = consumer
        .fetch_metadata(Some(topic_name), Duration::from_millis(TIMEOUT))
        .expect("Get metadata for topic failed");

    let topic = meta.topics()
        .into_iter()
        .filter(|t| t.name() == topic_name)
        .nth(0)
        .expect("Topic not found.");

    topic
        .partitions()
        .into_iter()
        .map(|p| {
            (
                p.id(),
                consumer
                    .fetch_watermarks(topic_name, p.id(), Duration::from_millis(TIMEOUT))
                    .expect("Get watermarks for topic and partition"),
            )
        })
        .collect()
}

fn message_at_offset<C: ConsumerContext>(
    consumer: &BaseConsumer<C>,
    topic_name: &str,
    partition: i32,
    offset: i64,
) -> protocol::WithOriginalOffset {
    let mut assignment = HashMap::with_capacity(1);
    assignment.insert((topic_name.to_string(), partition), Offset::Offset(offset));
    consumer
        .assign(&TopicPartitionList::from_topic_map(&assignment))
        .expect("Consumer offsets not assigned");

    let message = consumer
        .poll(Duration::from_millis(TIMEOUT))
        .and_then(|r| r.ok())
        .expect("Unable to poll message");

    let payload = message.payload().expect("Payload non empty");

    protocol::WithOriginalOffset::decode(&mut Cursor::new(payload)).expect("Unable to deserialize")
}

fn pipeline(
    consumer_brokers: &str,
    producer_brokers: &str,
    pool_size: usize,
    assignment: &TopicPartitionList,
    renaming: &HashMap<String, String>,
) {
    let mut core = Core::new().unwrap();
    let handle = core.handle();
    let cpu_pool = Builder::new().pool_size(pool_size).create();

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", consumer_brokers)
        .set("group.id", "kafka-copy")
        .set("enable.partition.eof", "false")
        .set("enable.auto.commit", "false")
        .create()
        .expect("Consumer creation failed");

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", producer_brokers)
        .set("produce.offset.report", "true")
        .create()
        .expect("Producer creation error");

    consumer
        .assign(assignment)
        .expect("Consumer offsets not assigned");

    let processed_stream = consumer
        .start()
        .filter_map(|result| match result {
            Ok(msg) => Some(msg),
            Err(kafka_error) => {
                warn!("Error while receiving from Kafka: {:?}", kafka_error);
                None
            }
        })
        .for_each(|msg| {
            let producer = producer.clone();
            let owned_message = msg.detach();
            let dest_topic = renaming.get(owned_message.topic()).unwrap().clone();

            let process_message = cpu_pool
                .spawn_fn(move || {
                    let offsets = protocol::WithOriginalOffset {
                        original_offset: owned_message.offset(),
                    };
                    let payload = owned_message.payload().map(|payload| {
                        let mut buf = Vec::new();
                        buf.reserve(offsets.encoded_len());
                        offsets.encode(&mut buf).unwrap();
                        [payload, &buf].concat()
                    });
                    let timestamp = match owned_message.timestamp() {
                        Timestamp::NotAvailable => None,
                        Timestamp::CreateTime(t) => Some(t),
                        Timestamp::LogAppendTime(t) => Some(t),
                    };

                    producer.send_copy(
                        &dest_topic,
                        Some(owned_message.partition()),
                        payload.as_ref(),
                        owned_message.key(),
                        timestamp,
                        1000,
                    )
                })
                .map_err(|e| {
                    warn!("Error while sending message to Kafka {:?}", e);
                })
                .and_then(|delivery_result| match delivery_result {
                    Ok(_) => ok(()),
                    Err((e, _)) => {
                        warn!("Error while sending message to Kafka {:?}", e);
                        err(())
                    }
                });

            handle.spawn(process_message);
            Ok(())
        });

    info!("Starting event loop");
    core.run(processed_stream).unwrap();
    info!("Stream processing terminated");
}

fn main() {
    env_logger::init();
    info!("Logger Initialized!");

    let matches = App::new("Kafka Copy Thread")
        .version("0.1.0")
        .author("Roman M. <splusminusx@gmail.com>")
        .about("Copy kafka topics between different clusters.")
        .arg(
            Arg::with_name("config")
                .short("c")
                .long("config")
                .value_name("FILE")
                .help("Sets a custom config file")
                .required(true)
                .takes_value(true),
        )
        .get_matches();

    let config = matches
        .value_of("config")
        .map(|path| {
            let mut file =
                File::open(&path).expect(format!("Unable to open file {}", path).as_ref());
            let mut s = String::new();
            file.read_to_string(&mut s).expect("Unable to read file");
            let c: config::ServerConfig =
                toml::from_str(&s).expect(format!("Unable to parse config {}", s).as_ref());
            c
        })
        .expect("config option is required");
    info!("Using config {:?}", config);

    let consumer: BaseConsumer = ClientConfig::new()
        .set(
            "bootstrap.servers",
            config.producer_brokers.join(",").as_ref(),
        )
        .set("group.id", "kafka-copy")
        .create()
        .expect("Consumer creation failed");

    let origin_offsets: Vec<((String, i32), Offset)> = config
        .topic_mappings
        .iter()
        .flat_map(|topic| {
            latest_offsets(&consumer, topic.to.as_ref())
                .iter()
                .map(|offs| {
                    info!("{:?}", offs);
                    let &(partition, (_, last_offset)) = offs;
                    (
                        (topic.from.clone(), partition),
                        if topic.use_offsets_from_dest {
                            Offset::Offset(
                                message_at_offset(
                                    &consumer,
                                    topic.to.as_ref(),
                                    partition,
                                    last_offset - 1,
                                ).original_offset,
                            )
                        } else {
                            Offset::Beginning
                        },
                    )
                })
                .collect::<Vec<((String, i32), Offset)>>()
        })
        .collect();

    info!("Start from offsets: {:?}", origin_offsets);

    let assignment = TopicPartitionList::from_topic_map(&HashMap::from_iter(origin_offsets));

    let renaming: HashMap<String, String> = HashMap::from_iter(
        config
            .topic_mappings
            .iter()
            .map(|m| (m.from.clone(), m.to.clone())),
    );

    pipeline(
        config.consumer_brokers.join(",").as_ref(),
        config.producer_brokers.join(",").as_ref(),
        config.thread_count,
        &assignment,
        &renaming,
    );
}
