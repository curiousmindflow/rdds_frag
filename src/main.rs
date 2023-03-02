use std::{
    iter,
    thread::{self, sleep},
};

use rustdds::{DomainParticipant, Duration, QosPolicyBuilder};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct Time {
    pub(crate) sec: i64,
    pub(crate) nanosec: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct Header {
    pub(crate) stamp: Time,
    pub(crate) frame_id: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct PointField {
    pub(crate) name: String,
    pub(crate) offset: u64,
    pub(crate) data_type: u8,
    pub(crate) count: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct PointCloud2 {
    pub(crate) header: Header,
    pub(crate) height: u64,
    pub(crate) width: u64,
    pub(crate) fields: Vec<PointField>,
    pub(crate) point_step: u64,
    pub(crate) row_step: u64,
    pub(crate) data: Vec<u8>,
}

fn main() {
    let domain_participant = DomainParticipant::new(0).unwrap();

    let qos = QosPolicyBuilder::new().build();

    let publisher = domain_participant.create_publisher(&qos).unwrap();
    let subscriber = domain_participant.create_subscriber(&qos).unwrap();

    let topic = domain_participant
        .create_topic(
            "/test".to_string(),
            "".to_string(),
            &qos,
            rustdds::TopicKind::NoKey,
        )
        .unwrap();

    let _qos = qos.clone();
    let _topic = topic.clone();
    let write_handle = thread::spawn(move || {
        let writer = publisher
            .create_datawriter_no_key_cdr::<PointCloud2>(&_topic, Some(_qos))
            .unwrap();

        let mut point_cloud = PointCloud2::default();
        point_cloud.data = iter::repeat(0).take(4 * 4 * 64 * 2048).collect(); // 2MiB

        writer.write(point_cloud, None).unwrap();
    });

    let _qos = qos.clone();
    let _topic = topic.clone();
    let read_handle = thread::spawn(move || {
        let mut reader = subscriber
            .create_datareader_no_key_cdr::<PointCloud2>(&_topic, Some(_qos))
            .unwrap();

        loop {
            match reader.read_next_sample() {
                Ok(Some(_point_cloud)) => {
                    // println!("{point_cloud:?}");
                    println!("point_cloud received"); // insert a breakpoint here
                    break;
                }
                Ok(None) => {
                    sleep(Duration::from_millis(10).into()); // insert a breakppoint here, let pass by this breakpoint several times and the issue will appear
                    continue;
                }
                Err(e) => eprintln!("{e:?}"),
            };
        }
    });

    write_handle.join().unwrap();
    read_handle.join().unwrap();
}
