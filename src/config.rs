#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
pub struct TopicMapping {
    pub from: String,
    pub to: String,
    pub use_offsets_from_dest: bool,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
pub struct ServerConfig {
    pub health_check_port: u16,
    pub consumer_brokers: Vec<String>,
    pub producer_brokers: Vec<String>,
    pub topic_mappings: Vec<TopicMapping>,
    pub fetch_max_wait_time_millis: u64,
    pub thread_count: usize,
}
