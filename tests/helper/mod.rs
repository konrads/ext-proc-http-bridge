use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct Payload {
    pub method: String,
    pub value: u16,
}

pub mod mock_h2_node;
pub mod mock_handler;
pub mod mock_node;
