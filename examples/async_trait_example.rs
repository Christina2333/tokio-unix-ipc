use serde_::{Deserialize, Serialize};
use std::{env, process};
use std::sync::{Arc};
use bytes::Bytes;
use tokio_unix_ipc::{Bootstrapper, channel, Receiver, Sender};
use crate::Location::Ip;

const ENV_VAR: &str = "PROC_CONNECT_TO";

#[derive(Serialize, Deserialize)]
#[serde(crate = "serde_")]
pub enum Task {
    Sum(Vec<i64>, Sender<i64>),
    MultiGet(MultiGetRequest, Sender<MultiGetResponse>),
    Shutdown
}
#[derive(Serialize, Deserialize, Debug)]
#[serde(crate = "serde_")]
pub enum Location {
    Ip(String),
    Fd(u32)
}
#[derive(Serialize, Deserialize, Debug)]
#[serde(crate = "serde_")]
pub struct Client {
    pub location: Option<Location>,
}
#[derive(Serialize, Deserialize)]
#[serde(crate = "serde_")]
pub struct MultiGetRequest {
    pub client: Option<Client>,
    pub object_ids: Vec<String>
}
#[derive(Serialize, Deserialize, Debug)]
#[serde(crate = "serde_")]
pub struct MultiGetResponse {
    pub client: Option<Client>,
    pub object_ids: Vec<String>
}
#[derive(Serialize, Deserialize)]
#[serde(crate = "serde_")]
pub struct KvMetaResponse {
    pub object_id: String,
    /// 状态码；
    pub status: u32,
    pub message: String,
}

#[async_trait::async_trait]
pub trait TaskService {
    async fn sum(&self, vec: Vec<i64>) -> i64;
    async fn shutdown(&self);
    async fn multi_get(&self, req: MultiGetRequest) -> MultiGetResponse;
}

#[tokio::main]
async fn main() {
    if let Ok(path) = env::var(ENV_VAR) {
        let server = IpcReceiver::new(path).await;
        server.start().await;
    } else {
        let client = IpcSender::new();
        let mut child = process::Command::new(env::current_exe().unwrap())
            .env(ENV_VAR, client.get_path())
            .spawn()
            .unwrap();

        let sum = client.sum(vec![1, 2, 3]).await;
        println!("sum = {}", sum);

        let req = MultiGetRequest {
            client: None,
            object_ids: vec![]
        };
        let resp = client.multi_get(req).await;
        println!("resp = {:?}", resp);

        child.kill().ok();
        child.wait().ok();
    }
}


/// ipc_receiver
pub struct IpcReceiver {
    pub recv: Receiver<Task>,
}
impl IpcReceiver {
    pub async fn new(path: String) -> Self {
        let receiver = Receiver::<Task>::connect(path).await.unwrap();
        IpcReceiver {
            recv: receiver,
        }
    }
    pub async fn start(&self) {
        loop {
            let task = self.recv.recv().await.unwrap();
            match task {
                Task::Sum(vec, tx) => {
                    let sum = self.sum(vec).await;
                    tx.send(sum).await.unwrap();
                },
                Task::Shutdown => {
                    self.shutdown().await;
                    break
                }
                Task::MultiGet(req, tx) => {
                    let resp = self.multi_get(req).await;
                    tx.send(resp).await.unwrap();
                }
            }
        }
    }
}
#[async_trait::async_trait]
impl TaskService for IpcReceiver {
    async fn sum(&self, vec: Vec<i64>) -> i64 {
        vec.into_iter().sum::<i64>()
    }

    async fn shutdown(&self) {
    }

    async fn multi_get(&self, req: MultiGetRequest) -> MultiGetResponse {
        let client = Client {
            location: Some(Ip("192.168.1.1".parse().unwrap()))
        };
        MultiGetResponse {
            client: Some(client),
            object_ids: vec!["1".to_string(), "2".to_string()]
        }
    }
}

/// ipc_sender
pub struct IpcSender {
    pub bootstrap: Arc<Bootstrapper>,
}
impl IpcSender {
    pub fn new() -> Self {
        let bootstrapper = Bootstrapper::new().unwrap();
        IpcSender {
            bootstrap: Arc::new(bootstrapper)
        }
    }
    pub fn get_path(&self) -> String {
        String::from(self.bootstrap.get_path())
    }
}
#[async_trait::async_trait]
impl TaskService for IpcSender {
    async fn sum(&self, vec: Vec<i64>) -> i64 {
        let (tx, rx) = channel().unwrap();
        self.bootstrap.send(Task::Sum(vec, tx)).await.unwrap();
        rx.recv().await.unwrap()
    }

    async fn shutdown(&self) {
        self.bootstrap.send(Task::Shutdown).await.unwrap()
    }

    async fn multi_get(&self, req: MultiGetRequest) -> MultiGetResponse {
        let (tx, rx) = channel().unwrap();
        self.bootstrap.send(Task::MultiGet(req, tx)).await.unwrap();
        rx.recv().await.unwrap()
    }
}