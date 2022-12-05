use std::borrow::BorrowMut;
use std::time::Duration;

use anyhow::anyhow;
use etcd_client::{Client, ConnectOptions, Error, LeaseClient};
use risingwave_pb::meta::MetaLeaderInfo;
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time;
use tokio_stream::StreamExt;

use crate::MetaResult;

pub struct ElectionContext {
    pub events: watch::Receiver<Option<MetaLeaderInfo>>,
    pub handle: JoinHandle<()>,
    pub stop_sender: oneshot::Sender<()>,
    pub leader: MetaLeaderInfo,
}

#[async_trait::async_trait]
pub trait ElectionClient {
    async fn start(&self, id: String, lease_ttl: i64) -> MetaResult<ElectionContext>;
}

pub struct MemoryElectionClient {}

impl MemoryElectionClient {
    pub fn new() -> Self {
        MemoryElectionClient {}
    }
}

#[async_trait::async_trait]
impl ElectionClient for MemoryElectionClient {
    async fn start(&self, id: String, _lease_ttl: i64) -> MetaResult<ElectionContext> {
        let (sender, receiver) = tokio::sync::watch::channel(None);
        let (stop_sender, stop_receiver) = oneshot::channel();
        let leader = MetaLeaderInfo {
            node_address: id,
            lease_id: 0,
        };

        let _leader = leader.clone();
        let handle = tokio::spawn(async move {
            sender.send(Some(_leader)).unwrap();
            stop_receiver.await.unwrap();
        });

        let result = Ok(ElectionContext {
            events: receiver,
            handle,
            stop_sender,
            leader,
        });
        return result;
    }
}

pub struct EtcdElectionClient {
    pub client: Client,
}

struct EtcdLeaseSession {
    lease_id: i64,
    ttl: i64,
    handle: JoinHandle<()>,
    client: LeaseClient,
}

impl EtcdLeaseSession {
    async fn drop_session(mut self) {
        let _ = self.client.revoke(self.lease_id).await;
        self.handle.abort()
    }

    async fn replace_lease(&mut self, lease_id: i64) -> MetaResult<()> {
        self.handle.abort();

        let mut lease_client = self.client.clone();

        let ttl = self.ttl;
        let handle = tokio::spawn(async move {
            Self::lease_keep_loop(
                &mut lease_client,
                lease_id,
                Duration::from_secs((ttl / 2) as u64),
            )
            .await
        });

        self.lease_id = lease_id;
        self.handle = handle;

        Ok(())
    }

    pub async fn lease_keep_loop(
        lease_client: &mut LeaseClient,
        lease_id: i64,
        period: Duration,
    ) -> ! {
        let mut ticker = time::interval(period);

        let (mut lease_keeper, _lease_keep_alive_stream) =
            lease_client.keep_alive(lease_id).await.unwrap();

        loop {
            ticker.tick().await;
            if let Err(e) = lease_keeper.keep_alive().await {
                println!("lease keeper failed {}", e);

                let (new_keeper, _stream) = match lease_client.keep_alive(lease_id).await {
                    Ok((a, b)) => (a, b),
                    Err(e) => {
                        println!("rebuild lease keeper failed {}", e);

                        continue;
                    }
                };

                lease_keeper = new_keeper;
            }
        }
    }
}

const META_ELECTION_KEY: &str = "ELECTION";

impl EtcdElectionClient {
    async fn lease_session(&self, ttl: i64) -> MetaResult<EtcdLeaseSession> {
        let mut lease_client = self.client.lease_client();
        let lease_id = lease_client.grant(ttl, None).await.unwrap();

        let lease_id = lease_id.id();

        let handle = tokio::spawn(async move {
            EtcdLeaseSession::lease_keep_loop(
                &mut lease_client,
                lease_id,
                Duration::from_secs((ttl / 2) as u64),
            )
            .await
        });

        Ok(EtcdLeaseSession {
            lease_id,
            ttl,
            handle,
            client: self.client.lease_client(),
        })
    }

    fn value_to_address(value: &[u8]) -> String {
        String::from_utf8_lossy(value).to_string()
    }

    async fn run(
        &self,
        stop: oneshot::Receiver<()>,
        value: &Vec<u8>,
        lease_ttl: i64,
    ) -> MetaResult<(
        JoinHandle<()>,
        watch::Receiver<Option<MetaLeaderInfo>>,
        MetaLeaderInfo,
    )> {
        #[derive(Debug, Clone, Copy)]
        enum State {
            Init,
            Reconnect,
            Campaign,
            Observe,
        }

        let mut _client = self.client.clone();
        let mut client = self.client.clone();
        let mut stop = stop;

        let mut lease_session = self.lease_session(lease_ttl).await.expect("lease ");

        let (sender, receiver) = watch::channel::<Option<MetaLeaderInfo>>(None);
        let event_sender = sender;

        let id = value.clone();

        let fallback_timeout = Duration::from_secs(1);
        let join_handle = tokio::spawn(async move {
            let mut state = State::Init;
            let mut current_leader = None;

            let lease = lease_session.lease_id;

            tracing::info!("init election with lease id {}", lease);

            let mut ticker = time::interval(fallback_timeout);
            loop {
                ticker.tick().await;

                tracing::info!("current etcd election state {:?}", state);

                match state {
                    State::Init => {
                        let leader_kv = match client.leader(META_ELECTION_KEY).await {
                            Ok(mut leader) => match leader.take_kv() {
                                None => continue,
                                Some(kv) => kv,
                            },
                            Err(Error::GRpcStatus(e)) if e.message() == "election: no leader" => {
                                tracing::info!("no leader now, run election");

                                // no leader now
                                state = State::Campaign;
                                continue;
                            }
                            Err(e) => {
                                tracing::error!("error happened when calling leader {}", e);

                                // todo, continue is ok?
                                state = State::Reconnect;
                                continue;
                            }
                        };

                        let discovered_leader_id = leader_kv.value().to_vec();

                        if match &mut current_leader {
                            None => true,
                            Some(current_leader_id) => *current_leader_id != discovered_leader_id,
                        } {
                            event_sender
                                .send(Some(MetaLeaderInfo {
                                    node_address: Self::value_to_address(leader_kv.value()),
                                    lease_id: leader_kv.lease() as u64,
                                }))
                                .unwrap();

                            current_leader = Some(discovered_leader_id);
                        }

                        if leader_kv.value() == id.as_slice() {
                            lease_session
                                .replace_lease(leader_kv.lease())
                                .await
                                .unwrap();

                            state = State::Observe
                        } else {
                            state = State::Campaign
                        }
                    }
                    State::Campaign => {
                        tokio::select! {
                            resp = client.campaign(META_ELECTION_KEY, id.clone(), lease) => {
                                match resp {
                                    Ok(_) => {
                                        tracing::info!("election campaign done, changing state to observe mode");
                                        state = State::Observe;
                                    }
                                    Err(e) => {
                                        tracing::error!("election campaign failed due tp {}, changing state to observe mode", e.to_string());
                                        state = State::Reconnect;
                                    }
                                }
                                continue
                            }
                            _ = stop.borrow_mut() => {
                                tracing::info!("stopping election");
                                lease_session.drop_session().await;
                                return
                            }
                        }
                    }
                    State::Observe => {
                        let mut observe_stream = client
                            .observe(META_ELECTION_KEY)
                            .await
                            .expect("creating observe stream failed");

                        let mut ticker =
                            time::interval(Duration::from_secs((lease_ttl / 2) as u64));

                        loop {
                            ticker.tick().await;

                            tokio::select! {
                                _ = stop.borrow_mut() => {
                                    return
                                }

                                leader_resp = observe_stream.next() => {
                                    let leader_resp =
                                        leader_resp.expect("observe stream should never stop");

                                    let leader_kv = match leader_resp {
                                        Ok(mut leader) => match leader.take_kv() {
                                            Some(kv) => kv,
                                            None => continue,
                                        },
                                        Err(e) => {
                                            tracing::info!("observing stream failed {}", e.to_string());
                                            state = State::Reconnect;

                                            break;
                                        }
                                    };

                                    if leader_kv.value() != id.as_slice() {
                                        panic!("leader changed!");
                                    }
                                }
                            }
                        }
                    }

                    State::Reconnect => {
                        let mut ticker = time::interval(fallback_timeout);

                        loop {
                            ticker.tick().await;
                            match client.status().await {
                                Ok(s) => {
                                    tracing::info!("etcd election server status is {:?}", s);
                                    state = State::Init;
                                    break;
                                }
                                Err(e) => {
                                    tracing::error!(
                                        "etcd election server status is failed {}",
                                        e.to_string()
                                    );
                                }
                            }
                        }
                    }
                }
            }
        });

        let mut ticker = time::interval(fallback_timeout);

        let init_leader_kv = loop {
            ticker.tick().await;
            let leader_resp = _client.leader(META_ELECTION_KEY).await;
            match leader_resp {
                Ok(mut leader) => {
                    let kv = match leader.take_kv() {
                        None => continue,
                        Some(kv) => kv,
                    };

                    break kv;
                }
                Err(Error::GRpcStatus(e)) if e.message() == "election: no leader" => {
                    tracing::info!("waiting for first leader");
                    continue;
                }
                Err(e) => {
                    tracing::error!("waiting for first leader failed {}", e.to_string());
                    continue;
                }
            };
        };

        Ok((
            join_handle,
            receiver,
            MetaLeaderInfo {
                node_address: Self::value_to_address(init_leader_kv.value()),
                lease_id: init_leader_kv.lease() as u64,
            },
        ))
    }
}

#[async_trait::async_trait]
impl ElectionClient for EtcdElectionClient {
    async fn start(&self, id: String, lease_ttl: i64) -> MetaResult<ElectionContext> {
        let (stop_sender, stop_recv) = oneshot::channel::<()>();

        let value = id.as_bytes().to_vec();

        let (handle, receiver, leader) = self
            .run(stop_recv, &value, lease_ttl)
            .await
            .map_err(|e| anyhow!(e))?;

        Ok(ElectionContext {
            leader,
            events: receiver,
            handle,
            stop_sender,
        })
    }
}

impl EtcdElectionClient {
    pub(crate) async fn new(
        endpoints: Vec<String>,
        options: Option<ConnectOptions>,
        auth_enabled: bool,
    ) -> MetaResult<Self> {
        assert!(!auth_enabled, "auth not supported");

        let client = Client::connect(&endpoints, options.clone()).await.unwrap();

        Ok(Self { client })
    }
}
