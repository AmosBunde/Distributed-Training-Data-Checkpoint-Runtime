//! DataService: cached object reads and proxied checkpoint chunk uploads for
//! workers without direct storage credentials.

use crate::pb::data::data_service_server::DataService;
use crate::pb::data::upload_chunk_request::Msg;
use crate::pb::data::*;
use crate::{to_status, AppState};
use dtr_common::types::CheckpointToken;
use std::sync::Arc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

const DEFAULT_FRAME_BYTES: u64 = 1024 * 1024;

pub struct DataSvc {
    state: Arc<AppState>,
}

impl DataSvc {
    pub fn new(state: Arc<AppState>) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl DataService for DataSvc {
    async fn list_objects(
        &self,
        request: Request<ListObjectsRequest>,
    ) -> Result<Response<ListObjectsResponse>, Status> {
        let req = request.into_inner();
        self.state
            .auth(&req.worker_id, &req.session_token)
            .map_err(to_status)?;
        let objects = self
            .state
            .storage
            .list(&req.prefix)
            .await
            .map_err(to_status)?;
        Ok(Response::new(ListObjectsResponse {
            objects: objects
                .into_iter()
                .map(|o| ObjectInfo {
                    key: o.key,
                    size_bytes: o.size_bytes,
                })
                .collect(),
        }))
    }

    type ReadObjectStream = ReceiverStream<Result<ObjectFrame, Status>>;

    async fn read_object(
        &self,
        request: Request<ReadObjectRequest>,
    ) -> Result<Response<Self::ReadObjectStream>, Status> {
        let req = request.into_inner();
        self.state
            .auth(&req.worker_id, &req.session_token)
            .map_err(to_status)?;

        // Warm the cache for the keys the worker says it needs next.
        if !req.prefetch_hint.is_empty() {
            self.state.io.prefetch_keys(&req.prefetch_hint);
        }

        let frame_bytes = if req.frame_bytes == 0 {
            DEFAULT_FRAME_BYTES
        } else {
            req.frame_bytes
        } as usize;

        // Read through the cache (whole object; range slices when cached).
        let data = if req.length == 0 && req.offset == 0 {
            self.state.io.read(&req.key).await.map_err(to_status)?
        } else {
            let meta = self
                .state
                .storage
                .head(&req.key)
                .await
                .map_err(to_status)?
                .ok_or_else(|| Status::not_found(format!("object {} not found", req.key)))?;
            let end = if req.length == 0 {
                meta.size_bytes
            } else {
                (req.offset + req.length).min(meta.size_bytes)
            };
            self.state
                .io
                .read_range(&req.key, req.offset..end)
                .await
                .map_err(to_status)?
        };

        let base_offset = req.offset;
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        tokio::spawn(async move {
            let mut sent = 0usize;
            while sent < data.len() {
                let end = (sent + frame_bytes).min(data.len());
                let frame = ObjectFrame {
                    offset: base_offset + sent as u64,
                    data: data.slice(sent..end).to_vec(),
                };
                if tx.send(Ok(frame)).await.is_err() {
                    break; // client hung up
                }
                sent = end;
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn upload_checkpoint_chunk(
        &self,
        request: Request<Streaming<UploadChunkRequest>>,
    ) -> Result<Response<UploadChunkResponse>, Status> {
        let mut stream = request.into_inner();

        // First frame must be the header.
        let header = match stream.message().await? {
            Some(UploadChunkRequest {
                msg: Some(Msg::Header(h)),
            }) => h,
            _ => {
                return Err(Status::invalid_argument(
                    "first frame must be an UploadChunkHeader",
                ))
            }
        };
        self.state
            .auth(&header.worker_id, &header.session_token)
            .map_err(to_status)?;

        // Resolve (target, step) from the barrier token.
        let token = CheckpointToken::new(&header.checkpoint_token);
        let barrier = self
            .state
            .coordinator
            .barriers
            .get(&token)
            .map_err(to_status)?;
        let target = self.state.target(&barrier.target_id).map_err(to_status)?;

        // Accumulate data frames.
        let mut buf: Vec<u8> = Vec::new();
        while let Some(frame) = stream.message().await? {
            match frame.msg {
                Some(Msg::Data(data)) => buf.extend_from_slice(&data),
                Some(Msg::Header(_)) => {
                    return Err(Status::invalid_argument("duplicate header frame"))
                }
                None => {}
            }
        }
        if buf.is_empty() {
            return Err(Status::invalid_argument("chunk upload carried no data"));
        }

        let record = self
            .state
            .ckpt
            .upload_chunk(
                &target.prefix,
                barrier.step,
                header.rank,
                header.part_index,
                bytes::Bytes::from(buf),
            )
            .await
            .map_err(to_status)?;

        Ok(Response::new(UploadChunkResponse {
            key: record.key,
            size_bytes: record.size_bytes,
            sha256: record.sha256,
        }))
    }
}
