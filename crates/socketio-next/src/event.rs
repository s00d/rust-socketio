use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use serde::de::DeserializeOwned;
use tokio::sync::mpsc;

use crate::actor::Command;
use crate::protocol::socketio::Packet;
use crate::{Error, Payload, Result};

/// An event from the server: its name, its arguments and, when the server asked for one, the
/// means to acknowledge it.
#[derive(Debug, Clone)]
pub struct Event {
    name: String,
    payload: Payload,
    ack: Option<AckSender>,
}

impl Event {
    pub(crate) fn new(name: String, payload: Payload, ack: Option<AckSender>) -> Self {
        Event { name, payload, ack }
    }

    /// The event name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The arguments.
    pub fn payload(&self) -> &Payload {
        &self.payload
    }

    /// The arguments, owned.
    pub fn into_payload(self) -> Payload {
        self.payload
    }

    /// Argument `index` as `T` (see [`Payload::arg`]).
    pub fn arg<T: DeserializeOwned>(&self, index: usize) -> Result<T> {
        Ok(self.payload.arg(index)?)
    }

    /// The server emitted this event with a callback (`socket.emit("ev", data, (reply) => ..)`).
    pub fn wants_ack(&self) -> bool {
        self.ack.is_some()
    }

    /// Answers the server's callback with `data`. Sent at most once: later calls (and calls on
    /// clones) are ignored, as in socket.io-client. A no-op when no ack was requested.
    pub fn ack(&self, data: impl Into<Payload>) -> Result<()> {
        match &self.ack {
            Some(ack) => ack.send(data),
            None => Ok(()),
        }
    }

    /// The ack handle, to answer later or from another task.
    pub fn ack_sender(&self) -> Option<AckSender> {
        self.ack.clone()
    }
}

/// Answers one server callback. Clones share the "already sent" flag.
#[derive(Debug, Clone)]
pub struct AckSender {
    id: u64,
    nsp: Arc<str>,
    cmd: mpsc::UnboundedSender<Command>,
    sent: Arc<AtomicBool>,
}

impl AckSender {
    pub(crate) fn new(id: u64, nsp: Arc<str>, cmd: mpsc::UnboundedSender<Command>) -> Self {
        AckSender {
            id,
            nsp,
            cmd,
            sent: Arc::default(),
        }
    }

    /// The ack id the server chose.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Sends the ack (ACK, or BINARY_ACK when `data` has binary parts). Only the first call
    /// sends.
    pub fn send(&self, data: impl Into<Payload>) -> Result<()> {
        if self.sent.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        let packet = Packet::new_ack(data.into(), &self.nsp, self.id)?;
        self.cmd
            .send(Command::Send { packet })
            .map_err(|_| Error::Closed)
    }
}
