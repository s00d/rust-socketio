//! The engine.io app of `ci/engine-io.js` (`respond`, `close`) and `ci/engine-io-polling.js`
//! (also `respond_large`, `utf8`, `emoji`, `binary`).

use bytes::Bytes;

use super::engine::{Action, Ctx, Message};

#[derive(Debug)]
pub(super) struct Echo {
    /// `ci/engine-io-polling.js` answers more commands than `ci/engine-io.js`.
    pub polling_only: bool,
}

impl Echo {
    pub fn on_open(&self, ctx: &mut Ctx<'_>) {
        ctx.send(Message::Text("hello client".into()));
    }

    pub fn on_message(&self, message: Message, ctx: &mut Ctx<'_>) {
        let Message::Text(text) = message else {
            return;
        };
        let reply = match (text.as_str(), self.polling_only) {
            ("respond", _) => Message::Text("Roger Roger".into()),
            ("close", _) => return ctx.act(Action::Close),
            // larger than one HTTP body chunk: the polling response arrives in several frames
            ("respond_large", true) => Message::Text("x".repeat(64 * 1024)),
            ("utf8", true) => Message::Text("cash money €€€ てすと".into()),
            ("emoji", true) => Message::Text("😀🎉".into()),
            ("binary", true) => Message::Binary(Bytes::from_static(&[1, 2, 3])),
            _ => return,
        };
        ctx.send(reply);
    }
}
