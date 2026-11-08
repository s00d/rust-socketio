use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

/// The arguments of an event or an ack.
///
/// socket.io sends a list of arguments; each is JSON or binary. Received arguments arrive as:
/// - [`Payload::Text`]: JSON only (the common case),
/// - [`Payload::Binary`]: exactly one binary argument,
/// - [`Payload::Mixed`]: binary data anywhere else (several buffers, a buffer next to JSON
///   arguments, a buffer inside an object).
///
/// Build one with `From` (a [`Value`], a `Vec<Value>`, a string, bytes) or with
/// [`Payload::serialize`] for any `Serialize` type; read one with [`Payload::arg`] /
/// [`Payload::deserialize`].
#[derive(Debug, PartialEq, Eq, Clone)]
#[non_exhaustive]
pub enum Payload {
    /// One binary argument.
    Binary(Bytes),
    /// JSON arguments.
    Text(Vec<Value>),
    /// Arguments with binary data anywhere in them: each `{"_placeholder":true,"num":n}` in
    /// `args` stands for `attachments[n]` (socket.io-parser binary encoding). Build placeholders
    /// with [`Payload::placeholder`].
    Mixed {
        /// The JSON arguments, placeholders included.
        args: Vec<Value>,
        /// The binary attachments the placeholders refer to.
        attachments: Vec<Bytes>,
    },
}

impl Default for Payload {
    fn default() -> Self {
        Payload::Text(Vec::new())
    }
}

impl Payload {
    /// The placeholder for `attachments[num]` in [`Payload::Mixed`].
    pub fn placeholder(num: usize) -> Value {
        crate::protocol::socketio::placeholder(num)
    }

    /// No arguments (`socket.emit("ping")`).
    pub fn empty() -> Self {
        Payload::default()
    }

    /// One argument from any `Serialize` value.
    pub fn serialize<T: Serialize + ?Sized>(value: &T) -> serde_json::Result<Self> {
        Ok(Payload::Text(vec![serde_json::to_value(value)?]))
    }

    /// The JSON arguments (placeholders included for [`Payload::Mixed`]; empty for
    /// [`Payload::Binary`]).
    pub fn args(&self) -> &[Value] {
        match self {
            Payload::Text(args) | Payload::Mixed { args, .. } => args,
            Payload::Binary(_) => &[],
        }
    }

    /// The binary data: the one buffer of [`Payload::Binary`], the attachments of
    /// [`Payload::Mixed`].
    pub fn binary(&self) -> &[Bytes] {
        match self {
            Payload::Binary(bytes) => std::slice::from_ref(bytes),
            Payload::Mixed { attachments, .. } => attachments,
            Payload::Text(_) => &[],
        }
    }

    /// Number of arguments.
    pub fn len(&self) -> usize {
        match self {
            Payload::Binary(_) => 1,
            Payload::Text(args) | Payload::Mixed { args, .. } => args.len(),
        }
    }

    /// No arguments at all.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Argument `index` as `T` (`null` when it is missing, so `Option<T>` reads optional ones).
    pub fn arg<T: DeserializeOwned>(&self, index: usize) -> serde_json::Result<T> {
        T::deserialize(self.args().get(index).unwrap_or(&Value::Null))
    }

    /// All arguments as `T`: a tuple `(A, B)`, a `Vec<_>`, or a struct with one field per
    /// argument position.
    pub fn deserialize<T: DeserializeOwned>(&self) -> serde_json::Result<T> {
        T::deserialize(Value::Array(self.args().to_vec()))
    }
}

impl From<&str> for Payload {
    /// One string argument.
    fn from(string: &str) -> Self {
        Payload::Text(vec![Value::String(string.to_owned())])
    }
}

impl From<String> for Payload {
    /// One string argument (never parsed as JSON).
    fn from(string: String) -> Self {
        Payload::Text(vec![Value::String(string)])
    }
}

impl From<Vec<Value>> for Payload {
    /// One argument per value.
    fn from(values: Vec<Value>) -> Self {
        Payload::Text(values)
    }
}

impl From<Value> for Payload {
    /// One argument.
    fn from(value: Value) -> Self {
        Payload::Text(vec![value])
    }
}

impl From<()> for Payload {
    /// No arguments.
    fn from(_: ()) -> Self {
        Payload::default()
    }
}

macro_rules! from_scalar {
    ($($t:ty),*) => {$(
        impl From<$t> for Payload {
            /// One JSON argument.
            fn from(value: $t) -> Self {
                Payload::Text(vec![Value::from(value)])
            }
        }
    )*};
}
from_scalar!(bool, i8, i16, i32, i64, u8, u16, u32, u64, f32, f64);

impl From<Vec<u8>> for Payload {
    /// One binary argument.
    fn from(val: Vec<u8>) -> Self {
        Payload::Binary(Bytes::from(val))
    }
}

impl From<&'static [u8]> for Payload {
    /// One binary argument.
    fn from(val: &'static [u8]) -> Self {
        Payload::Binary(Bytes::from_static(val))
    }
}

impl From<Bytes> for Payload {
    /// One binary argument.
    fn from(bytes: Bytes) -> Self {
        Payload::Binary(bytes)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn strings_are_never_parsed_as_json() {
        assert_eq!(
            Payload::from("{\"a\":1}"),
            Payload::Text(vec![json!("{\"a\":1}")])
        );
        assert_eq!(
            Payload::from(String::from("foo ™")),
            Payload::Text(vec![json!("foo ™")])
        );
    }

    #[test]
    fn conversions() {
        assert_eq!(
            Payload::from(json!({"a": 1})),
            Payload::Text(vec![json!({"a": 1})])
        );
        assert_eq!(Payload::from(vec![json!(1), json!("x")]).len(), 2);
        assert_eq!(Payload::from(()), Payload::empty());
        assert!(Payload::empty().is_empty());
        assert_eq!(
            Payload::from(vec![1u8, 2]),
            Payload::Binary(Bytes::from_static(&[1, 2]))
        );
        assert_eq!(
            Payload::from(&[1u8][..]).binary(),
            &[Bytes::from_static(&[1])]
        );
        assert_eq!(
            Payload::serialize(&("x", 2)).unwrap(),
            Payload::Text(vec![json!(["x", 2])])
        );
    }

    #[test]
    fn typed_reads() {
        #[derive(serde::Deserialize, PartialEq, Debug)]
        struct Data {
            id: u32,
        }
        let payload = Payload::from(vec![json!({"id": 7}), json!("err")]);
        assert_eq!(payload.arg::<Data>(0).unwrap(), Data { id: 7 });
        assert_eq!(
            payload.arg::<Option<String>>(1).unwrap().as_deref(),
            Some("err")
        );
        assert_eq!(payload.arg::<Option<String>>(2).unwrap(), None);
        let (data, err): (Data, String) = payload.deserialize().unwrap();
        assert_eq!((data.id, err.as_str()), (7, "err"));
        assert!(payload.arg::<u32>(1).is_err());
    }
}
