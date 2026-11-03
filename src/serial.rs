//! Events that carry objects of different classes through the same queue.

use crate::client::GlassClient;
use crate::error::{Error, Result};
use crate::event::Event;
use crate::types::*;
use crate::wire::Bytes;
use std::any::Any;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

/// An object that can travel through an [`EventSerializable`].
pub trait Serializable: Any + Send {
    /// The name its class was registered with.
    fn class_name(&self) -> &str;
    fn pack(&self) -> Vec<u8>;
    /// Allows getting the concrete class back: `object.as_any().downcast_ref()`.
    fn as_any(&self) -> &dyn Any;
}

type Factory = Box<dyn Fn(&[u8]) -> Option<Box<dyn Serializable>> + Send + Sync>;

/// The classes a client knows how to rebuild from the network.
#[derive(Default)]
pub struct ObjectRegistry {
    factories: BTreeMap<String, Factory>,
}

impl ObjectRegistry {
    pub fn new() -> ObjectRegistry {
        ObjectRegistry::default()
    }

    /// Registers how to rebuild objects of a class. Returns false if the name
    /// is invalid or is already taken.
    pub fn register<F>(&mut self, class_name: &str, factory: F) -> bool
    where
        F: Fn(&[u8]) -> Option<Box<dyn Serializable>> + Send + Sync + 'static,
    {
        if class_name.is_empty()
            || class_name.contains('\0')
            || self.factories.contains_key(class_name)
        {
            return false;
        }
        self.factories
            .insert(class_name.to_owned(), Box::new(factory));
        true
    }

    fn build(&self, class_name: &str, data: &[u8]) -> Option<Box<dyn Serializable>> {
        self.factories.get(class_name)?(data)
    }
}

fn encode(object: &dyn Serializable) -> Bytes {
    let mut out = object.class_name().as_bytes().to_vec();
    out.push(0);
    out.extend_from_slice(&object.pack());
    Bytes(out)
}

fn decode(registry: &ObjectRegistry, bytes: &[u8]) -> Result<Box<dyn Serializable>> {
    let end = bytes.iter().position(|&b| b == 0).ok_or(Error::Decode)?;
    let class_name = std::str::from_utf8(&bytes[..end]).map_err(|_| Error::Decode)?;
    registry
        .build(class_name, &bytes[end + 1..])
        .ok_or(Error::Decode)
}

/// An [`Event`] whose objects can be of any registered class.
pub struct EventSerializable {
    event: Event<Bytes>,
    registry: Arc<ObjectRegistry>,
}

impl EventSerializable {
    pub fn new(
        client: &GlassClient,
        name: &str,
        registry: Arc<ObjectRegistry>,
        mode: Mode,
    ) -> Result<EventSerializable> {
        Ok(EventSerializable {
            event: Event::new(client, name, mode)?,
            registry,
        })
    }

    /// Sends an object to a node or group, such as [`EVERYBODY_ID`].
    pub fn enqueue(&self, object: &dyn Serializable, target: NodeId) -> Result<()> {
        self.event.enqueue(&encode(object), target, false)
    }

    /// Removes the first object in the queue, if any. An object of an unknown
    /// class is an error, and it is removed anyway.
    pub fn try_get(&self) -> Result<Option<Box<dyn Serializable>>> {
        match self.event.try_get()? {
            Some(Bytes(bytes)) => decode(&self.registry, &bytes).map(Some),
            None => Ok(None),
        }
    }

    /// Waits for an object and removes it from the queue.
    pub fn get_timeout(&self, timeout: Duration) -> Result<Box<dyn Serializable>> {
        let Bytes(bytes) = self.event.get_timeout(timeout)?;
        decode(&self.registry, &bytes)
    }

    pub fn is_empty(&self) -> bool {
        self.event.is_empty()
    }

    pub fn queue_size(&self) -> usize {
        self.event.queue_size()
    }

    pub fn clear(&self) {
        self.event.clear();
    }

    /// Calls `handler` for each object, instead of queueing them. It runs on the
    /// client's reading thread. Objects of unknown classes are discarded.
    pub fn set_handler<F>(&self, mut handler: F)
    where
        F: FnMut(Box<dyn Serializable>) + Send + 'static,
    {
        let registry = self.registry.clone();
        self.event
            .set_handler(move |Bytes(bytes)| match decode(&registry, &bytes) {
                Ok(object) => handler(object),
                Err(e) => log::warn!("discarding object: {e}"),
            });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Point(i32);

    impl Serializable for Point {
        fn class_name(&self) -> &str {
            "Point"
        }
        fn pack(&self) -> Vec<u8> {
            self.0.to_be_bytes().to_vec()
        }
        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    fn registry() -> ObjectRegistry {
        let mut registry = ObjectRegistry::new();
        assert!(registry.register("Point", |bytes| {
            let value = i32::from_be_bytes(bytes.try_into().ok()?);
            Some(Box::new(Point(value)))
        }));
        registry
    }

    #[test]
    fn objects_round_trip() {
        let Bytes(bytes) = encode(&Point(7));
        let object = decode(&registry(), &bytes).unwrap();
        let point = object.as_any().downcast_ref::<Point>().unwrap();
        assert_eq!(point.0, 7);
    }

    #[test]
    fn registering_validates_the_name() {
        let mut registry = registry();
        assert!(!registry.register("Point", |_| None));
        assert!(!registry.register("", |_| None));
        assert!(!registry.register("a\0b", |_| None));
    }

    #[test]
    fn unknown_class_and_bad_data_are_errors() {
        let registry = registry();
        assert!(matches!(decode(&registry, b"Other\0"), Err(Error::Decode)));
        assert!(matches!(
            decode(&registry, b"Point\0abc"),
            Err(Error::Decode)
        ));
        assert!(matches!(decode(&registry, b"no nul"), Err(Error::Decode)));
    }
}
