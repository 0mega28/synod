use crate::message::{Message, NodeId};

pub trait Transport<V> {
    fn send_message(&self, node_id: NodeId, message: Message<V>) -> ();
    fn receive_message(&mut self, receive_callback: Box<dyn FnMut(Message<V>)>) -> ();
}
