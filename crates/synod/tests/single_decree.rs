use std::{collections::HashMap, vec};

use rand::{RngExt, SeedableRng, rngs::StdRng};
use synod::{
    message::{Message, NodeId},
    node::{Node, NodeEvent, NodeOutput},
};

type MsgType = u32;
type MsgQueueEle = (NodeId, NodeId, Message<MsgType>);
type MsgQueue = Vec<MsgQueueEle>;

#[test]
fn one_proposer_three_nodes_reach_the_same_done() {
    let seed = std::env::var("SEED")
        .ok()
        .and_then(|seed| seed.parse().ok())
        .unwrap_or_else(rand::random::<u64>);
    println!("seed: {seed}");
    let mut rng = StdRng::seed_from_u64(seed);

    let node_ids = vec![NodeId(1), NodeId(2), NodeId(3)];
    let mut nodes: HashMap<NodeId, Node<MsgType>> = node_ids
        .iter()
        .map(|id| (*id, Node::new(*id, node_ids.clone())))
        .collect();

    let mut message_queue: MsgQueue = vec![];
    let mut dones: Vec<(NodeId, MsgType)> = vec![];

    let node_1_key = node_ids[0];
    let (messages, done) = step(
        &mut nodes,
        node_1_key,
        NodeEvent::ClientRequest { value: 42 },
    );
    message_queue.extend(messages);
    dones.extend(done.map(|value| (node_1_key, value)));

    while let Some((from, node_id, message)) =
        random_message_from_queue(&mut message_queue, &mut rng)
    {
        println!("From [{from:?}] To [{node_id:?}]\n Message: [{message:?}]\n-----");
        let (messages, done) = step(&mut nodes, node_id, NodeEvent::Message { from, message });
        message_queue.extend(messages);
        dones.extend(done.map(|value| (node_id, value)));
    }

    dones.sort();
    assert_eq!(
        dones,
        vec![(NodeId(1), 42), (NodeId(2), 42), (NodeId(3), 42)]
    );
}

fn step(
    nodes: &mut HashMap<NodeId, Node<MsgType>>,
    node_id: NodeId,
    event: NodeEvent<MsgType>,
) -> (Vec<MsgQueueEle>, Option<MsgType>) {
    let node = nodes.remove(&node_id).unwrap();
    let NodeOutput {
        new_node_state,
        messages,
        done,
        ..
    } = node.transition(event);
    nodes.insert(node_id, new_node_state);
    let messages = messages
        .into_iter()
        .map(|(to, message)| (node_id, to, message))
        .collect();
    (messages, done)
}

fn random_message_from_queue(
    message_queue: &mut MsgQueue,
    rng: &mut StdRng,
) -> Option<MsgQueueEle> {
    if message_queue.is_empty() {
        return None;
    }
    let index = rng.random_range(0..message_queue.len());
    Some(message_queue.swap_remove(index))
}
