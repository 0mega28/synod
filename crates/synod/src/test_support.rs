use crate::message::{NodeId, ProposalNumber};

pub(crate) fn pn(round: u32, node_id: u32) -> ProposalNumber {
    ProposalNumber {
        round,
        node_id: NodeId(node_id),
    }
}

pub(crate) fn nodes() -> Vec<NodeId> {
    vec![NodeId(1), NodeId(2), NodeId(3)]
}
