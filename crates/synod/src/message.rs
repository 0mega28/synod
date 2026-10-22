use std::cmp::Ordering;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(pub u32);

// TODO: how to build a ProposalNumber (constructor vs pub fields, and who
// picks the next round after a reject).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ProposalNumber {
    pub round: u32,
    pub node_id: NodeId,
}

impl Ord for ProposalNumber {
    fn cmp(&self, other: &Self) -> Ordering {
        self.round
            .cmp(&other.round)
            .then(self.node_id.cmp(&other.node_id))
    }
}

impl PartialOrd for ProposalNumber {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceptedProposal<V> {
    pub proposal_number: ProposalNumber,
    pub value: V,
}

/// Wire messages, grouped by the role that receives them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message<V> {
    ToAcceptor(AcceptorMessage<V>),
    ToProposer(ProposerMessage<V>),
    ToLearner(LearnerMessage<V>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AcceptorMessage<V> {
    Prepare {
        proposal_number: ProposalNumber,
    },
    Accept {
        proposal_number: ProposalNumber,
        value: V,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProposerMessage<V> {
    Promise {
        proposal_number: ProposalNumber,
        accepted: Option<AcceptedProposal<V>>,
    },
    Reject {
        promised: ProposalNumber,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LearnerMessage<V> {
    Accepted {
        proposal_number: ProposalNumber,
        value: V,
    },
}

impl<V> From<AcceptorMessage<V>> for Message<V> {
    fn from(message: AcceptorMessage<V>) -> Self {
        Message::ToAcceptor(message)
    }
}

impl<V> From<ProposerMessage<V>> for Message<V> {
    fn from(message: ProposerMessage<V>) -> Self {
        Message::ToProposer(message)
    }
}

impl<V> From<LearnerMessage<V>> for Message<V> {
    fn from(message: LearnerMessage<V>) -> Self {
        Message::ToLearner(message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pn(round: u32, node_id: u32) -> ProposalNumber {
        ProposalNumber {
            round,
            node_id: NodeId(node_id),
        }
    }

    #[test]
    fn higher_round_wins_regardless_of_node_id() {
        assert!(pn(2, 0) > pn(1, 4));
    }

    #[test]
    fn node_id_breaks_ties_within_a_round() {
        assert!(pn(1, 3) > pn(1, 2));
    }
}
