use std::cmp::Ordering;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(pub u32);

// TODO: how to build a ProposalNumber (constructor vs pub fields, and who
// picks the next round after a reject).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

#[derive(Debug, PartialEq, Eq)]
pub enum Message<V> {
    Prepare {
        proposal_number: ProposalNumber,
    },
    Promise {
        proposal_number: ProposalNumber,
        accepted: Option<AcceptedProposal<V>>,
    },
    Reject {
        promised: ProposalNumber,
    },
    Accept {
        proposal_number: ProposalNumber,
        value: V,
    },
    Accepted {
        proposal_number: ProposalNumber,
        value: V,
    },
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
