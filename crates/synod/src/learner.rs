use std::collections::{HashMap, HashSet};

use crate::message::{LearnerMessage, NodeId, ProposalNumber};

#[derive(Debug, Clone)]
pub struct ProposalVotes<V> {
    value: V,
    voters: HashSet<NodeId>,
}

#[derive(Debug, Clone)]
pub struct Learner<V> {
    pub votes: HashMap<ProposalNumber, ProposalVotes<V>>,
    pub chosen: Option<V>,
}

pub struct LearnerOutput<V> {
    pub new_learner_state: Learner<V>,
    pub chosen: Option<V>,
}

impl<V: Clone + Eq> Learner<V> {
    pub fn transition(
        mut self,
        nodes: &[NodeId],
        from: NodeId,
        message: LearnerMessage<V>,
    ) -> LearnerOutput<V> {
        let LearnerMessage::Accepted {
            proposal_number,
            value,
        } = message;
        let proposal_votes = self
            .votes
            .entry(proposal_number)
            .or_insert_with(|| ProposalVotes {
                value: value.clone(),
                voters: HashSet::new(),
            });
        assert!(
            proposal_votes.value == value,
            "Malformed Message, value in proposal votes differs from Message::Accepted value"
        );

        proposal_votes.voters.insert(from);

        let chosen = (self.chosen.is_none() && proposal_votes.voters.len() * 2 > nodes.len())
            .then_some(value);
        if chosen.is_some() {
            self.chosen = chosen.clone();
        }

        LearnerOutput {
            new_learner_state: self,
            chosen,
        }
    }
}

#[cfg(test)]
mod tests;
