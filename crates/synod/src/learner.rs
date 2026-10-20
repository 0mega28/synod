use std::collections::{HashMap, HashSet};

use crate::message::{Message, NodeId, ProposalNumber};

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
        message: Message<V>,
    ) -> LearnerOutput<V> {
        if let Message::Accepted {
            proposal_number,
            value,
        } = message
        {
            let proposal_votes =
                self.votes
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

            return LearnerOutput {
                new_learner_state: self,
                chosen,
            };
        }
        unreachable!(
            "Learner only handles Message::Accepted but received {}",
            message.kind()
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{nodes, pn};

    fn fresh() -> Learner<&'static str> {
        Learner {
            votes: HashMap::new(),
            chosen: None,
        }
    }

    /// Feeds `(from, round, node_id, value)` accepts in order and returns the
    /// final state plus what each message reported as newly chosen.
    fn feed(
        accepts: &[(u32, u32, u32, &'static str)],
    ) -> (Learner<&'static str>, Vec<Option<&'static str>>) {
        let mut learner = fresh();
        let mut reported = Vec::new();
        for &(from, round, node_id, value) in accepts {
            let output = learner.transition(
                &nodes(),
                NodeId(from),
                Message::Accepted {
                    proposal_number: pn(round, node_id),
                    value,
                },
            );
            learner = output.new_learner_state;
            reported.push(output.chosen);
        }
        (learner, reported)
    }

    #[test]
    fn one_accepted_is_not_chosen() {
        let (learner, reported) = feed(&[(1, 4, 1, "y")]);

        assert_eq!(reported, vec![None]);
        assert_eq!(learner.chosen, None);
    }

    #[test]
    fn majority_for_one_proposal_is_chosen() {
        let (learner, reported) = feed(&[(1, 4, 1, "y"), (2, 4, 1, "y")]);

        assert_eq!(reported, vec![None, Some("y")]);
        assert_eq!(learner.chosen, Some("y"));
    }

    #[test]
    fn repeated_accepted_from_one_node_counts_once() {
        let (learner, reported) = feed(&[(1, 4, 1, "y"), (1, 4, 1, "y")]);

        assert_eq!(reported, vec![None, None]);
        assert_eq!(learner.chosen, None);
    }

    #[test]
    fn accepted_after_chosen_reports_nothing() {
        let (learner, reported) = feed(&[(1, 4, 1, "y"), (2, 4, 1, "y"), (3, 4, 1, "y")]);

        assert_eq!(reported, vec![None, Some("y"), None]);
        assert_eq!(learner.chosen, Some("y"));
    }

    #[test]
    fn duplicate_after_chosen_reports_nothing() {
        let (learner, reported) = feed(&[
            (1, 4, 1, "y"),
            (2, 4, 1, "y"),
            (3, 4, 1, "y"),
            (1, 4, 1, "y"),
        ]);

        assert_eq!(reported, vec![None, Some("y"), None, None]);
        assert_eq!(learner.chosen, Some("y"));
    }

    #[test]
    fn accepts_for_different_numbers_do_not_add_up() {
        let (learner, reported) = feed(&[(1, 4, 1, "y"), (2, 5, 2, "y")]);

        assert_eq!(reported, vec![None, None]);
        assert_eq!(learner.chosen, None);
    }

    #[test]
    fn earlier_vote_still_counts_after_a_higher_accept() {
        let (learner, reported) = feed(&[(1, 4, 1, "y"), (1, 5, 3, "y"), (2, 4, 1, "y")]);

        assert_eq!(reported, vec![None, None, Some("y")]);
        assert_eq!(learner.chosen, Some("y"));
    }

    #[test]
    fn value_chosen_again_at_a_higher_round_is_not_reported() {
        let (learner, reported) = feed(&[
            (1, 4, 1, "y"),
            (2, 4, 1, "y"),
            (1, 5, 3, "y"),
            (2, 5, 3, "y"),
        ]);

        assert_eq!(reported, vec![None, Some("y"), None, None]);
        assert_eq!(learner.chosen, Some("y"));
    }

    #[test]
    #[should_panic]
    fn two_values_under_one_number_panics() {
        feed(&[(1, 4, 1, "y"), (2, 4, 1, "z")]);
    }
}
