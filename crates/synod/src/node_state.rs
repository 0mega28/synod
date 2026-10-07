use crate::message::{AcceptedProposal, Message, NodeId, ProposalNumber};
use std::cmp::Ordering;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Acceptor<V> {
    pub promised: Option<ProposalNumber>,
    pub accepted_proposal: Option<AcceptedProposal<V>>,
}

#[derive(Debug)]
pub struct Proposer {
    pub highest_proposal_number: Option<ProposalNumber>,
}

#[derive(Debug)]
pub struct AcceptorOutput<V> {
    pub new_acceptor_state: Acceptor<V>,
    pub reply: Option<(NodeId, Message<V>)>,
    pub persist: bool,
}

impl<V: Clone> Acceptor<V> {
    pub fn transition(&self, sender_node_id: NodeId, message: Message<V>) -> AcceptorOutput<V> {
        let Acceptor {
            promised,
            accepted_proposal,
        } = self;
        match message {
            Message::Prepare { proposal_number } => {
                if let Some(promised) = promised {
                    return match promised.cmp(&proposal_number) {
                        Ordering::Less | Ordering::Equal => AcceptorOutput {
                            new_acceptor_state: Acceptor {
                                promised: Some(proposal_number),
                                accepted_proposal: accepted_proposal.clone(),
                            },
                            reply: Some((
                                sender_node_id,
                                Message::Promise {
                                    proposal_number,
                                    accepted: accepted_proposal.clone(),
                                },
                            )),
                            persist: true,
                        },
                        Ordering::Greater => AcceptorOutput {
                            new_acceptor_state: self.clone(),
                            reply: Some((
                                sender_node_id,
                                Message::Reject {
                                    promised: *promised,
                                },
                            )),
                            persist: false,
                        },
                    };
                }
                AcceptorOutput {
                    new_acceptor_state: Acceptor {
                        promised: Some(proposal_number),
                        accepted_proposal: accepted_proposal.clone(),
                    },
                    reply: Some((
                        sender_node_id,
                        Message::Promise {
                            proposal_number,
                            accepted: accepted_proposal.clone(),
                        },
                    )),
                    persist: true,
                }
            }
            Message::Accept {
                proposal_number,
                value,
            } => {
                if let Some(promised) = promised
                    && *promised > proposal_number
                {
                    return AcceptorOutput {
                        new_acceptor_state: self.clone(),
                        reply: Some((
                            sender_node_id,
                            Message::Reject {
                                promised: *promised,
                            },
                        )),
                        persist: false,
                    };
                }
                AcceptorOutput {
                    new_acceptor_state: Acceptor {
                        promised: Some(proposal_number),
                        accepted_proposal: Some(AcceptedProposal {
                            proposal_number,
                            value: value.clone(),
                        }),
                    },
                    reply: Some((
                        sender_node_id,
                        Message::Accepted {
                            proposal_number,
                            value,
                        },
                    )),
                    persist: true,
                }
            }
            Message::Promise { .. } => {
                unreachable!("Acceptor shouldn't receive Promise messages");
            }
            Message::Reject { .. } => {
                unreachable!("Acceptor shouldn't receive Reject messages");
            }
            Message::Accepted { .. } => {
                unreachable!("Acceptor shouldn't receive Accepted messages");
            }
        }
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

    fn fresh() -> Acceptor<&'static str> {
        Acceptor {
            promised: None,
            accepted_proposal: None,
        }
    }

    fn prepare(proposal_number: ProposalNumber) -> Message<&'static str> {
        Message::Prepare { proposal_number }
    }

    fn accept(proposal_number: ProposalNumber, value: &'static str) -> Message<&'static str> {
        Message::Accept {
            proposal_number,
            value,
        }
    }

    #[test]
    fn promise_reports_previously_accepted_proposal() {
        let acceptor = fresh();

        let acceptor = acceptor
            .transition(NodeId(1), prepare(pn(1, 1)))
            .new_acceptor_state;
        let acceptor = acceptor
            .transition(NodeId(1), accept(pn(1, 1), "x"))
            .new_acceptor_state;
        let output = acceptor.transition(NodeId(2), prepare(pn(2, 2)));

        assert_eq!(
            output.reply,
            Some((
                NodeId(2),
                Message::Promise {
                    proposal_number: pn(2, 2),
                    accepted: Some(AcceptedProposal {
                        proposal_number: pn(1, 1),
                        value: "x",
                    }),
                },
            ))
        );
    }

    #[test]
    fn lower_prepare_after_promise_is_rejected() {
        let acceptor = fresh();

        let acceptor = acceptor
            .transition(NodeId(2), prepare(pn(2, 2)))
            .new_acceptor_state;
        let output = acceptor.transition(NodeId(1), prepare(pn(1, 1)));

        assert_eq!(
            output.reply,
            Some((NodeId(1), Message::Reject { promised: pn(2, 2) }))
        );
    }

    #[test]
    fn accept_below_promise_is_rejected() {
        let acceptor = fresh();

        let acceptor = acceptor
            .transition(NodeId(2), prepare(pn(2, 2)))
            .new_acceptor_state;
        let output = acceptor.transition(NodeId(1), accept(pn(1, 1), "x"));

        assert_eq!(
            output.reply,
            Some((NodeId(1), Message::Reject { promised: pn(2, 2) }))
        );
    }

    #[test]
    fn accept_is_answered_with_accepted() {
        let acceptor = fresh();

        let acceptor = acceptor
            .transition(NodeId(1), prepare(pn(1, 1)))
            .new_acceptor_state;
        let output = acceptor.transition(NodeId(1), accept(pn(1, 1), "x"));

        assert_eq!(
            output.reply,
            Some((
                NodeId(1),
                Message::Accepted {
                    proposal_number: pn(1, 1),
                    value: "x",
                },
            ))
        );
    }

    #[test]
    fn accept_raises_promise_so_lower_accept_is_rejected() {
        let acceptor = fresh();

        let acceptor = acceptor
            .transition(NodeId(3), prepare(pn(3, 3)))
            .new_acceptor_state;
        let acceptor = acceptor
            .transition(NodeId(2), accept(pn(5, 2), "x"))
            .new_acceptor_state;
        let output = acceptor.transition(NodeId(1), accept(pn(4, 1), "y"));

        assert_eq!(
            output.reply,
            Some((NodeId(1), Message::Reject { promised: pn(5, 2) }))
        );
        assert_eq!(
            output.new_acceptor_state.accepted_proposal,
            Some(AcceptedProposal {
                proposal_number: pn(5, 2),
                value: "x",
            })
        );
    }

    #[test]
    fn fresh_acceptor_answers_accept_with_accepted() {
        let output = fresh().transition(NodeId(1), accept(pn(1, 1), "x"));

        assert_eq!(
            output.reply,
            Some((
                NodeId(1),
                Message::Accepted {
                    proposal_number: pn(1, 1),
                    value: "x",
                },
            ))
        );
    }

    #[test]
    fn duplicate_prepare_leaves_state_unchanged() {
        let acceptor = fresh()
            .transition(NodeId(1), prepare(pn(1, 1)))
            .new_acceptor_state;

        let output = acceptor.transition(NodeId(1), prepare(pn(1, 1)));

        assert_eq!(output.new_acceptor_state, acceptor);
    }

    #[test]
    fn duplicate_accept_leaves_state_unchanged() {
        let acceptor = fresh()
            .transition(NodeId(1), accept(pn(1, 1), "x"))
            .new_acceptor_state;

        let output = acceptor.transition(NodeId(1), accept(pn(1, 1), "x"));

        assert_eq!(output.new_acceptor_state, acceptor);
    }

    #[test]
    fn reject_leaves_state_unchanged() {
        let acceptor = fresh()
            .transition(NodeId(2), prepare(pn(2, 2)))
            .new_acceptor_state;

        let output = acceptor.transition(NodeId(1), prepare(pn(1, 1)));

        assert_eq!(output.new_acceptor_state, acceptor);
    }
}
