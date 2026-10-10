use crate::message::{AcceptedProposal, Message, NodeId, ProposalNumber};
use std::{cmp::Ordering, collections::HashSet};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Acceptor<V> {
    pub promised: Option<ProposalNumber>,
    pub accepted_proposal: Option<AcceptedProposal<V>>,
}

/// Phases: `Idle`, `Prepare`, `Accept`, `Done`.
///
/// - `Idle -> Prepare`: client request or timeout
/// - `Prepare -> Accept`: majority of promises, and a report or a client value
/// - `Prepare -> Idle`: majority of rejects, or timeout
/// - `Prepare -> Idle`: majority of promises, no reports, no client value (learner inititated a round)
/// - `Accept -> Done`: majority of accepted
/// - `Accept -> Idle`: majority of rejects, or timeout
#[derive(Debug)]
pub struct Proposer<V> {
    pub highest_proposal_number_sent: Option<ProposalNumber>,
    pub highest_promise_received: Option<ProposalNumber>,
    pub client_value: Option<V>,
    pub acceptors: Vec<NodeId>,
    pub phase: ProposerPhase<V>,
}

#[derive(Debug)]
pub enum ProposerPhase<V> {
    Idle,
    Prepare {
        promised: HashSet<NodeId>,
        rejected: HashSet<NodeId>,
        highest_proposal_received: Option<AcceptedProposal<V>>,
    },
    Accept {
        value: V,
        accepted: HashSet<NodeId>,
        rejected: HashSet<NodeId>,
    },
    Done {
        value: V,
    },
}

pub enum ProposerEvent<V> {
    ClientRequest { value: V },
    Message { from: NodeId, message: Message<V> },
    Timeout,
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

#[derive(Debug)]
pub struct ProposerOutput<V> {
    pub new_proposer_state: Proposer<V>,
    pub send: Option<(Vec<NodeId>, Message<V>)>,
    pub persist: bool,
}

impl<V: Clone> Proposer<V> {
    pub fn transition(&self, event: ProposerEvent<V>) -> ProposerOutput<V> {
        todo!()
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
        assert!(output.persist);
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
        assert_eq!(output.new_acceptor_state, acceptor);
        assert!(!output.persist);
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
        assert_eq!(output.new_acceptor_state, acceptor);
        assert!(!output.persist);
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
        assert!(output.persist);
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
        assert!(!output.persist);
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
        assert!(output.persist);
    }

    #[test]
    fn proposer_adopts_highest_reported_value() {
        let proposer: Proposer<&str> = Proposer {
            highest_proposal_number_sent: Some(pn(4, 0)),
            highest_promise_received: None,
            client_value: Some("x"),
            acceptors: vec![NodeId(1), NodeId(2), NodeId(3)],
            phase: ProposerPhase::Prepare {
                promised: HashSet::new(),
                rejected: HashSet::new(),
                highest_proposal_received: None,
            },
        };

        let proposer = proposer
            .transition(ProposerEvent::Message {
                from: NodeId(1),
                message: Message::Promise {
                    proposal_number: pn(4, 0),
                    accepted: Some(AcceptedProposal {
                        proposal_number: pn(3, 2),
                        value: "y",
                    }),
                },
            })
            .new_proposer_state;
        let output = proposer.transition(ProposerEvent::Message {
            from: NodeId(2),
            message: Message::Promise {
                proposal_number: pn(4, 0),
                accepted: Some(AcceptedProposal {
                    proposal_number: pn(1, 1),
                    value: "z",
                }),
            },
        });

        assert_eq!(
            output.send,
            Some((
                vec![NodeId(1), NodeId(2), NodeId(3)],
                Message::Accept {
                    proposal_number: pn(4, 0),
                    value: "y",
                },
            ))
        );
        assert!(matches!(
            output.new_proposer_state.phase,
            ProposerPhase::Accept { value: "y", .. }
        ));
        assert!(!output.persist);
    }
}
