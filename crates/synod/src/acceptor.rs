use crate::message::{AcceptedProposal, Message, NodeId, ProposalNumber};
use std::cmp::Ordering;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Acceptor<V> {
    pub promised: Option<ProposalNumber>,
    pub accepted_proposal: Option<AcceptedProposal<V>>,
}

#[derive(Debug)]
pub struct AcceptorOutput<V> {
    pub new_acceptor_state: Acceptor<V>,
    pub send: Option<(Vec<NodeId>, Message<V>)>,
    pub persist: bool,
}

impl<V: Clone> Acceptor<V> {
    pub fn transition(
        &self,
        nodes: &[NodeId],
        sender_node_id: NodeId,
        message: Message<V>,
    ) -> AcceptorOutput<V> {
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
                            send: Some((
                                vec![sender_node_id],
                                Message::Promise {
                                    proposal_number,
                                    accepted: accepted_proposal.clone(),
                                },
                            )),
                            persist: true,
                        },
                        Ordering::Greater => AcceptorOutput {
                            new_acceptor_state: self.clone(),
                            send: Some((
                                vec![sender_node_id],
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
                    send: Some((
                        vec![sender_node_id],
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
                        send: Some((
                            vec![sender_node_id],
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
                    send: Some((
                        nodes.to_vec(),
                        Message::Accepted {
                            proposal_number,
                            value,
                        },
                    )),
                    persist: true,
                }
            }
            _ => {
                unreachable!(
                    "Acceptor only handles Message::Prepare and Message::Accept, but received {}",
                    message.kind()
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{nodes, pn};

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
            .transition(&nodes(), NodeId(1), prepare(pn(1, 1)))
            .new_acceptor_state;
        let acceptor = acceptor
            .transition(&nodes(), NodeId(1), accept(pn(1, 1), "x"))
            .new_acceptor_state;
        let output = acceptor.transition(&nodes(), NodeId(2), prepare(pn(2, 2)));

        assert_eq!(
            output.send,
            Some((
                vec![NodeId(2)],
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
            .transition(&nodes(), NodeId(2), prepare(pn(2, 2)))
            .new_acceptor_state;
        let output = acceptor.transition(&nodes(), NodeId(1), prepare(pn(1, 1)));

        assert_eq!(
            output.send,
            Some((vec![NodeId(1)], Message::Reject { promised: pn(2, 2) }))
        );
        assert_eq!(output.new_acceptor_state, acceptor);
        assert!(!output.persist);
    }

    #[test]
    fn accept_below_promise_is_rejected() {
        let acceptor = fresh();

        let acceptor = acceptor
            .transition(&nodes(), NodeId(2), prepare(pn(2, 2)))
            .new_acceptor_state;
        let output = acceptor.transition(&nodes(), NodeId(1), accept(pn(1, 1), "x"));

        assert_eq!(
            output.send,
            Some((vec![NodeId(1)], Message::Reject { promised: pn(2, 2) }))
        );
        assert_eq!(output.new_acceptor_state, acceptor);
        assert!(!output.persist);
    }

    #[test]
    fn accept_is_answered_with_accepted() {
        let acceptor = fresh();

        let acceptor = acceptor
            .transition(&nodes(), NodeId(1), prepare(pn(1, 1)))
            .new_acceptor_state;
        let output = acceptor.transition(&nodes(), NodeId(1), accept(pn(1, 1), "x"));

        assert_eq!(
            output.send,
            Some((
                nodes(),
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
            .transition(&nodes(), NodeId(3), prepare(pn(3, 3)))
            .new_acceptor_state;
        let acceptor = acceptor
            .transition(&nodes(), NodeId(2), accept(pn(5, 2), "x"))
            .new_acceptor_state;
        let output = acceptor.transition(&nodes(), NodeId(1), accept(pn(4, 1), "y"));

        assert_eq!(
            output.send,
            Some((vec![NodeId(1)], Message::Reject { promised: pn(5, 2) }))
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
        let output = fresh().transition(&nodes(), NodeId(1), accept(pn(1, 1), "x"));

        assert_eq!(
            output.send,
            Some((
                nodes(),
                Message::Accepted {
                    proposal_number: pn(1, 1),
                    value: "x",
                },
            ))
        );
        assert!(output.persist);
    }

    #[test]
    fn duplicate_prepare_is_promised_again() {
        let acceptor = fresh()
            .transition(&nodes(), NodeId(1), prepare(pn(1, 1)))
            .new_acceptor_state;
        let output = acceptor.transition(&nodes(), NodeId(1), prepare(pn(1, 1)));

        assert_eq!(
            output.send,
            Some((
                vec![NodeId(1)],
                Message::Promise {
                    proposal_number: pn(1, 1),
                    accepted: None,
                },
            ))
        );
    }
}
