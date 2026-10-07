use crate::message::{AcceptedProposal, Message, NodeId, ProposalNumber};
use std::cmp::Ordering;

#[derive(Debug, Clone)]
pub struct Acceptor<V> {
    pub promised: Option<ProposalNumber>,
    pub accepted_proposal: Option<AcceptedProposal<V>>,
}

#[derive(Debug)]
pub struct Proposer {
    pub highest_proposal_number: Option<ProposalNumber>,
}

#[allow(dead_code)]
impl<V: Clone> Acceptor<V> {
    fn transition(&self, message: Message<V>, node_id: NodeId) -> (Self, Option<Message<V>>) {
        let Acceptor {
            promised,
            accepted_proposal,
        } = self;
        match message {
            Message::Prepare { proposal_number } => {
                if let Some(promised) = promised {
                    return match promised.cmp(&proposal_number) {
                        Ordering::Less | Ordering::Equal => (
                            Acceptor {
                                promised: Some(proposal_number),
                                accepted_proposal: accepted_proposal.clone(),
                            },
                            Some(Message::Promise {
                                proposal_number,
                                from: node_id,
                                accepted: accepted_proposal.clone(),
                            }),
                        ),
                        Ordering::Greater => (
                            self.clone(),
                            Some(Message::Reject {
                                promised: *promised,
                            }),
                        ),
                    };
                }
                (
                    Acceptor {
                        promised: Some(proposal_number),
                        accepted_proposal: accepted_proposal.clone(),
                    },
                    Some(Message::Promise {
                        proposal_number,
                        from: node_id,
                        accepted: accepted_proposal.clone(),
                    }),
                )
            }
            Message::Accept {
                proposal_number,
                value,
            } => {
                if let Some(promised) = promised
                    && *promised > proposal_number
                {
                    return (
                        self.clone(),
                        Some(Message::Reject {
                            promised: *promised,
                        }),
                    );
                }
                (
                    Acceptor {
                        promised: Some(proposal_number),
                        accepted_proposal: Some(AcceptedProposal {
                            proposal_number,
                            value: value.clone(),
                        }),
                    },
                    Some(Message::Accepted {
                        proposal_number,
                        value,
                        from: node_id,
                    }),
                )
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

    #[test]
    fn promise_reports_previously_accepted_proposal() {
        let me = NodeId(0);
        let acceptor: Acceptor<&str> = Acceptor {
            promised: None,
            accepted_proposal: None,
        };

        let (acceptor, _) = acceptor.transition(
            Message::Prepare {
                proposal_number: pn(1, 1),
            },
            me,
        );
        let (acceptor, _) = acceptor.transition(
            Message::Accept {
                proposal_number: pn(1, 1),
                value: "x",
            },
            me,
        );
        let (_, reply) = acceptor.transition(
            Message::Prepare {
                proposal_number: pn(2, 2),
            },
            me,
        );

        assert_eq!(
            reply,
            Some(Message::Promise {
                proposal_number: pn(2, 2),
                from: me,
                accepted: Some(AcceptedProposal {
                    proposal_number: pn(1, 1),
                    value: "x",
                }),
            })
        );
    }

    #[test]
    fn lower_prepare_after_promise_is_rejected() {
        let me = NodeId(0);
        let acceptor: Acceptor<&str> = Acceptor {
            promised: None,
            accepted_proposal: None,
        };

        let (acceptor, _) = acceptor.transition(
            Message::Prepare {
                proposal_number: pn(2, 2),
            },
            me,
        );
        let (_, reply) = acceptor.transition(
            Message::Prepare {
                proposal_number: pn(1, 1),
            },
            me,
        );

        assert_eq!(reply, Some(Message::Reject { promised: pn(2, 2) }));
    }

    #[test]
    fn accept_below_promise_is_rejected() {
        let me = NodeId(0);
        let acceptor: Acceptor<&str> = Acceptor {
            promised: None,
            accepted_proposal: None,
        };

        let (acceptor, _) = acceptor.transition(
            Message::Prepare {
                proposal_number: pn(2, 2),
            },
            me,
        );
        let (_, reply) = acceptor.transition(
            Message::Accept {
                proposal_number: pn(1, 1),
                value: "x",
            },
            me,
        );

        assert_eq!(reply, Some(Message::Reject { promised: pn(2, 2) }));
    }

    #[test]
    fn accept_is_answered_with_accepted() {
        let me = NodeId(0);
        let acceptor: Acceptor<&str> = Acceptor {
            promised: None,
            accepted_proposal: None,
        };

        let (acceptor, _) = acceptor.transition(
            Message::Prepare {
                proposal_number: pn(1, 1),
            },
            me,
        );
        let (_, reply) = acceptor.transition(
            Message::Accept {
                proposal_number: pn(1, 1),
                value: "x",
            },
            me,
        );

        assert_eq!(
            reply,
            Some(Message::Accepted {
                proposal_number: pn(1, 1),
                value: "x",
                from: me,
            })
        );
    }

    #[test]
    fn accept_raises_promise_so_lower_accept_is_rejected() {
        let me = NodeId(0);
        let acceptor: Acceptor<&str> = Acceptor {
            promised: None,
            accepted_proposal: None,
        };

        let (acceptor, _) = acceptor.transition(
            Message::Prepare {
                proposal_number: pn(3, 3),
            },
            me,
        );
        let (acceptor, _) = acceptor.transition(
            Message::Accept {
                proposal_number: pn(5, 2),
                value: "x",
            },
            me,
        );
        let (acceptor, reply) = acceptor.transition(
            Message::Accept {
                proposal_number: pn(4, 1),
                value: "y",
            },
            me,
        );

        assert_eq!(reply, Some(Message::Reject { promised: pn(5, 2) }));
        assert_eq!(
            acceptor.accepted_proposal,
            Some(AcceptedProposal {
                proposal_number: pn(5, 2),
                value: "x",
            })
        );
    }

    #[test]
    fn fresh_acceptor_answers_accept_with_accepted() {
        let me = NodeId(0);
        let acceptor: Acceptor<&str> = Acceptor {
            promised: None,
            accepted_proposal: None,
        };

        let (_, reply) = acceptor.transition(
            Message::Accept {
                proposal_number: pn(1, 1),
                value: "x",
            },
            me,
        );

        assert_eq!(
            reply,
            Some(Message::Accepted {
                proposal_number: pn(1, 1),
                value: "x",
                from: me,
            })
        );
    }
}
