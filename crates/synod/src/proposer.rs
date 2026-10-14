use crate::message::{AcceptedProposal, Message, NodeId, ProposalNumber};
use std::collections::HashSet;

/// Phases: `Idle`, `Prepare`, `Accept`, `Done`.
///
/// - `Idle -> Prepare`: client request or timeout
/// - `Prepare -> Accept`: majority of promises, and a report or a client value
/// - `Prepare -> Idle`: majority of rejects, or timeout
/// - `Prepare -> Idle`: majority of promises, no reports, no client value (learner inititated a round)
/// - `Accept -> Done`: majority of accepted
/// - `Accept -> Idle`: majority of rejects, or timeout
#[derive(Debug, Clone)]
pub struct Proposer<V> {
    pub highest_proposal_number_sent: Option<ProposalNumber>,
    pub highest_promise_received: Option<ProposalNumber>,
    pub client_value: Option<V>,
    pub phase: ProposerPhase<V>,
}

#[derive(Debug, Clone)]
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

impl<V> ProposerPhase<V> {
    fn rejected_mut(&mut self) -> Option<&mut HashSet<NodeId>> {
        match self {
            ProposerPhase::Prepare { rejected, .. } | ProposerPhase::Accept { rejected, .. } => {
                Some(rejected)
            }
            ProposerPhase::Idle | ProposerPhase::Done { .. } => None,
        }
    }
}

#[derive(Debug)]
pub struct ProposerOutput<V> {
    pub new_proposer_state: Proposer<V>,
    pub send: Option<(Vec<NodeId>, Message<V>)>,
    pub persist: bool,
}

#[allow(unused)]
impl<V: Clone> Proposer<V> {
    pub fn transition(
        self,
        own_id: NodeId,
        nodes: &[NodeId],
        event: ProposerEvent<V>,
    ) -> ProposerOutput<V> {
        let Proposer {
            highest_proposal_number_sent,
            highest_promise_received,
            client_value,
            phase,
        } = self;

        match (event, phase) {
            (
                ProposerEvent::Message {
                    from,
                    message:
                        Message::Promise {
                            proposal_number,
                            accepted,
                        },
                },
                ProposerPhase::Prepare {
                    mut promised,
                    rejected,
                    highest_proposal_received,
                },
            ) if let Some(highest_proposal_number_sent) = highest_proposal_number_sent
                && highest_proposal_number_sent == proposal_number
                && !promised.contains(&from) =>
            {
                promised.insert(from);

                let new_highest_proposal_received = match (accepted, highest_proposal_received) {
                    (Some(accepted), Some(highest_proposal_received)) => {
                        if accepted.proposal_number > highest_proposal_received.proposal_number {
                            Some(accepted)
                        } else {
                            Some(highest_proposal_received)
                        }
                    }
                    (Some(accepted), None) => Some(accepted),
                    (_, highest_proposal_received) => highest_proposal_received,
                };

                if promised.len() * 2 <= nodes.len() {
                    return ProposerOutput {
                        new_proposer_state: Proposer {
                            highest_proposal_number_sent: Some(highest_proposal_number_sent),
                            highest_promise_received,
                            client_value,
                            phase: ProposerPhase::Prepare {
                                promised,
                                rejected,
                                highest_proposal_received: new_highest_proposal_received,
                            },
                        },
                        send: None,
                        persist: false,
                    };
                }

                let value = new_highest_proposal_received
                    .map(|p| p.value)
                    .or(client_value.clone());

                match value {
                    Some(value) => ProposerOutput {
                        new_proposer_state: Proposer {
                            highest_proposal_number_sent: Some(highest_proposal_number_sent),
                            highest_promise_received,
                            client_value,
                            phase: ProposerPhase::Accept {
                                value: value.clone(),
                                accepted: HashSet::new(),
                                rejected: HashSet::new(),
                            },
                        },
                        send: Some((
                            nodes.to_vec(),
                            Message::Accept {
                                proposal_number: highest_proposal_number_sent,
                                value,
                            },
                        )),
                        persist: false,
                    },
                    None => ProposerOutput {
                        new_proposer_state: Proposer {
                            highest_proposal_number_sent: Some(highest_proposal_number_sent),
                            highest_promise_received,
                            client_value,
                            phase: ProposerPhase::Idle,
                        },
                        send: None,
                        persist: false,
                    },
                }
            }
            // An acceptor rejects round n only after promising something higher,
            // so a reject at or below the current round answers an older one.
            (
                ProposerEvent::Message {
                    from,
                    message: Message::Reject { promised },
                },
                mut phase @ (ProposerPhase::Idle
                | ProposerPhase::Prepare { .. }
                | ProposerPhase::Accept { .. }),
            ) if highest_proposal_number_sent.is_none_or(|sent| promised > sent) => {
                let majority_rejected = phase.rejected_mut().is_some_and(|rejected| {
                    rejected.insert(from);
                    rejected.len() * 2 > nodes.len()
                });

                ProposerOutput {
                    new_proposer_state: Proposer {
                        highest_proposal_number_sent,
                        highest_promise_received: highest_promise_received.max(Some(promised)),
                        client_value,
                        phase: if majority_rejected {
                            ProposerPhase::Idle
                        } else {
                            phase
                        },
                    },
                    send: None,
                    persist: false,
                }
            }
            // Duplicates, replies from an old round, and replies that arrive
            // after the phase moved on are normal on a real network.
            (
                ProposerEvent::Message {
                    message: Message::Promise { .. } | Message::Reject { .. },
                    ..
                },
                phase,
            )
            | (_, phase @ ProposerPhase::Done { .. }) => ProposerOutput {
                new_proposer_state: Proposer {
                    highest_proposal_number_sent,
                    highest_promise_received,
                    client_value,
                    phase,
                },
                send: None,
                persist: false,
            },
            (_, _) => todo!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{nodes, pn};

    #[test]
    fn proposer_adopts_highest_reported_value() {
        let proposer: Proposer<&str> = Proposer {
            highest_proposal_number_sent: Some(pn(4, 1)),
            highest_promise_received: None,
            client_value: Some("x"),
            phase: ProposerPhase::Prepare {
                promised: HashSet::new(),
                rejected: HashSet::new(),
                highest_proposal_received: None,
            },
        };

        let proposer = proposer
            .transition(
                NodeId(1),
                &nodes(),
                ProposerEvent::Message {
                    from: NodeId(1),
                    message: Message::Promise {
                        proposal_number: pn(4, 1),
                        accepted: Some(AcceptedProposal {
                            proposal_number: pn(3, 2),
                            value: "y",
                        }),
                    },
                },
            )
            .new_proposer_state;
        let output = proposer.transition(
            NodeId(1),
            &nodes(),
            ProposerEvent::Message {
                from: NodeId(2),
                message: Message::Promise {
                    proposal_number: pn(4, 1),
                    accepted: Some(AcceptedProposal {
                        proposal_number: pn(1, 1),
                        value: "z",
                    }),
                },
            },
        );

        assert_eq!(
            output.send,
            Some((
                vec![NodeId(1), NodeId(2), NodeId(3)],
                Message::Accept {
                    proposal_number: pn(4, 1),
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

    fn preparing(client_value: Option<&'static str>) -> Proposer<&'static str> {
        Proposer {
            highest_proposal_number_sent: Some(pn(4, 1)),
            highest_promise_received: None,
            client_value,
            phase: ProposerPhase::Prepare {
                promised: HashSet::new(),
                rejected: HashSet::new(),
                highest_proposal_received: None,
            },
        }
    }

    fn promise(
        from: u32,
        accepted: Option<AcceptedProposal<&'static str>>,
    ) -> ProposerEvent<&'static str> {
        ProposerEvent::Message {
            from: NodeId(from),
            message: Message::Promise {
                proposal_number: pn(4, 1),
                accepted,
            },
        }
    }

    #[test]
    fn learner_round_adopts_reported_value() {
        let proposer = preparing(None)
            .transition(
                NodeId(1),
                &nodes(),
                promise(
                    1,
                    Some(AcceptedProposal {
                        proposal_number: pn(3, 2),
                        value: "y",
                    }),
                ),
            )
            .new_proposer_state;
        let output = proposer.transition(NodeId(1), &nodes(), promise(2, None));

        assert_eq!(
            output.send,
            Some((
                vec![NodeId(1), NodeId(2), NodeId(3)],
                Message::Accept {
                    proposal_number: pn(4, 1),
                    value: "y",
                },
            ))
        );
        assert!(matches!(
            output.new_proposer_state.phase,
            ProposerPhase::Accept { value: "y", .. }
        ));
    }

    #[test]
    fn learner_round_with_no_reports_goes_idle() {
        let proposer = preparing(None)
            .transition(NodeId(1), &nodes(), promise(1, None))
            .new_proposer_state;
        let output = proposer.transition(NodeId(1), &nodes(), promise(2, None));

        assert_eq!(output.send, None);
        assert!(matches!(
            output.new_proposer_state.phase,
            ProposerPhase::Idle
        ));
        assert!(!output.persist);
    }

    #[test]
    fn duplicate_promise_is_ignored() {
        let proposer = preparing(Some("x"))
            .transition(NodeId(1), &nodes(), promise(1, None))
            .new_proposer_state;
        let output = proposer.transition(NodeId(1), &nodes(), promise(1, None));

        assert_eq!(output.send, None);
        assert!(!output.persist);
        assert!(matches!(
            output.new_proposer_state.phase,
            ProposerPhase::Prepare { promised, .. } if promised.len() == 1
        ));
    }

    #[test]
    fn promise_for_old_round_is_ignored() {
        let output = preparing(Some("x")).transition(
            NodeId(1),
            &nodes(),
            ProposerEvent::Message {
                from: NodeId(1),
                message: Message::Promise {
                    proposal_number: pn(3, 1),
                    accepted: None,
                },
            },
        );

        assert_eq!(output.send, None);
        assert!(!output.persist);
        assert!(matches!(
            output.new_proposer_state.phase,
            ProposerPhase::Prepare { promised, .. } if promised.is_empty()
        ));
    }

    #[test]
    fn proposer_adopts_highest_report_when_it_arrives_last() {
        let proposer = preparing(Some("x"))
            .transition(
                NodeId(1),
                &nodes(),
                promise(
                    1,
                    Some(AcceptedProposal {
                        proposal_number: pn(1, 1),
                        value: "z",
                    }),
                ),
            )
            .new_proposer_state;
        let output = proposer.transition(
            NodeId(1),
            &nodes(),
            promise(
                2,
                Some(AcceptedProposal {
                    proposal_number: pn(3, 2),
                    value: "y",
                }),
            ),
        );

        assert_eq!(
            output.send,
            Some((
                nodes(),
                Message::Accept {
                    proposal_number: pn(4, 1),
                    value: "y",
                },
            ))
        );
    }

    fn accepting() -> Proposer<&'static str> {
        Proposer {
            highest_proposal_number_sent: Some(pn(4, 1)),
            highest_promise_received: None,
            client_value: Some("x"),
            phase: ProposerPhase::Accept {
                value: "y",
                accepted: HashSet::new(),
                rejected: HashSet::new(),
            },
        }
    }

    fn reject(from: u32, promised: ProposalNumber) -> ProposerEvent<&'static str> {
        ProposerEvent::Message {
            from: NodeId(from),
            message: Message::Reject { promised },
        }
    }

    #[test]
    fn reject_in_prepare_is_counted_and_recorded() {
        let output = preparing(Some("x")).transition(NodeId(1), &nodes(), reject(1, pn(7, 3)));

        assert_eq!(output.send, None);
        assert!(!output.persist);
        assert_eq!(
            output.new_proposer_state.highest_promise_received,
            Some(pn(7, 3))
        );
        assert!(matches!(
            output.new_proposer_state.phase,
            ProposerPhase::Prepare { rejected, .. } if rejected.len() == 1
        ));
    }

    #[test]
    fn majority_of_rejects_in_prepare_goes_idle() {
        let proposer = preparing(Some("x"))
            .transition(NodeId(1), &nodes(), reject(1, pn(9, 2)))
            .new_proposer_state;
        let output = proposer.transition(NodeId(1), &nodes(), reject(2, pn(7, 3)));

        assert_eq!(output.send, None);
        assert!(!output.persist);
        assert!(matches!(
            output.new_proposer_state.phase,
            ProposerPhase::Idle
        ));
        assert_eq!(
            output.new_proposer_state.highest_promise_received,
            Some(pn(9, 2))
        );
        assert_eq!(output.new_proposer_state.client_value, Some("x"));
    }

    #[test]
    fn majority_of_rejects_in_accept_goes_idle() {
        let proposer = accepting()
            .transition(NodeId(1), &nodes(), reject(1, pn(5, 2)))
            .new_proposer_state;
        let output = proposer.transition(NodeId(1), &nodes(), reject(2, pn(5, 2)));

        assert_eq!(output.send, None);
        assert!(matches!(
            output.new_proposer_state.phase,
            ProposerPhase::Idle
        ));
        assert_eq!(output.new_proposer_state.client_value, Some("x"));
    }

    #[test]
    fn repeated_reject_from_one_node_counts_once() {
        let proposer = preparing(Some("x"))
            .transition(NodeId(1), &nodes(), reject(1, pn(7, 3)))
            .new_proposer_state;
        let output = proposer.transition(NodeId(1), &nodes(), reject(1, pn(7, 3)));

        assert!(matches!(
            output.new_proposer_state.phase,
            ProposerPhase::Prepare { rejected, .. } if rejected.len() == 1
        ));
    }

    #[test]
    fn reject_below_current_round_is_ignored() {
        let output = preparing(Some("x")).transition(NodeId(1), &nodes(), reject(1, pn(2, 1)));

        assert_eq!(output.send, None);
        assert_eq!(output.new_proposer_state.highest_promise_received, None);
        assert!(matches!(
            output.new_proposer_state.phase,
            ProposerPhase::Prepare { rejected, .. } if rejected.is_empty()
        ));
    }

    #[test]
    fn reject_equal_to_current_round_is_ignored() {
        let output = preparing(Some("x")).transition(NodeId(1), &nodes(), reject(2, pn(4, 1)));

        assert_eq!(output.send, None);
        assert_eq!(output.new_proposer_state.highest_promise_received, None);
        assert!(matches!(
            output.new_proposer_state.phase,
            ProposerPhase::Prepare { rejected, .. } if rejected.is_empty()
        ));
    }

    #[test]
    fn late_reject_in_idle_is_recorded() {
        let proposer: Proposer<&str> = Proposer {
            highest_proposal_number_sent: Some(pn(4, 1)),
            highest_promise_received: Some(pn(5, 2)),
            client_value: Some("x"),
            phase: ProposerPhase::Idle,
        };
        let output = proposer.transition(NodeId(1), &nodes(), reject(3, pn(7, 3)));

        assert_eq!(output.send, None);
        assert_eq!(
            output.new_proposer_state.highest_promise_received,
            Some(pn(7, 3))
        );
        assert!(matches!(
            output.new_proposer_state.phase,
            ProposerPhase::Idle
        ));
    }
}
