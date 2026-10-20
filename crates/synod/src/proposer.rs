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

impl<V: Clone> Proposer<V> {
    pub fn transition(
        mut self,
        own_id: NodeId,
        nodes: &[NodeId],
        event: ProposerEvent<V>,
    ) -> ProposerOutput<V> {
        match (event, self.phase) {
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
            ) if self
                .highest_proposal_number_sent
                .is_some_and(|sent| sent == proposal_number)
                && !promised.contains(&from) =>
            {
                promised.insert(from);

                let highest_proposal_received = accepted
                    .into_iter()
                    .chain(highest_proposal_received)
                    .max_by_key(|p| p.proposal_number);

                let (phase, send) = if promised.len() * 2 <= nodes.len() {
                    (
                        ProposerPhase::Prepare {
                            promised,
                            rejected,
                            highest_proposal_received,
                        },
                        None,
                    )
                } else {
                    match highest_proposal_received
                        .map(|p| p.value)
                        .or_else(|| self.client_value.clone())
                    {
                        Some(value) => (
                            ProposerPhase::Accept {
                                value: value.clone(),
                                accepted: HashSet::new(),
                                rejected: HashSet::new(),
                            },
                            Some((
                                nodes.to_vec(),
                                Message::Accept {
                                    proposal_number,
                                    value,
                                },
                            )),
                        ),
                        None => (ProposerPhase::Idle, None),
                    }
                };

                self.phase = phase;
                ProposerOutput {
                    new_proposer_state: self,
                    send,
                    persist: false,
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
            ) if self
                .highest_proposal_number_sent
                .is_none_or(|sent| promised > sent) =>
            {
                let majority_rejected = phase.rejected_mut().is_some_and(|rejected| {
                    rejected.insert(from);
                    rejected.len() * 2 > nodes.len()
                });

                self.highest_promise_received = self.highest_promise_received.max(Some(promised));
                self.phase = if majority_rejected {
                    ProposerPhase::Idle
                } else {
                    phase
                };
                ProposerOutput {
                    new_proposer_state: self,
                    send: None,
                    persist: false,
                }
            }
            (
                ProposerEvent::Message {
                    from,
                    message:
                        Message::Accepted {
                            proposal_number, ..
                        },
                },
                ProposerPhase::Accept {
                    value,
                    mut accepted,
                    rejected,
                },
            ) if self
                .highest_proposal_number_sent
                .is_some_and(|sent| sent == proposal_number)
                && !accepted.contains(&from) =>
            {
                accepted.insert(from);

                self.phase = if accepted.len() * 2 <= nodes.len() {
                    ProposerPhase::Accept {
                        value,
                        accepted,
                        rejected,
                    }
                } else {
                    ProposerPhase::Done { value }
                };
                ProposerOutput {
                    new_proposer_state: self,
                    send: None,
                    persist: false,
                }
            }
            // Duplicates, replies from an old round, and replies that arrive
            // after the phase moved on are normal on a real network.
            (
                ProposerEvent::Message {
                    message:
                        Message::Promise { .. } | Message::Reject { .. } | Message::Accepted { .. },
                    ..
                },
                phase,
            )
            | (_, phase @ ProposerPhase::Done { .. }) => {
                self.phase = phase;
                ProposerOutput {
                    new_proposer_state: self,
                    send: None,
                    persist: false,
                }
            }
            (
                event @ (ProposerEvent::ClientRequest { .. } | ProposerEvent::Timeout),
                ProposerPhase::Idle,
            ) => {
                if let ProposerEvent::ClientRequest { value } = event {
                    // TODO(multi-decree): a reject may have occurred in phase 1 and sent us
                    // back to idle, and then a new client request replaces the current client
                    // request.
                    self.client_value = Some(value);
                }

                self.phase = ProposerPhase::Prepare {
                    promised: HashSet::new(),
                    rejected: HashSet::new(),
                    highest_proposal_received: None,
                };
                let new_proposal_number = self.next_proposal_number(own_id);

                self.highest_proposal_number_sent = Some(new_proposal_number);
                ProposerOutput {
                    new_proposer_state: self,
                    send: Some((
                        nodes.to_vec(),
                        Message::Prepare {
                            proposal_number: new_proposal_number,
                        },
                    )),
                    persist: true,
                }
            }
            (_, _) => todo!(),
        }
    }

    fn next_proposal_number(&self, own_id: NodeId) -> ProposalNumber {
        ProposalNumber {
            round: self
                .highest_proposal_number_sent
                .max(self.highest_promise_received)
                .map_or(1, |p| p.round + 1),
            node_id: own_id,
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

    fn accepted(
        from: u32,
        proposal_number: ProposalNumber,
        value: &'static str,
    ) -> ProposerEvent<&'static str> {
        ProposerEvent::Message {
            from: NodeId(from),
            message: Message::Accepted {
                proposal_number,
                value,
            },
        }
    }

    #[test]
    fn accepted_in_accept_is_counted() {
        let output = accepting().transition(NodeId(1), &nodes(), accepted(1, pn(4, 1), "y"));

        assert_eq!(output.send, None);
        assert!(!output.persist);
        assert!(matches!(
            output.new_proposer_state.phase,
            ProposerPhase::Accept { accepted, .. } if accepted.len() == 1
        ));
    }

    #[test]
    fn majority_of_accepted_is_done() {
        let proposer = accepting()
            .transition(NodeId(1), &nodes(), accepted(1, pn(4, 1), "y"))
            .new_proposer_state;
        let output = proposer.transition(NodeId(1), &nodes(), accepted(2, pn(4, 1), "y"));

        assert_eq!(output.send, None);
        assert!(!output.persist);
        assert!(matches!(
            output.new_proposer_state.phase,
            ProposerPhase::Done { value: "y" }
        ));
    }

    #[test]
    fn repeated_accepted_from_one_node_counts_once() {
        let proposer = accepting()
            .transition(NodeId(1), &nodes(), accepted(2, pn(4, 1), "y"))
            .new_proposer_state;
        let output = proposer.transition(NodeId(1), &nodes(), accepted(2, pn(4, 1), "y"));

        assert!(matches!(
            output.new_proposer_state.phase,
            ProposerPhase::Accept { accepted, .. } if accepted.len() == 1
        ));
    }

    #[test]
    fn accepted_for_another_round_is_ignored() {
        let output = accepting().transition(NodeId(1), &nodes(), accepted(2, pn(3, 2), "z"));

        assert_eq!(output.send, None);
        assert!(!output.persist);
        assert!(matches!(
            output.new_proposer_state.phase,
            ProposerPhase::Accept { accepted, .. } if accepted.is_empty()
        ));
    }

    #[test]
    fn accepted_in_prepare_is_ignored() {
        let output =
            preparing(Some("x")).transition(NodeId(1), &nodes(), accepted(2, pn(3, 2), "z"));

        assert_eq!(output.send, None);
        assert!(!output.persist);
        assert!(matches!(
            output.new_proposer_state.phase,
            ProposerPhase::Prepare { promised, .. } if promised.is_empty()
        ));
    }

    #[test]
    fn accepted_in_idle_is_ignored() {
        let proposer: Proposer<&str> = Proposer {
            highest_proposal_number_sent: Some(pn(4, 1)),
            highest_promise_received: None,
            client_value: Some("x"),
            phase: ProposerPhase::Idle,
        };
        let output = proposer.transition(NodeId(1), &nodes(), accepted(2, pn(4, 1), "y"));

        assert_eq!(output.send, None);
        assert!(!output.persist);
        assert!(matches!(
            output.new_proposer_state.phase,
            ProposerPhase::Idle
        ));
    }

    fn idle(
        highest_proposal_number_sent: Option<ProposalNumber>,
        highest_promise_received: Option<ProposalNumber>,
        client_value: Option<&'static str>,
    ) -> Proposer<&'static str> {
        Proposer {
            highest_proposal_number_sent,
            highest_promise_received,
            client_value,
            phase: ProposerPhase::Idle,
        }
    }

    fn assert_starts_round(output: ProposerOutput<&'static str>, round: ProposalNumber) {
        assert_eq!(
            output.send,
            Some((
                nodes(),
                Message::Prepare {
                    proposal_number: round
                }
            ))
        );
        assert!(output.persist);
        assert_eq!(
            output.new_proposer_state.highest_proposal_number_sent,
            Some(round)
        );
        assert!(matches!(
            output.new_proposer_state.phase,
            ProposerPhase::Prepare { promised, rejected, highest_proposal_received: None }
                if promised.is_empty() && rejected.is_empty()
        ));
    }

    #[test]
    fn client_request_on_fresh_proposer_starts_round_one() {
        let output = idle(None, None, None).transition(
            NodeId(1),
            &nodes(),
            ProposerEvent::ClientRequest { value: "x" },
        );

        assert_eq!(output.new_proposer_state.client_value, Some("x"));
        assert_starts_round(output, pn(1, 1));
    }

    #[test]
    fn next_round_is_above_highest_sent() {
        let output = idle(Some(pn(4, 1)), None, None).transition(
            NodeId(1),
            &nodes(),
            ProposerEvent::ClientRequest { value: "x" },
        );

        assert_starts_round(output, pn(5, 1));
    }

    #[test]
    fn next_round_is_above_highest_promise_received() {
        let output = idle(Some(pn(4, 1)), Some(pn(7, 3)), None).transition(
            NodeId(1),
            &nodes(),
            ProposerEvent::ClientRequest { value: "x" },
        );

        assert_starts_round(output, pn(8, 1));
    }

    #[test]
    fn timeout_in_idle_retries_with_client_value() {
        let output = idle(Some(pn(4, 1)), None, Some("x")).transition(
            NodeId(1),
            &nodes(),
            ProposerEvent::Timeout,
        );

        assert_eq!(output.new_proposer_state.client_value, Some("x"));
        assert_starts_round(output, pn(5, 1));
    }

    #[test]
    fn timeout_in_idle_without_client_value_starts_learner_round() {
        let output = idle(Some(pn(4, 1)), None, None).transition(
            NodeId(1),
            &nodes(),
            ProposerEvent::Timeout,
        );

        assert_eq!(output.new_proposer_state.client_value, None);
        assert_starts_round(output, pn(5, 1));
    }
}
