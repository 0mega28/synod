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
                message: ProposerMessage::Promise {
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
            message: ProposerMessage::Promise {
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
            AcceptorMessage::Accept {
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
        message: ProposerMessage::Promise {
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
            AcceptorMessage::Accept {
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
            message: ProposerMessage::Promise {
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
            AcceptorMessage::Accept {
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
            rejected: HashSet::new(),
        },
    }
}

fn reject(from: u32, promised: ProposalNumber) -> ProposerEvent<&'static str> {
    ProposerEvent::Message {
        from: NodeId(from),
        message: ProposerMessage::Reject { promised },
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
    // Round 4's timer is still pending and doubles as the retry delay.
    assert_eq!(output.timer, None);
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

fn chosen(value: &'static str) -> ProposerEvent<&'static str> {
    ProposerEvent::Chosen { value }
}

fn assert_done(output: ProposerOutput<&'static str>, value: &'static str) {
    assert_eq!(output.send, None);
    assert_eq!(output.timer, None);
    assert!(!output.persist);
    assert!(matches!(
        output.new_proposer_state.phase,
        ProposerPhase::Done { value: done } if done == value
    ));
}

#[test]
fn chosen_in_idle_is_done() {
    let output = idle(Some(pn(4, 1)), None, Some("x")).transition(NodeId(1), &nodes(), chosen("y"));

    assert_eq!(output.new_proposer_state.client_value, Some("x"));
    assert_eq!(output.done, Some("y"));
    assert_done(output, "y");
}

#[test]
fn chosen_in_prepare_is_done() {
    let output = preparing(Some("x")).transition(NodeId(1), &nodes(), chosen("y"));

    assert_eq!(output.new_proposer_state.client_value, Some("x"));
    assert_eq!(output.done, Some("y"));
    assert_done(output, "y");
}

#[test]
fn chosen_in_accept_is_done() {
    let output = accepting().transition(NodeId(1), &nodes(), chosen("y"));

    assert_eq!(output.done, Some("y"));
    assert_done(output, "y");
}

#[test]
fn chosen_in_done_stays_done() {
    let proposer: Proposer<&str> = Proposer {
        highest_proposal_number_sent: Some(pn(4, 1)),
        highest_promise_received: None,
        client_value: Some("x"),
        phase: ProposerPhase::Done { value: "y" },
    };
    let output = proposer.transition(NodeId(1), &nodes(), chosen("y"));

    assert_eq!(output.done, None);
    assert_done(output, "y");
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
            AcceptorMessage::Prepare {
                proposal_number: round
            }
        ))
    );
    assert!(output.persist);
    assert_eq!(output.timer, Some(round));
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
    let output =
        idle(Some(pn(4, 1)), None, Some("x")).transition(NodeId(1), &nodes(), timeout(pn(4, 1)));

    assert_eq!(output.new_proposer_state.client_value, Some("x"));
    assert_starts_round(output, pn(5, 1));
}

#[test]
fn timeout_in_idle_without_client_value_is_ignored() {
    let output =
        idle(Some(pn(4, 1)), None, None).transition(NodeId(1), &nodes(), timeout(pn(4, 1)));

    assert_eq!(output.send, None);
    assert!(!output.persist);
    assert_eq!(output.timer, None);
    assert_eq!(
        output.new_proposer_state.highest_proposal_number_sent,
        Some(pn(4, 1))
    );
    assert!(matches!(
        output.new_proposer_state.phase,
        ProposerPhase::Idle
    ));
}

#[test]
fn learn_on_fresh_proposer_starts_round_one() {
    let output = idle(None, None, None).transition(NodeId(1), &nodes(), ProposerEvent::Learn);

    assert_eq!(output.new_proposer_state.client_value, None);
    assert_starts_round(output, pn(1, 1));
}

#[test]
fn learn_after_restart_starts_round_above_persisted_one() {
    let output =
        idle(Some(pn(4, 1)), None, None).transition(NodeId(1), &nodes(), ProposerEvent::Learn);

    assert_starts_round(output, pn(5, 1));
}

#[test]
fn learn_in_idle_keeps_client_value() {
    let output =
        idle(Some(pn(4, 1)), None, Some("x")).transition(NodeId(1), &nodes(), ProposerEvent::Learn);

    assert_eq!(output.new_proposer_state.client_value, Some("x"));
    assert_starts_round(output, pn(5, 1));
}

#[test]
fn learn_while_preparing_is_ignored() {
    let output = preparing(Some("x")).transition(NodeId(1), &nodes(), ProposerEvent::Learn);

    assert_eq!(output.send, None);
    assert!(!output.persist);
    assert_eq!(
        output.new_proposer_state.highest_proposal_number_sent,
        Some(pn(4, 1))
    );
    assert!(matches!(
        output.new_proposer_state.phase,
        ProposerPhase::Prepare { .. }
    ));
}

#[test]
fn learn_while_accepting_is_ignored() {
    let output = accepting().transition(NodeId(1), &nodes(), ProposerEvent::Learn);

    assert_eq!(output.send, None);
    assert!(!output.persist);
    assert!(matches!(
        output.new_proposer_state.phase,
        ProposerPhase::Accept { value: "y", .. }
    ));
}

#[test]
fn learn_in_done_stays_done() {
    let proposer: Proposer<&str> = Proposer {
        highest_proposal_number_sent: Some(pn(4, 1)),
        highest_promise_received: None,
        client_value: Some("x"),
        phase: ProposerPhase::Done { value: "y" },
    };
    let output = proposer.transition(NodeId(1), &nodes(), ProposerEvent::Learn);

    assert_eq!(output.done, None);
    assert_done(output, "y");
}

#[test]
fn timeout_in_prepare_goes_idle() {
    let output = preparing(Some("x")).transition(NodeId(1), &nodes(), timeout(pn(4, 1)));

    assert_eq!(output.send, None);
    assert!(!output.persist);
    assert_eq!(output.timer, Some(pn(4, 1)));
    assert!(matches!(
        output.new_proposer_state.phase,
        ProposerPhase::Idle
    ));
    assert_eq!(
        output.new_proposer_state.highest_proposal_number_sent,
        Some(pn(4, 1))
    );
    assert_eq!(output.new_proposer_state.client_value, Some("x"));
}

#[test]
fn timeout_in_accept_goes_idle() {
    let output = accepting().transition(NodeId(1), &nodes(), timeout(pn(4, 1)));

    assert_eq!(output.send, None);
    assert!(!output.persist);
    assert_eq!(output.timer, Some(pn(4, 1)));
    assert!(matches!(
        output.new_proposer_state.phase,
        ProposerPhase::Idle
    ));
    assert_eq!(
        output.new_proposer_state.highest_proposal_number_sent,
        Some(pn(4, 1))
    );
    assert_eq!(output.new_proposer_state.client_value, Some("x"));
}

#[test]
fn timeout_in_done_stays_done() {
    let proposer: Proposer<&str> = Proposer {
        highest_proposal_number_sent: Some(pn(4, 1)),
        highest_promise_received: None,
        client_value: Some("x"),
        phase: ProposerPhase::Done { value: "y" },
    };
    let output = proposer.transition(NodeId(1), &nodes(), timeout(pn(4, 1)));

    assert_eq!(output.send, None);
    assert!(!output.persist);
    assert!(matches!(
        output.new_proposer_state.phase,
        ProposerPhase::Done { value: "y" }
    ));
}

#[test]
fn second_timeout_after_stalled_round_retries_higher() {
    let proposer = preparing(Some("x"))
        .transition(NodeId(1), &nodes(), timeout(pn(4, 1)))
        .new_proposer_state;
    let output = proposer.transition(NodeId(1), &nodes(), timeout(pn(4, 1)));

    assert_eq!(output.new_proposer_state.client_value, Some("x"));
    assert_starts_round(output, pn(5, 1));
}

fn timeout(proposal_number: ProposalNumber) -> ProposerEvent<&'static str> {
    ProposerEvent::Timeout { proposal_number }
}

#[test]
fn reaching_accept_requests_no_new_timer() {
    let proposer = preparing(Some("x"))
        .transition(NodeId(1), &nodes(), promise(1, None))
        .new_proposer_state;
    let output = proposer.transition(NodeId(1), &nodes(), promise(2, None));

    assert!(matches!(
        output.new_proposer_state.phase,
        ProposerPhase::Accept { .. }
    ));
    assert_eq!(output.timer, None);
}

#[test]
fn stale_timeout_in_prepare_is_ignored() {
    let output = preparing(Some("x")).transition(NodeId(1), &nodes(), timeout(pn(3, 1)));

    assert_eq!(output.send, None);
    assert!(!output.persist);
    assert_eq!(output.timer, None);
    assert!(matches!(
        output.new_proposer_state.phase,
        ProposerPhase::Prepare { .. }
    ));
}

#[test]
fn stale_timeout_in_accept_is_ignored() {
    let output = accepting().transition(NodeId(1), &nodes(), timeout(pn(3, 1)));

    assert_eq!(output.send, None);
    assert_eq!(output.timer, None);
    assert!(matches!(
        output.new_proposer_state.phase,
        ProposerPhase::Accept { value: "y", .. }
    ));
}

#[test]
fn stale_timeout_in_idle_does_not_retry() {
    // Round 5 went idle after rejects; a leftover timer from round 4 fires.
    let output =
        idle(Some(pn(5, 1)), None, Some("x")).transition(NodeId(1), &nodes(), timeout(pn(4, 1)));

    assert_eq!(output.send, None);
    assert!(!output.persist);
    assert_eq!(output.timer, None);
    assert_eq!(
        output.new_proposer_state.highest_proposal_number_sent,
        Some(pn(5, 1))
    );
    assert!(matches!(
        output.new_proposer_state.phase,
        ProposerPhase::Idle
    ));
}
