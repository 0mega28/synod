use crate::message::ProposerMessage;
use std::collections::{HashMap, HashSet};

use super::*;
use crate::{
    message::{AcceptedProposal, LearnerMessage},
    proposer::ProposerPhase,
    test_support::{nodes, pn},
};

/// Node 1 of `nodes()`, fresh in every role.
fn fresh() -> Node<&'static str> {
    Node {
        acceptor: Acceptor {
            promised: None,
            accepted_proposal: None,
        },
        proposer: Proposer {
            highest_proposal_number_sent: None,
            highest_promise_received: None,
            client_value: None,
            phase: ProposerPhase::Idle,
        },
        learner: Learner {
            votes: HashMap::new(),
            chosen: None,
        },
        id: NodeId(1),
        nodes: nodes(),
    }
}

/// Node 1 preparing round `(1, 1)` for client value `"x"`.
fn preparing() -> Node<&'static str> {
    let mut node = fresh();
    node.proposer = Proposer {
        highest_proposal_number_sent: Some(pn(1, 1)),
        highest_promise_received: None,
        client_value: Some("x"),
        phase: ProposerPhase::Prepare {
            promised: HashSet::new(),
            rejected: HashSet::new(),
            highest_proposal_received: None,
        },
    };
    node
}

fn message(from: u32, message: impl Into<Message<&'static str>>) -> NodeEvent<&'static str> {
    NodeEvent::Message {
        from: NodeId(from),
        message: message.into(),
    }
}

fn to_every_node(
    message: impl Into<Message<&'static str>>,
) -> Vec<(NodeId, Message<&'static str>)> {
    let message = message.into();
    nodes()
        .into_iter()
        .map(|node| (node, message.clone()))
        .collect()
}

fn accepted(from: u32, value: &'static str) -> NodeEvent<&'static str> {
    message(
        from,
        LearnerMessage::Accepted {
            proposal_number: pn(1, 1),
            value,
        },
    )
}

// Proposer routing

#[test]
fn client_request_sends_prepare_to_every_node() {
    let output = fresh().transition(NodeEvent::ClientRequest { value: "x" });

    assert_eq!(
        output.messages,
        to_every_node(AcceptorMessage::Prepare {
            proposal_number: pn(1, 1)
        })
    );
    assert!(matches!(output.persist, Some(Persist::Proposer)));
    assert_eq!(output.timer, Some(pn(1, 1)));
    assert_eq!(output.client_response, None);
    assert_eq!(output.new_node_state.proposer.client_value, Some("x"));
}

#[test]
fn start_runs_a_learner_round() {
    let output = fresh().transition(NodeEvent::Start);

    assert_eq!(
        output.messages,
        to_every_node(AcceptorMessage::Prepare {
            proposal_number: pn(1, 1)
        })
    );
    assert!(matches!(output.persist, Some(Persist::Proposer)));
    assert_eq!(output.timer, Some(pn(1, 1)));
    assert_eq!(output.new_node_state.proposer.client_value, None);
}

#[test]
fn timeout_reaches_the_proposer() {
    let output = preparing().transition(NodeEvent::Timeout {
        proposal_number: pn(1, 1),
    });

    assert!(output.messages.is_empty());
    assert!(output.persist.is_none());
    assert_eq!(output.timer, Some(pn(1, 1)));
    assert!(matches!(
        output.new_node_state.proposer.phase,
        ProposerPhase::Idle
    ));
}

#[test]
fn majority_of_promises_sends_accept_to_every_node() {
    let node = preparing()
        .transition(message(
            1,
            ProposerMessage::Promise {
                proposal_number: pn(1, 1),
                accepted: None,
            },
        ))
        .new_node_state;
    let output = node.transition(message(
        2,
        ProposerMessage::Promise {
            proposal_number: pn(1, 1),
            accepted: None,
        },
    ));

    assert_eq!(
        output.messages,
        to_every_node(AcceptorMessage::Accept {
            proposal_number: pn(1, 1),
            value: "x",
        })
    );
    assert!(output.persist.is_none());
    assert_eq!(output.timer, None);
}

// Acceptor routing

#[test]
fn promise_goes_back_to_the_sender_only() {
    let output = fresh().transition(message(
        2,
        AcceptorMessage::Prepare {
            proposal_number: pn(1, 2),
        },
    ));

    assert_eq!(
        output.messages,
        vec![(
            NodeId(2),
            ProposerMessage::Promise {
                proposal_number: pn(1, 2),
                accepted: None,
            }
            .into()
        )]
    );
    assert!(matches!(output.persist, Some(Persist::Acceptor)));
    assert_eq!(output.timer, None);
    assert_eq!(output.new_node_state.acceptor.promised, Some(pn(1, 2)));
}

#[test]
fn reject_goes_back_to_the_sender_only() {
    let mut node = fresh();
    node.acceptor.promised = Some(pn(5, 3));
    let output = node.transition(message(
        2,
        AcceptorMessage::Prepare {
            proposal_number: pn(1, 2),
        },
    ));

    assert_eq!(
        output.messages,
        vec![(
            NodeId(2),
            ProposerMessage::Reject { promised: pn(5, 3) }.into()
        )]
    );
    assert!(output.persist.is_none());
}

#[test]
fn accept_is_announced_to_every_node() {
    let output = fresh().transition(message(
        2,
        AcceptorMessage::Accept {
            proposal_number: pn(1, 2),
            value: "x",
        },
    ));

    assert_eq!(
        output.messages,
        to_every_node(LearnerMessage::Accepted {
            proposal_number: pn(1, 2),
            value: "x",
        })
    );
    assert!(matches!(output.persist, Some(Persist::Acceptor)));
    assert_eq!(
        output.new_node_state.acceptor.accepted_proposal,
        Some(AcceptedProposal {
            proposal_number: pn(1, 2),
            value: "x",
        })
    );
}

// Learner routing

#[test]
fn accepted_below_majority_reports_nothing() {
    let output = preparing().transition(accepted(1, "x"));

    assert!(output.messages.is_empty());
    assert_eq!(output.client_response, None);
    assert_eq!(output.new_node_state.learner.chosen, None);
}

#[test]
fn chosen_value_is_reported_and_moves_proposer_to_done() {
    let node = preparing().transition(accepted(1, "x")).new_node_state;
    let output = node.transition(accepted(2, "x"));

    assert!(output.messages.is_empty());
    assert!(output.persist.is_none());
    assert_eq!(output.timer, None);
    assert_eq!(output.client_response, Some("x"));
    assert_eq!(output.new_node_state.learner.chosen, Some("x"));
    assert!(matches!(
        output.new_node_state.proposer.phase,
        ProposerPhase::Done { value: "x" }
    ));
}

#[test]
fn chosen_value_is_reported_once() {
    let node = preparing()
        .transition(accepted(1, "x"))
        .new_node_state
        .transition(accepted(2, "x"))
        .new_node_state;
    let output = node.transition(accepted(3, "x"));

    assert_eq!(output.client_response, None);
    assert_eq!(output.new_node_state.learner.chosen, Some("x"));
}

#[test]
fn chosen_value_is_not_reported_again_after_done() {
    let node = preparing()
        .transition(accepted(1, "x"))
        .new_node_state
        .transition(accepted(2, "x"))
        .new_node_state;
    // Round (1, 1)'s timer is still pending and fires after the value is chosen.
    let output = node.transition(NodeEvent::Timeout {
        proposal_number: pn(1, 1),
    });

    assert_eq!(output.client_response, None);
    assert!(output.messages.is_empty());
    assert!(matches!(
        output.new_node_state.proposer.phase,
        ProposerPhase::Done { value: "x" }
    ));
}
