use super::*;
use crate::test_support::pn;

fn fresh() -> Acceptor<&'static str> {
    Acceptor {
        promised: None,
        accepted_proposal: None,
    }
}

fn prepare(proposal_number: ProposalNumber) -> AcceptorMessage<&'static str> {
    AcceptorMessage::Prepare { proposal_number }
}

fn accept(proposal_number: ProposalNumber, value: &'static str) -> AcceptorMessage<&'static str> {
    AcceptorMessage::Accept {
        proposal_number,
        value,
    }
}

#[test]
fn promise_reports_previously_accepted_proposal() {
    let acceptor = fresh();

    let acceptor = acceptor.transition(prepare(pn(1, 1))).new_acceptor_state;
    let acceptor = acceptor
        .transition(accept(pn(1, 1), "x"))
        .new_acceptor_state;
    let output = acceptor.transition(prepare(pn(2, 2)));

    assert_eq!(
        output.reply,
        Some(ProposerMessage::Promise {
            proposal_number: pn(2, 2),
            accepted: Some(AcceptedProposal {
                proposal_number: pn(1, 1),
                value: "x",
            }),
        })
    );
    assert_eq!(output.announce, None);
    assert!(output.persist);
}

#[test]
fn lower_prepare_after_promise_is_rejected() {
    let acceptor = fresh();

    let acceptor = acceptor.transition(prepare(pn(2, 2))).new_acceptor_state;
    let output = acceptor.transition(prepare(pn(1, 1)));

    assert_eq!(
        output.reply,
        Some(ProposerMessage::Reject { promised: pn(2, 2) })
    );
    assert_eq!(output.announce, None);
    assert_eq!(output.new_acceptor_state, acceptor);
    assert!(!output.persist);
}

#[test]
fn accept_below_promise_is_rejected() {
    let acceptor = fresh();

    let acceptor = acceptor.transition(prepare(pn(2, 2))).new_acceptor_state;
    let output = acceptor.transition(accept(pn(1, 1), "x"));

    assert_eq!(
        output.reply,
        Some(ProposerMessage::Reject { promised: pn(2, 2) })
    );
    assert_eq!(output.announce, None);
    assert_eq!(output.new_acceptor_state, acceptor);
    assert!(!output.persist);
}

#[test]
fn accept_is_answered_with_accepted() {
    let acceptor = fresh();

    let acceptor = acceptor.transition(prepare(pn(1, 1))).new_acceptor_state;
    let output = acceptor.transition(accept(pn(1, 1), "x"));

    assert_eq!(output.reply, None);
    assert_eq!(
        output.announce,
        Some(LearnerMessage::Accepted {
            proposal_number: pn(1, 1),
            value: "x",
        })
    );
    assert!(output.persist);
}

#[test]
fn accept_raises_promise_so_lower_accept_is_rejected() {
    let acceptor = fresh();

    let acceptor = acceptor.transition(prepare(pn(3, 3))).new_acceptor_state;
    let acceptor = acceptor
        .transition(accept(pn(5, 2), "x"))
        .new_acceptor_state;
    let output = acceptor.transition(accept(pn(4, 1), "y"));

    assert_eq!(
        output.reply,
        Some(ProposerMessage::Reject { promised: pn(5, 2) })
    );
    assert_eq!(output.announce, None);
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
    let output = fresh().transition(accept(pn(1, 1), "x"));

    assert_eq!(output.reply, None);
    assert_eq!(
        output.announce,
        Some(LearnerMessage::Accepted {
            proposal_number: pn(1, 1),
            value: "x",
        })
    );
    assert!(output.persist);
}

#[test]
fn duplicate_prepare_is_promised_again() {
    let acceptor = fresh().transition(prepare(pn(1, 1))).new_acceptor_state;
    let output = acceptor.transition(prepare(pn(1, 1)));

    assert_eq!(
        output.reply,
        Some(ProposerMessage::Promise {
            proposal_number: pn(1, 1),
            accepted: None,
        })
    );
    assert_eq!(output.announce, None);
}
