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
            LearnerMessage::Accepted {
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
