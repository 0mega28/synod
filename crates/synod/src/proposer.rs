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
        rejected: HashSet<NodeId>,
    },
    Done {
        value: V,
    },
}

pub enum ProposerEvent<V> {
    ClientRequest { value: V },
    Message { from: NodeId, message: Message<V> },
    Chosen { value: V },
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
            // Duplicates, replies from an old round, and replies that arrive
            // after the phase moved on are normal on a real network.
            (
                ProposerEvent::Message {
                    message: Message::Promise { .. } | Message::Reject { .. },
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
            (
                ProposerEvent::Timeout,
                ProposerPhase::Prepare { .. } | ProposerPhase::Accept { .. },
            ) => {
                self.phase = ProposerPhase::Idle;
                ProposerOutput {
                    new_proposer_state: self,
                    send: None,
                    persist: false,
                }
            }
            (ProposerEvent::Chosen { value }, _) => {
                self.phase = ProposerPhase::Done { value };
                ProposerOutput {
                    new_proposer_state: self,
                    send: None,
                    persist: false,
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
mod tests;
