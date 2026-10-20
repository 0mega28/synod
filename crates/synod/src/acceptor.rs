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
mod tests;
