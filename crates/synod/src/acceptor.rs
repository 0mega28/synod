use crate::message::{
    AcceptedProposal, AcceptorMessage, LearnerMessage, ProposalNumber, ProposerMessage,
};
use std::cmp::Ordering;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Acceptor<V> {
    pub promised: Option<ProposalNumber>,
    pub accepted_proposal: Option<AcceptedProposal<V>>,
}

#[derive(Debug)]
pub struct AcceptorOutput<V> {
    pub new_acceptor_state: Acceptor<V>,
    /// Goes back to the sender of the message.
    pub reply: Option<ProposerMessage<V>>,
    /// Goes to every node.
    pub announce: Option<LearnerMessage<V>>,
    pub persist: bool,
}

impl<V: Clone> Acceptor<V> {
    pub fn transition(&self, message: AcceptorMessage<V>) -> AcceptorOutput<V> {
        let Acceptor {
            promised,
            accepted_proposal,
        } = self;
        match message {
            AcceptorMessage::Prepare { proposal_number } => {
                if let Some(promised) = promised {
                    return match promised.cmp(&proposal_number) {
                        Ordering::Less | Ordering::Equal => AcceptorOutput {
                            new_acceptor_state: Acceptor {
                                promised: Some(proposal_number),
                                accepted_proposal: accepted_proposal.clone(),
                            },
                            reply: Some(ProposerMessage::Promise {
                                proposal_number,
                                accepted: accepted_proposal.clone(),
                            }),
                            announce: None,
                            persist: true,
                        },
                        Ordering::Greater => AcceptorOutput {
                            new_acceptor_state: self.clone(),
                            reply: Some(ProposerMessage::Reject {
                                promised: *promised,
                            }),
                            announce: None,
                            persist: false,
                        },
                    };
                }
                AcceptorOutput {
                    new_acceptor_state: Acceptor {
                        promised: Some(proposal_number),
                        accepted_proposal: accepted_proposal.clone(),
                    },
                    reply: Some(ProposerMessage::Promise {
                        proposal_number,
                        accepted: accepted_proposal.clone(),
                    }),
                    announce: None,
                    persist: true,
                }
            }
            AcceptorMessage::Accept {
                proposal_number,
                value,
            } => {
                if let Some(promised) = promised
                    && *promised > proposal_number
                {
                    return AcceptorOutput {
                        new_acceptor_state: self.clone(),
                        reply: Some(ProposerMessage::Reject {
                            promised: *promised,
                        }),
                        announce: None,
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
                    reply: None,
                    announce: Some(LearnerMessage::Accepted {
                        proposal_number,
                        value,
                    }),
                    persist: true,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
