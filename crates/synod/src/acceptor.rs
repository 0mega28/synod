use crate::message::{
    AcceptedProposal, AcceptorMessage, LearnerMessage, ProposalNumber, ProposerMessage,
};

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

impl<V: Clone + Eq> Acceptor<V> {
    pub fn transition(mut self, message: AcceptorMessage<V>) -> AcceptorOutput<V> {
        match message {
            AcceptorMessage::Prepare { proposal_number } => {
                if let Some(promised) = self.promised
                    && promised > proposal_number
                {
                    return AcceptorOutput {
                        new_acceptor_state: self,
                        reply: Some(ProposerMessage::Reject { promised }),
                        announce: None,
                        persist: false,
                    };
                }
                let persist = self.promised != Some(proposal_number);
                self.promised = Some(proposal_number);
                AcceptorOutput {
                    reply: Some(ProposerMessage::Promise {
                        proposal_number,
                        accepted: self.accepted_proposal.clone(),
                    }),
                    new_acceptor_state: self,
                    announce: None,
                    persist,
                }
            }
            AcceptorMessage::Accept {
                proposal_number,
                value,
            } => {
                if let Some(promised) = self.promised
                    && promised > proposal_number
                {
                    return AcceptorOutput {
                        new_acceptor_state: self,
                        reply: Some(ProposerMessage::Reject { promised }),
                        announce: None,
                        persist: false,
                    };
                }
                // TODO: revisit whether to assert or ignore the message.
                if let Some(accepted_proposal) = &self.accepted_proposal
                    && accepted_proposal.proposal_number == proposal_number
                {
                    assert!(accepted_proposal.value == value);
                }
                assert!(
                    self.accepted_proposal.as_ref().map(|p| p.proposal_number) <= self.promised,
                    "accepted proposal is above the promise"
                );
                // Only the accepted number needs comparing: accepted is always <= promised,
                // and the guard above means promised <= proposal_number, so if accepted is
                // already proposal_number, promised is too.
                let persist =
                    self.accepted_proposal.map(|p| p.proposal_number) != Some(proposal_number);

                self.promised = Some(proposal_number);
                self.accepted_proposal = Some(AcceptedProposal {
                    proposal_number,
                    value: value.clone(),
                });
                AcceptorOutput {
                    new_acceptor_state: self,
                    reply: None,
                    announce: Some(LearnerMessage::Accepted {
                        proposal_number,
                        value,
                    }),
                    persist,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
