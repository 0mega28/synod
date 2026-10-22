use crate::{
    acceptor::{Acceptor, AcceptorOutput},
    learner::{Learner, LearnerOutput},
    message::{AcceptorMessage, LearnerMessage, Message, NodeId, ProposalNumber},
    proposer::{Proposer, ProposerEvent, ProposerOutput},
};

#[derive(Debug, Clone)]
pub struct Node<V> {
    pub acceptor: Acceptor<V>,
    pub proposer: Proposer<V>,
    pub learner: Learner<V>,
    pub id: NodeId,
    pub nodes: Vec<NodeId>,
}

pub enum NodeEvent<V> {
    Message { from: NodeId, message: Message<V> },
    ClientRequest { value: V },
    Timeout { proposal_number: ProposalNumber },
    Start,
}

pub enum Persist {
    Acceptor,
    Proposer,
}

pub struct NodeOutput<V> {
    pub new_node_state: Node<V>,
    pub messages: Vec<(NodeId, Message<V>)>,
    pub done: Option<V>,
    pub persist: Option<Persist>,
    pub timer: Option<ProposalNumber>,
}

impl<V: Clone + Eq> Node<V> {
    pub fn transition(self, event: NodeEvent<V>) -> NodeOutput<V> {
        match event {
            NodeEvent::Message {
                from,
                message: Message::ToAcceptor(message),
            } => self.run_acceptor(from, message),
            NodeEvent::Message {
                from,
                message: Message::ToProposer(message),
            } => self.run_proposer(ProposerEvent::Message { from, message }),
            NodeEvent::Message {
                from,
                message: Message::ToLearner(message),
            } => self.run_learner(from, message),
            NodeEvent::ClientRequest { value } => {
                self.run_proposer(ProposerEvent::ClientRequest { value })
            }
            NodeEvent::Timeout { proposal_number } => {
                self.run_proposer(ProposerEvent::Timeout { proposal_number })
            }
            NodeEvent::Start => self.run_proposer(ProposerEvent::Learn),
        }
    }

    fn run_acceptor(mut self, from: NodeId, message: AcceptorMessage<V>) -> NodeOutput<V> {
        let AcceptorOutput {
            new_acceptor_state,
            reply,
            announce,
            persist,
        } = self.acceptor.transition(message);
        self.acceptor = new_acceptor_state;

        let mut messages = Vec::new();
        if let Some(reply) = reply {
            messages.push((from, reply.into()));
        }
        if let Some(announce) = announce {
            messages.extend(
                self.nodes
                    .iter()
                    .map(|&node| (node, announce.clone().into())),
            );
        }

        NodeOutput {
            new_node_state: self,
            messages,
            done: None,
            persist: persist.then_some(Persist::Acceptor),
            timer: None,
        }
    }

    fn run_proposer(mut self, proposer_event: ProposerEvent<V>) -> NodeOutput<V> {
        let ProposerOutput {
            new_proposer_state,
            send,
            timer,
            persist,
            done,
        } = self
            .proposer
            .transition(self.id, &self.nodes, proposer_event);
        self.proposer = new_proposer_state;

        let messages = if let Some((nodes, message)) = send {
            nodes
                .iter()
                .map(|node| (*node, message.clone().into()))
                .collect()
        } else {
            vec![]
        };

        NodeOutput {
            new_node_state: self,
            messages,
            done,
            persist: persist.then_some(Persist::Proposer),
            timer,
        }
    }

    fn run_learner(mut self, from: NodeId, message: LearnerMessage<V>) -> NodeOutput<V> {
        let LearnerOutput {
            new_learner_state,
            chosen,
        } = self.learner.transition(&self.nodes, from, message);
        self.learner = new_learner_state;

        if let Some(value) = chosen {
            return self.run_proposer(ProposerEvent::Chosen { value });
        }
        NodeOutput {
            new_node_state: self,
            messages: vec![],
            done: None,
            persist: None,
            timer: None,
        }
    }
}

#[cfg(test)]
mod tests;
