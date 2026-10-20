//! Multi-Paxos consensus core.

pub mod acceptor;
pub mod learner;
pub mod message;
pub mod proposer;
pub mod transport;

#[cfg(test)]
mod test_support;
