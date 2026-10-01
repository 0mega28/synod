# Single Decree Paxos

Notes on [Paxos Made Simple](https://lamport.azurewebsites.net/pubs/paxos-simple.pdf).

## Roles in Consensus Algorithm
- Proposers
- Acceptors
- Learners

## Glossary
- Prepare: A proposer asks a majority to promise n and report what they've accepted.
- Issue: Issue a proposal request (n, v) after majority of the acceptors have responded.
    - v is the value from the highest number proposal (if any)
    - or, a value is picked by the proposer only if no acceptor responded with any proposal that they have accepted
- Accept: Acceptor accepts an accept request (accept phase) unless it promised a higher number.
- Choose: A value is chosen when a majority of the same proposal request is accepted.
- Promise: Acceptor promised (prepare request) to not accept any proposal with number less than n.
- Learn: what a learner does. It finds out that a value has been chosen.

## Choosing a value
1. An Acceptor must accept the first proposal that it receives
2. Every proposal must have different numbers – figure it out in the implementation
3. A value is chosen when a single proposal with that value has been accepted 
    by majority of the acceptors – Proposal has been chosen

### Prepare Phase
1. A Proposer selects a proposal number n sends a prepare request to majority of the acceptor
2. If an acceptor receives a prepare with number n greater than any prepare that it has already
    responded, then it responds to the request with a promise not to accept any proposal number
    less than n, and the proposal with highest number (if any) that it has accepted

### Accept Phase
1. If a Proposer receives a response from the majority of the acceptor then it sends a accept request
    to each of those acceptor for a proposal number n with a value v, where v is the value of the 
    highest number proposal among the response, or a value picked by the proposer (if no acceptor 
    has accepted any proposal request in the responding majority).
2. If an Acceptor receives an accept request for a proposal number n, it accepts the proposal unless
    it has already responded to a prepare request with proposal number greater than n.

### TODO Optimization

## Learning a chosen value
An acceptor can broadcast to each of the learners the value they have accepted
    - High network cost
An acceptor can inform a distinguished learner
    - What if the learner goes down
An acceptor can broadcast their acceptance to some distinguished set of learners
    and in-turn they can broadcast to all the learners
    

A learner can request a proposer to issue a proposal and then can observe a majority of 
    acceptor that accepted the same proposal

## Progress
Progress needs a single distinguished proposer. Randomness and timeouts
help select one.
Two proposers at once breaks progress (livelock) but never safety.

## Implementation
Acceptor must record its decision in storage before sending the response.
With a bare majority, one acceptor forgetting is enough to lose a chosen value.

Proposal numbers must be unique across nodes, and any node must be able to
go higher than any number it has seen. Proposal number is (round, node_id),
compared round first, node_id breaks ties.
After a reject with (7, node 3), the node uses (8, own_id).

Each Proposer must record the highest round that it has issued, in storage,
before sending the prepare request. node_id is fixed, so only the round.
Otherwise a restart can reuse a number: (6, x) and (6, y) from the same node.

---

# TODO
- P-Chain blog post
