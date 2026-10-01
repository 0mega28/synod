# synod

A strongly consistent key-value store in Rust, built on a from-scratch
Multi-Paxos replicated log.

The name comes from Lamport's *The Part-Time Parliament*, where the
single-decree protocol at the heart of Paxos is called the Synod algorithm.
Multi-Paxos is, roughly, that algorithm run once per slot of a log.

## Why this exists

This is a learning project. I want to understand consensus well enough to
build it, break it, and explain it, and this repo is where that happens.
It is not production software and makes no attempt to be.

Alongside the code, I'm writing a blog series that walks through the
design decisions, the bugs, and the things that surprised me. [Link coming
soon.]

## Design

The goal is a Paxos implementation that never touches the outside world: a
pure state machine where messages and clock ticks go in, and messages to
send, state to persist, and entries to apply come out. No sockets, no
wall-clock time, no randomness.

That would make it possible to run an entire cluster inside a
deterministic simulator that can drop, delay, reorder, and duplicate
messages, and crash nodes with or without their durable state. Every run
would be reproducible from a seed, so a failure is a bug with a replayable
trace rather than a flaky test.

I'm not starting there. The plan is to build the naive version first, wire
it straight to a real network, and feel why that is hard to test before
refactoring toward the pure core and the simulator. The mistakes along the
way are part of the point.

## Layout

```
crates/
  synod/          consensus core
  synod-island/   deterministic simulator (arrives in M7)
docs/
  learning-log.md running notes: surprises and open questions
```

More crates (the KV state machine, a networked node binary, a
linearizability checker) arrive as the project reaches them.

## Roadmap

- [ ] **M1:** Single-decree Paxos in one process, messages passed by hand
- [ ] **M2:** Break it: reordered, dropped, and duplicated messages, and
      acceptor crashes
- [ ] **M3:** Real network: separate processes, timeouts, and retries
- [ ] **M4:** Multi-Paxos: a log of slots and a stable leader
- [ ] **M5:** KV store on the log, with client deduplication
- [ ] **M6:** Crash recovery: persistence and snapshots
- [ ] **M7:** Deterministic simulation with seeded, replayable runs

After that: formal verification of the protocol and parts of the code.

Out of scope: membership changes, read leases.

## Running

```
cargo build
cargo test
```

## How this was built

The consensus protocol, simulator, and core design are my own work. I used
an LLM as a tutor and code reviewer, and for mechanical scaffolding like CI
and build configuration.

## Reading

Foundations:

- Leslie Lamport, *Time, Clocks, and the Ordering of Events in a
  Distributed System* (1978)
- Michael J. Fischer, Nancy A. Lynch, and Michael S. Paterson,
  *Impossibility of Distributed Consensus with One Faulty Process* (1985)
- Martin Kleppmann, *Designing Data-Intensive Applications*, chapter 9
- Will Wilson, *Testing Distributed Systems w/ Deterministic Simulation*
  (Strange Loop talk, 2014)

Paxos:

- Leslie Lamport, *Paxos Made Simple* (2001)
- Leslie Lamport, *The Part-Time Parliament* (1998)
- Robbert van Renesse and Deniz Altinbuken, *Paxos Made Moderately
  Complex* (2015)
- Tushar Chandra, Robert Griesemer, and Joshua Redstone, *Paxos Made Live*
  (2007)

Correctness:

- Maurice Herlihy and Jeannette Wing, *Linearizability: A Correctness
  Condition for Concurrent Objects* (1990)

