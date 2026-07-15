# Protocol and implementation notes

## Contract

Three fixed replica identities, numbered 0-2, share a single initial quota Q. Each request spends exactly one unit. A request ID is the canonical string `home:sequence`, where home is 0-2 and sequence is 0-10^12. The home encoded in the ID is mandatory. A wrong-home request is rejected, not forwarded. Clients retry the same ID at the same home.

The server response is `OK NEW`, `OK DUP`, `DEFER reason` or `ERR INPUT` (the first three include the request ID). NEW/DUP describe first and repeated acknowledgements at the home. A deferred request may later succeed. An owner timeout is an ambiguous result: the owner may already have spent the unit. Retrying the same ID resolves that ambiguity without spending again. Never change an ID merely because a response was lost.

## State and transitions

The implementation specialises Figure 2 of Balegas et al. [1] to one fixed initial allocation. `R` is a 3x3 matrix and `S` has three cumulative spending totals. `R[i,i]=A[i]` is immutable; off-diagonal `R[i,j]` is the cumulative amount transferred from i to j. Replica i alone advances its row and S[i]. The wire representation flattens R row-major followed by S.

The local spending capacity at i is:

```
B_i = A_i + sum(R[j,i], j != i) - sum(R[i,j], j != i) - S_i
```

The code has three safety transitions. A spend requires `B_i >= 1`, then increments S[i]. A positive transfer to j requires `B_i >= amount`, then advances `R[i,j]` **before sending a state message**. A merge takes the maximum of corresponding components. Duplicate and older state messages cannot lower components or apply a transfer twice. Diagonal changes and remotely advanced receiver-owned entries are rejected. Input snapshots and resulting states must have nonnegative balances and spending no greater than Q. These checks detect malformed input, not arbitrary Byzantine fabrication.

`src/counter.hpp` contains the safety mechanism. `src/server.cpp` supplies networking, deduplication and policy. Each server executes all state access in one `poll` event loop. A client callback and a peer callback cannot interleave a rights check with another state transition. Independent server processes do run concurrently. This deliberately simple serialisation mechanism is part of the correctness assumptions.

## Conservation argument for this implementation

Let G[i,j] and C[i] denote the latest actual outgoing-transfer and spending entries owned by i, whether or not other replicas have received them. Define the true remaining allocation `L_i = A_i + incoming G - outgoing G - C_i`. Initially L=A and C=0.

A local server knows its own outgoing G row and C exactly. It may lack incoming transfers. Thus its locally computed B is no larger than its true L. Spending after a local nonnegative check reduces both B and L by one and raises global spending by one. Transferring n reduces the sender's true L by n and increases the recipient's true L by n. The recipient cannot use these newly allocated rights until it has learned the transfer. The sender cannot use them again, including if its datagram is lost. A merge changes knowledge, not the true ledger.

Inductively every true L stays nonnegative and `sum(L) + sum(C) = Q`. Consequently `sum(C) <= Q`. Successful request IDs are recorded at the same serialised home transition as the first spend. Repeating an accepted ID returns a cached success without another increment. The central baseline additionally deduplicates all IDs at the owner. Therefore unique acknowledged spending cannot exceed actual spending, and actual spending cannot exceed Q.

This argument assumes the code implements the specified transitions, intact in-memory state, one writer per replica identity, consistent initial configuration, non-Byzantine processes and authentic messages from the current run. It is not a proof of the entire C++/OS stack. Tests and finite exploration check particular implementations and executions under those assumptions.

If operations cease and all latest state components are eventually disseminated, every view reaches the same component maxima. Maximum is associative, commutative and idempotent. Therefore delivery order and duplicate count do not affect that converged state. Periodic full-state gossip retries lost information; arbitrary permanent partitions provide no convergence guarantee.

## Policy

Threshold mode observes recent client activity (100 ms) and asks both peers when local rights are at or below 8. It sends at most one request per peer per 20 ms, asking for a cumulative incoming transfer target equal to the locally observed incoming component plus 16. The donor grants the positive difference between the target and its already-written transfer component, limited by its current rights. Duplicate NEED messages therefore cannot repeatedly grant the same target. The donor then sends its full state; periodic gossip is every 50 ms.

This policy has no forecast, fairness guarantee or soft reserve. Stale requests can overfetch, donors can be drained, and contention can cause rights to move back and forth. Those are policy limitations; a grant still goes through the safety transition. Idle replicas do not continually initiate borrowing. The `TRANSFER` administrative command is for focused tests and explanation; the partition demonstration uses automatic threshold borrowing.

## Baselines and transport

- **central:** initial rights `[Q,0,0]`; all successful spends occur at server 0. Requests still enter their fixed home TCP endpoint. Homes 1 and 2 forward an asynchronous ASK to the owner and await ANSWER for up to 100 ms. The owner deduplicates IDs across homes. The client retries timeouts. Owner 0 can remain available when isolated from the other servers; other homes require its communication path. This is one owner, not replicated consensus or a durable transactional database.
- **fixed:** rights are divided approximately equally. No NEED requests or transfers are permitted. Local exhaustion always defers even if unused rights exist elsewhere.
- **threshold:** the same distributed state plus the policy above. Exhausted homes defer while rights are requested asynchronously.

TCP clients use persistent newline-delimited connections. Peer communication uses UDP on separate loopback ports. All three servers gossip state, including baseline modes, for comparable state observability and convergence checks. In the normal experiments peers communicate directly. Fault experiments route peer datagrams through a Python relay that schedules delay, jitter, duplication and link drops. It cannot decide admissions or create rights. TCP clients bypass this relay.

The packet header carries a run epoch, source, destination and source-local message sequence. Each test run creates a fresh epoch. The receiver also checks the source UDP port. This prevents accidental mixing of runs and endpoints on the test host; it is not authentication against a malicious local process. No production security claim is made. Restarting a member within the same run is unsupported and can violate safety if previously spent rights are recreated. Start a completely new run of all servers for a new experiment; never interpret that as recovery of the old quota.

## Bounds and error handling

All stored counters are unsigned 64-bit. Parsing checks sign, range and complete consumption. A checked addition prevents wraparound; 128-bit signed intermediates avoid underflow in the three-replica balance formula. Even with no replenishment, cumulative transfers can eventually overflow after repeated circulation: the attempted increment is rejected without mutating state. Initial allocation addition is checked too.

Each server allows at most 128 client connections, 4 KiB of unconsumed client input and 64 KiB of response backlog per connection. Successful-ID sets have a configurable cap of at most two million entries. Crossing it defers new IDs with CAPACITY; known successes remain replayable. A fault relay holds at most 10,000 queued datagrams; excess is dropped. UDP payloads must fit below 4 KiB. Per-loop work budgets prevent a busy client or peer socket monopolising the event loop indefinitely. A silent client occupying a connection is not an adversarial-load scenario tested here.

## Verification boundary

The Python checker reads complete JSONL logs and uses Python integers. It rebuilds per-server views and an independent global ownership ledger, checks preconditions, enforces one spend per ID, links receives to actual prior sends, checks publication snapshots and verifies conservation after every event. It compares final states only after borrowing and periodic gossip are paused, explicit synchronisation completes and the relay drains. Client observations are cross-checked when present. Audit timestamps use the same host's monotonic clock; this ordering assumption must change for a physical cluster.

The exhaustive C++ test explores three replicas with Q=2, initial `[1,1,0]`, unit spends/transfers and at most eight transitions. Publishing puts immutable snapshots into a persistent unordered pool; delivery can choose any snapshot repeatedly. A visited-state set removes repeated worlds. Every reached world is checked for quota safety and convergence after quiescent dissemination. This explores the counter, not TCP parsing, request-ID deduplication, borrowing timers or the central RPC protocol. Real-process scenarios cover those additional areas.

Stress runs deliberately use snapshot-only logging to bound evidence volume. They check final client counts against converged spending, but are not claimed as independently replayed histories.

## References

[1] V. Balegas, D. Serra, S. Duarte, C. Ferreira, R. Rodrigues, N. Preguiça, M. Shapiro and M. Najafzadeh (2015), *Extending Eventually Consistent Cloud Databases for Enforcing Numeric Invariants*, arXiv:1503.09052v1, Figure 2 and Sections II-III. https://arxiv.org/abs/1503.09052

[2] P. Bailis, A. Fekete, M. J. Franklin, A. Ghodsi, J. M. Hellerstein and I. Stoica (2014), *Coordination Avoidance in Database Systems (Extended Version)*, arXiv:1402.2237v4, Sections 3-5 and 8. https://arxiv.org/abs/1402.2237
