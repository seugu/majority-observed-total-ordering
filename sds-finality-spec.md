---
title: SDS-FINALITY
name: Majority-observed total order for MAIN messages on top of SDS
status: raw
category: Standards Track
tags: sds, ordering, finality
editor: TBD
contributors: TBD
---

# SDS-FINALITY

## Abstract

A fixed group of N members exchanges messages through [SDS] over a lossy
channel. This protocol makes every member finalize the same sequence of MAIN
messages: position 1, 2, 3, and so on.

A MAIN is finalized at position k once a strict majority of members report it
as the first MAIN they have seen outside positions 1..k−1.

SDS is used to spread messages and repair losses. Its causal delivery is not
waited for: the order comes only from the rules below.

Two variants are defined (§4.6). A is the base protocol. B adds periodic
re-gossip and is RECOMMENDED.

RFC 2119 keywords apply.

## 1. Model

- **Members.** There are N members, fixed and known to all. The quorum is
  Q = ⌊N/2⌋ + 1, for example 4 for N = 6.
- **Honest and online.** Every member is honest and online.
- **Channel.** Messages may be lost, duplicated or delayed.
- **Rate limit.** At most one MAIN is released per interval across the whole
  network, for example one per minute. This is enforced outside this protocol,
  and safety does not depend on it.
- **No restarts.** Members never restart or lose state.
- **Message IDs.** IDs are collision-resistant hashes (§2.3).

## 2. Messages

Every message is an ordinary [SDS] message. Its type lives in the content.

| Type | Purpose | Carries |
|---|---|---|
| MAIN | Application payload. The only type that gets ordered. Rate limited. | view, payload |
| SEEN | "I have admitted these mains." Sent at once, not rate limited. | view |
| SYNC | Heartbeat, so that gap detection and repair keep running while a member has nothing else to send. | nonce |

### 2.1 Envelope

| Field | Rule |
|---|---|
| `type` | MAIN, SEEN or SYNC. |
| `msg_id`, `sender` | Equal to the SDS message ID and the SDS sender. |
| `view` | MAIN and SEEN only. |
| `payload` | MAIN only. |
| `nonce` | SYNC only. A per-sender counter. |

### 2.2 View

```
view = (base, frontier)
  base     = |P| at the sender when it sends
  frontier = the sender's admitted mains that are not yet finalized,
             in its admission order (MUST be complete)
```

A view says: "Beyond the first `base` finalized mains, these are all the mains
I have, in the order I got them."

### 2.3 IDs

| Type | ID |
|---|---|
| MAIN | H("MAIN" ‖ sender ‖ base ‖ frontier IDs except its own ‖ H(payload)) |
| SEEN | H("SEEN" ‖ sender ‖ view) |
| SYNC | H("SYNC" ‖ sender ‖ nonce) |

## 3. State per member

| State | Meaning |
|---|---|
| L | Admission log of mains. Append-only. |
| P | Finalized prefix p₁, p₂, … Append-only. Pₖ denotes its first k entries. |
| views[u] | Every view received from member u, and every view this member has published. |

## 4. Rules

### 4.1 Receiving

Everything here happens as soon as a message's bytes arrive. Nothing waits for
SDS to release the message from its causal buffer.

1. **Record views.** When a MAIN or SEEN arrives, record its view in
   `views[sender]`.
2. **Admit mains.** When a MAIN m arrives, admit m once every frontier entry
   before m itself is in L or P. Admitting m appends it to L.
3. **Never reply to SEEN or SYNC.**

Why not wait for SDS: one lost message that nobody can repair any more keeps
everything that causally follows it in the SDS buffer, mains and votes
included. Waiting would stall finality for good.

Rule 2 is the only ordering constraint the protocol needs. It keeps
main-to-main happened-before: a main never enters L before the mains its sender
had already admitted.

### 4.2 Sending

- **MAIN.** Use frontier = (L \ P) followed by m. Publish, then append m to L.
  The MAIN is the sender's report on m, so the sender sends no SEEN for its own
  main.
- **SEEN.** After any admission, publish exactly one SEEN carrying the current
  view. That one SEEN covers every main admitted in the same pass.
- **SYNC.** Every T_sync, publish a SYNC if nothing has been published since the
  last tick (T_sync per variant, §4.6).
- **Own views.** On publish, add the view to `views[self]`. A member counts its
  own vote only from views it has published.

### 4.3 Voting

The vote of member u for position k is computed as follows:

- Take any view of u with base ≤ k−1.
- Its vote is the first frontier entry that is not in Pₖ₋₁.
- A view with base > k−1 is ignored for k.
- A view whose whole frontier is in Pₖ₋₁ gives no vote.

All usable views of u give the same vote (§6), so each member has at most one
vote per position.

### 4.4 Finalization

1. Let k = |P| + 1.
2. If some main x has votes from at least Q distinct members, append x to P.
3. Repeat for k+1.

### 4.5 Application delivery

Deliver P to the application in order. Finalization is by ID.

A member can finalize a main it has not received, from other members' votes.
In that case the order is already fixed, but delivery of pₖ and everything
after it waits until pₖ arrives.

### 4.6 Variants

**Variant A (base).** Rules 4.1–4.5 only.

- A lost message comes back only through SDS-R.
- SDS-R requests travel on outgoing messages, so A needs SYNC as their carrier.
- RECOMMENDED: T_sync = 30 s.

**Variant B (RECOMMENDED): A plus re-gossip.**

- Every T_regossip (RECOMMENDED 20 s), a member that holds non-finalized mains
  re-broadcasts:
  - its latest published MAIN or SEEN, and
  - every main in L \ P.
- Re-broadcasts are byte-identical: same message, same ID.
- The member stops once every main it holds is finalized.
- RECOMMENDED: T_sync ≥ 120 s, or no SYNC at all.

Re-gossip only repeats existing messages, and receivers drop duplicates by ID.
It cannot change any view or vote, so safety (§6) is unaffected.

**Why B.** Measured in simulation on real nim-sds with N = 50:

- **Latency.** With B, finality stays close to the maximum propagation delay
  for up to 40% loss. With A it degrades 4–6× at 30–40% loss, because recovery
  waits on SDS-R timers (30–300 s).
- **Split votes.** With A, nodes on bad links could cause a permanent split
  vote (§8). B finished in every run.
- **Cost.** B re-sends payloads: about 2× the bytes of A with 20 kB mains.
  Packet counts were similar, because B needs almost no SYNC.

## 5. SDS configuration

- `maxCausalHistory` MUST be greater than 10 and the same at every member.
  RECOMMENDED: max(11, 2N).
- SDS-R is enabled, and the host rebroadcasts repair responses.
- Messages retrieved out of band are passed back through the SDS receive path.

## 6. Safety

**Claim.** No two members finalize different mains at the same position.

1. **Views are fixed.** A view never changes after it is sent, and every member
   that receives it reads the same content (honest senders, hash IDs).
2. **One vote per member.** L only grows. A later view of u can only add mains
   after the ones an earlier view listed, so all usable views of u give the same
   vote for k.
3. **Quorums intersect.** Two sets of Q members share at least one member,
   because 2Q > N. That member's vote is the same in both sets, so both
   finalize the same main. By induction on k, starting from P₀ = ∅, P is
   identical everywhere.

The proof uses only the content of views and the fact that each L only grows.
It never depends on SDS delivery, so acting on arrival (§4.1) keeps safety.

**No Condorcet cycles.** Positions are decided one at a time, and each member
contributes a single value per position. Mains are never compared in pairs, so
no preference cycle can form.

## 7. Resulting order

- Position k holds the main that a majority admitted first among the mains not
  yet finalized.
- If m's sender had admitted m′ before publishing m, then m′ precedes m in P.
- The result matches release order when each main reaches a majority before the
  next one is released, that is, when propagation time is shorter than the
  interval. A loss that needs repair can break this. Even then, the order is
  identical at every member.

**Example (N = 6, Q = 4).**

1. b releases X. One second later e releases Y.
2. a, b, c and d admit X first; e and f admit Y first.
3. Position 1 gets votes X, X, X, X, Y, Y, so X is finalized.
4. At position 2, every member's first main outside {X} is Y, so Y is
   finalized.

## 8. Limitations

- **Honest members only.** A member that publishes conflicting views breaks
  safety. Tolerating that needs signatures and a quorum above 2N/3.
- **Split votes.** A tie such as 3–3 at N = 6 stalls that position and every
  position after it. Simulation hit this with variant A when 20% of nodes had
  60% loss. A slow main reached some members only after the next main, so
  they voted for different mains at the same position.
- **Content availability.** A member can finalize a main it never receives.
  The order is fixed, but application delivery waits.
- **Privacy.** A SEEN reveals when, and in which order, a member admitted mains.
- **Size.** A view costs about 32 bytes per frontier entry.
- **[SDS-FS].** Views live in encrypted content, so a vote is counted only once
  the receiver can decrypt it. Content whose epoch key was deleted can never be
  counted.

## 9. Open questions

- **OQ1.** How to resolve split votes.
- **OQ2.** How to support restarts. This needs durable L, P and published views,
  and must never republish a conflicting view.
- **OQ3.** How a member gets a finalized main it never received. With variant B
  every member received every finalized main in simulation. With A this
  depends on SDS-R.
- **OQ4.** Whether SYNC should be sent outside the causal chain. It no longer
  blocks finality, but it still fills the SDS buffer.

## References

- **[SDS]** LIP-109, *Scalable Data Sync protocol for distributed logs*.
- **[SDS-FS]** *Forward Secrecy with SDS*.
