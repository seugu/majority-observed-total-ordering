## SDS-FINALITY simulator on top of the real nim-sds core.
##
## SDS layer: unmodified nim-sds protocol logic (wrapOutgoingMessage,
## unwrapReceivedMessage, runRepairSweep, onRepairReady), driven by a
## discrete-event loop with a virtual clock (sds.nim reads time via
## sdsGetTime(), see patches/). Not driven: resend / bloom cleanup (no-ops
## for this protocol). No Store retrieval.
##
## Host layer: the SDS-FINALITY spec (spec §4):
##   - MAIN / SEEN carry a view (base, frontier); SYNC is a heartbeat.
##   - views are recorded and MAINs admitted as soon as bytes arrive
##     (--votes / --admit = delivered restores "wait for SDS delivery").
##   - admission rule (gate b): a MAIN is admitted once every main listed
##     before it in its own frontier is already in L or P.
##   - one SEEN per admission pass; never reply to SEEN or SYNC.
##   - vote(u, k) = first frontier entry not in P[0..k-2] of any view of u
##     with base <= k-1; finalize when Q = N div 2 + 1 members agree.
## Variant B (not in spec yet): every node that has not finalized all mains
## periodically re-broadcasts its non-finalized mains and its latest view.
##
## Output: one JSON line per seed (fields documented in `report`).

import std/[heapqueue, tables, sets, random, strutils, json, os, sequtils,
            algorithm, math, parseopt, times]
import chronos, results
import sds

const
  CH = "ch"                          # single SDS channel
  BASE_MS = 1_760_000_000_000'i64    # virtual epoch (ms) fed to SDS
  SWEEP = 5.0                        # nim-sds repair sweep period (s)

type
  View = object
    base: int                        # |P| at sender when sent
    frontier: seq[string]            # sender's admitted, non-finalized mains, in its order
  Env = object                       # host envelope inside SDS content
    kind: char                       # 'M' main, 'S' seen, 'Y' sync
    sender: int
    msgId: string
    hasView: bool
    view: View
  Blob = ref object                  # serialized SDS message as sent on the wire
    id: string
    data: seq[byte]
  EvKind = enum evArrive, evSync, evSweep, evMain, evRegossip
  Ev = object
    t: float
    sq: int                          # tie-breaker: FIFO for equal times
    kind: EvKind
    node: int
    blob: Blob

  Node = ref object
    i: int
    rm: ReliabilityManager
    # SDS-delivery join (only matters for --votes/--admit=delivered):
    readyQueue: seq[string]          # ids signalled by onMessageReady during one unwrap
    readyPending: OrderedSet[string] # ready ids whose content is not yet cached
    contentCache: Table[string, Env] # received envelopes not yet SDS-delivered
    processed: HashSet[string]       # SDS-delivered (or own) ids
    # host state (spec §3):
    L, P: seq[string]                # admission log, finalized prefix
    Lset, Pset: HashSet[string]
    views: Table[int, seq[View]]     # member -> every view received from it (own included)
    votedMsgs: HashSet[string]       # msg ids whose view is already recorded
    mainHandled: HashSet[string]     # MAINs whose bytes this node has (submitted to the gate)
    mainFrontier: Table[string, seq[string]]  # main id -> frontier in that MAIN's own view
    gate: seq[string]                # received MAINs waiting for gate b
    unpublished: bool                # admitted mains not yet covered by a published view
    ownViewMsgs: seq[string]         # own MAIN/SEEN ids, in publish order
    publishedSinceSync: bool
    finTime: Table[string, float]    # main id -> time this node finalized it
    nonce: int
    repairOut: seq[seq[byte]]        # bytes handed out by onRepairReady during one sweep

  Params = object
    N, K, C, seed: int
    loss, badLoss, badFrac: float    # loss probabilities (0..1), fraction of bad-link nodes
    interval, minDelay, maxDelay: float
    syncInt, regossipInt, tEnd, wall: float
    offFrac, offAt, offDur: float    # offline window: fraction of nodes, start, duration
    payload: int                     # MAIN payload size (bytes)
    variant, votes, admit, delayDist, senders: string
    strip, runToEnd: bool

  Sim = ref object
    p: Params
    Q: int
    rng: Rand                        # loss/delay/timer randomness
    nodes: seq[Node]
    q: HeapQueue[Ev]
    sq: int
    blobs: Table[string, Blob]       # id -> latest bytes (for re-gossip)
    mainIds: seq[string]
    mainRelease: Table[string, float]
    bad: seq[bool]                   # node has a bad link (loss = badLoss to/from it)
    offline: seq[bool]               # node is offline during [offAt, offAt + offDur)
    pos: seq[float]                  # node position in [0,1] for --delay=geo
    sent, sentAfterDone: int         # broadcasts (one per send, not per receiver)
    bytes: int64                     # bytes broadcast (message size per send)
    tAllDone: float                  # first time every node finalized all K (-1 = never)
    voteInconsistency: bool          # sanity check: a member's views gave two different votes
    wallHit: bool

proc `<`(a, b: Ev): bool = (a.t, a.sq) < (b.t, b.sq)
proc N(s: Sim): int = s.p.N
proc K(s: Sim): int = s.p.K

# ---------- envelope encoding (host level, inside SDS content) ----------
# Plain text "kind;sender;id;base|-;frontier,csv;payload". Same fields as the
# spec's envelope; byte layout is not the spec's protobuf.
proc encode(e: Env, payload = 0): seq[byte] =
  let s = $e.kind & ";" & $e.sender & ";" & e.msgId & ";" &
    (if e.hasView: $e.view.base else: "-") & ";" & e.view.frontier.join(",") & ";" &
    repeat('x', payload)
  cast[seq[byte]](s)

proc decode(b: seq[byte]): Env =
  let p = cast[string](b).split(';')
  result.kind = p[0][0]
  result.sender = parseInt(p[1])
  result.msgId = p[2]
  result.hasView = p[3] != "-"
  if result.hasView:
    result.view.base = parseInt(p[3])
    result.view.frontier = if p[4].len > 0: p[4].split(',') else: @[]

# ---------- network model ----------
proc at(s: Sim, t: float, kind: EvKind, node = -1, blob: Blob = nil) =
  inc s.sq
  s.q.push(Ev(t: t, sq: s.sq, kind: kind, node: node, blob: blob))

proc setClock(t: float) =
  sdsSimTimeMs = BASE_MS + int64(t * 1000.0)

proc isOffline(s: Sim, i: int, t: float): bool =
  s.offline[i] and t >= s.p.offAt and t < s.p.offAt + s.p.offDur

proc delay(s: Sim, src, dst: int): float =
  ## One-way delay, independent per (packet, receiver).
  let (lo, hi) = (s.p.minDelay, s.p.maxDelay)
  case s.p.delayDist
  of "tail":   # 90% within the first 10% of the range, 10% spread over the rest
    let cut = lo + 0.1 * (hi - lo)
    if s.rng.rand(1.0) < 0.9: s.rng.rand(lo .. cut) else: s.rng.rand(cut .. hi)
  of "geo":    # correlated: distance between node positions + small jitter
    lo + abs(s.pos[src] - s.pos[dst]) * 0.9 * (hi - lo) + s.rng.rand(0.1 * (hi - lo))
  else:        # uniform
    s.rng.rand(lo .. hi)

proc broadcast(s: Sim, sender: int, blob: Blob, now: float) =
  inc s.sent
  s.bytes += blob.data.len
  if s.tAllDone >= 0: inc s.sentAfterDone
  for r in 0 ..< s.N:
    if r == sender: continue
    let loss = if s.bad[sender] or s.bad[r]: s.p.badLoss else: s.p.loss
    if s.rng.rand(1.0) < loss: continue
    let t = now + s.delay(sender, r)
    if s.isOffline(r, t): continue   # receiver offline at arrival: packet lost
    s.at(t, evArrive, r, blob)

# ---------- host: spec rules ----------
proc vote(s: Sim, n: Node, u, k: int): string =
  ## Vote of member u for position k, as seen by n ("" = no vote yet).
  result = ""
  for v in n.views.getOrDefault(u):
    if v.base <= k - 1:
      for x in v.frontier:
        if x notin n.Pset:
          if result.len == 0: result = x
          elif result != x: s.voteInconsistency = true
          break

proc finalize(s: Sim, n: Node, now: float): bool =
  ## Append to P while some main has >= Q votes for the next position.
  while true:
    let k = n.P.len + 1
    var c = initCountTable[string]()
    for u in 0 ..< s.N:
      let v = s.vote(n, u, k)
      if v.len > 0: c.inc(v)
    if c.len == 0: return
    let (x, cnt) = c.largest
    if cnt < s.Q: return
    n.P.add x; n.Pset.incl x
    n.finTime[x] = now
    result = true

proc admit(s: Sim, n: Node): bool =
  ## Gate b: admit every gated MAIN whose earlier frontier entries are in L or P.
  while true:
    var ready: seq[string]
    for mid in n.gate:
      let fr = n.mainFrontier[mid]
      if fr[0 ..< fr.len - 1].allIt(it in n.Lset or it in n.Pset): ready.add mid
    if ready.len == 0: return
    ready.sort()                     # deterministic tie-break
    for mid in ready:
      n.gate.keepItIf(it != mid)
      if mid in n.Pset or mid in n.Lset: continue
      n.L.add mid; n.Lset.incl mid
      n.unpublished = true
      result = true

proc wrapAndSend(s: Sim, n: Node, e: Env, now: float, payload = 0) =
  setClock(now)
  let r = waitFor n.rm.wrapOutgoingMessage(encode(e, payload), e.msgId, CH)
  doAssert r.isOk, "wrap failed"
  let blob = Blob(id: e.msgId, data: r.get())
  s.blobs[e.msgId] = blob
  n.processed.incl e.msgId
  if e.hasView:                      # own vote counts only once published (spec §4.2)
    n.views.mgetOrPut(n.i, @[]).add e.view
    n.votedMsgs.incl e.msgId
    n.ownViewMsgs.add e.msgId
  n.publishedSinceSync = true
  s.broadcast(n.i, blob, now)

proc currentFrontier(n: Node): seq[string] =
  for x in n.L:
    if x notin n.Pset: result.add x

proc hostLoop(s: Sim, n: Node, now: float) =
  ## Admit -> publish one SEEN if anything new was admitted -> finalize; repeat.
  while true:
    let a = s.admit(n)
    var published = false
    if n.unpublished:
      inc n.nonce
      let e = Env(kind: 'S', sender: n.i, msgId: "S" & $n.i & "-" & $n.nonce, hasView: true,
                  view: View(base: n.P.len, frontier: n.currentFrontier))
      s.wrapAndSend(n, e, now)
      n.unpublished = false
      published = true
    let f = s.finalize(n, now)
    if not (a or published or f): return

proc recordView(n: Node, e: Env): bool =
  if e.msgId in n.votedMsgs: return false
  n.votedMsgs.incl e.msgId
  n.views.mgetOrPut(e.sender, @[]).add e.view
  true

proc gateMain(n: Node, e: Env): bool =
  if e.msgId in n.mainHandled: return false
  n.mainHandled.incl e.msgId
  n.mainFrontier[e.msgId] = e.view.frontier
  n.gate.add e.msgId
  true

proc arrive(s: Sim, n: Node, blob: Blob, now: float) =
  setClock(now)
  n.readyQueue.setLen(0)
  let r = waitFor n.rm.unwrapReceivedMessage(blob.data)
  if r.isErr: return
  let content = r.get().message
  var changed = false
  if content.len > 0:
    let e = decode(content)
    if e.msgId notin n.processed and e.msgId notin n.contentCache:
      n.contentCache[e.msgId] = e
    # act on arrival (spec §4.1), unless the "delivered" mode is selected
    if s.p.votes == "received" and e.hasView and n.recordView(e): changed = true
    if s.p.admit == "received" and e.kind == 'M' and n.gateMain(e): changed = true
  # SDS delivery: id signalled ready AND content cached (cascades may signal first)
  for id in n.readyQueue: n.readyPending.incl id
  for id in toSeq(n.readyPending):
    if id in n.processed:
      n.readyPending.excl id
    elif id in n.contentCache:
      let e = n.contentCache[id]
      n.contentCache.del id
      n.readyPending.excl id
      n.processed.incl id
      if e.hasView and n.recordView(e): changed = true
      if e.kind == 'M' and n.gateMain(e): changed = true
  if changed: s.hostLoop(n, now)

proc publishMain(s: Sim, n: Node, now: float) =
  let mid = "M" & $s.mainIds.len
  s.mainIds.add mid
  s.mainRelease[mid] = now
  let fr = n.currentFrontier & @[mid]
  n.mainFrontier[mid] = fr
  let e = Env(kind: 'M', sender: n.i, msgId: mid, hasView: true,
              view: View(base: n.P.len, frontier: fr))
  s.wrapAndSend(n, e, now, s.p.payload)
  n.L.add mid; n.Lset.incl mid
  n.mainHandled.incl mid
  s.hostLoop(n, now)

proc stripRepair(data: seq[byte]): seq[byte] =
  ## --strip: drop the stale repair_request list carried by a repair rebroadcast.
  var m = deserializeMessage(data).get()
  if m.repairRequest.len == 0: return data
  m.repairRequest = @[]
  serializeMessage(m).get()

proc sweep(s: Sim, now: float) =
  ## SDS-R sweep for every online node; the host rebroadcasts repair responses.
  setClock(now)
  for n in s.nodes:
    if s.isOffline(n.i, now): continue
    n.repairOut.setLen(0)
    discard waitFor n.rm.runRepairSweep()
    for data in n.repairOut:
      let id = deserializeMessage(data).get().messageId
      s.broadcast(n.i, Blob(id: id, data: if s.p.strip: stripRepair(data) else: data), now)

proc syncTick(s: Sim, n: Node, now: float) =
  ## Heartbeat: publish a SYNC only if nothing was published since the last tick.
  if not n.publishedSinceSync and not s.isOffline(n.i, now):
    inc n.nonce
    s.wrapAndSend(n, Env(kind: 'Y', sender: n.i, msgId: "Y" & $n.i & "-" & $n.nonce), now)
  n.publishedSinceSync = false

proc regossip(s: Sim, n: Node, now: float) =
  ## Variant B: while not all mains are finalized here, re-send the latest own
  ## view and every admitted, non-finalized main (same bytes, same ids).
  if s.isOffline(n.i, now) or n.P.len >= s.K: return
  if n.L.len > n.P.len or n.gate.len > 0:
    if n.ownViewMsgs.len > 0:
      s.broadcast(n.i, s.blobs[n.ownViewMsgs[^1]], now)
    for x in n.L:
      if x notin n.Pset: s.broadcast(n.i, s.blobs[x], now)

# ---------- setup / run ----------
proc installCallbacks(nn: Node) =
  ## Separate proc so each node's closures get their own environment
  ## (closures created directly in a loop body share one environment in Nim).
  proc onReady(messageId: SdsMessageID, channelId: SdsChannelID) {.gcsafe.} =
    {.cast(gcsafe).}:
      nn.readyQueue.add messageId
  proc onSent(messageId: SdsMessageID, channelId: SdsChannelID) {.gcsafe.} =
    discard
  proc onMissing(messageId: SdsMessageID, missingDeps: seq[HistoryEntry],
                 channelId: SdsChannelID) {.gcsafe.} =
    discard
  proc onRepair(message: seq[byte], channelId: SdsChannelID) {.gcsafe.} =
    {.cast(gcsafe).}:
      nn.repairOut.add message
  waitFor nn.rm.setCallbacks(onReady, onSent, onMissing, onRepairReady = onRepair)

proc pickNodes(r: var Rand, n: int, frac: float, exclude0: bool): seq[bool] =
  ## Deterministic random subset of round(frac * n) nodes.
  result = newSeq[bool](n)
  var ids = toSeq((if exclude0: 1 else: 0) ..< n)
  r.shuffle(ids)
  for i in ids[0 ..< min(ids.len, int(round(frac * float(n))))]: result[i] = true

proc newSim(p: Params): Sim =
  result = Sim(p: p, Q: p.N div 2 + 1, rng: initRand(p.seed * 7919 + 17), tAllDone: -1)
  # Topology randomness uses its own generator so default runs stay reproducible.
  var aux = initRand(p.seed * 31337 + 7)
  result.bad = pickNodes(aux, p.N, p.badFrac, exclude0 = false)
  result.offline = pickNodes(aux, p.N, p.offFrac, exclude0 = true)
  result.pos = newSeq[float](p.N)
  for i in 0 ..< p.N: result.pos[i] = aux.rand(1.0)
  # Simulation-only speedups in nim-sds (results identical, see patches/).
  sdsSimSkipPersistence = getEnv("SIM_FULL_PERSIST") != "1"
  sdsSimDecodeCache = getEnv("SIM_NO_DECODE_CACHE") != "1"
  let cfg = ReliabilityConfig.init(maxCausalHistory = p.C)
  for i in 0 ..< p.N:
    let rm = newReliabilityManager(SdsParticipantID("n" & $i), cfg).get()
    doAssert (waitFor rm.ensureChannel(CH)).isOk
    let n = Node(i: i, rm: rm)
    result.nodes.add n
    installCallbacks(n)

proc chooseSenders(s: Sim): seq[int] =
  ## M1 from n0; others random (or all n0 with --senders=single). Offline nodes never send.
  var r = initRand(s.p.seed * 104729 + 3)
  result = @[0]
  for _ in 1 ..< s.K:
    if s.p.senders == "single": result.add 0; continue
    var x = r.rand(s.N - 1)
    while s.offline[x]: x = r.rand(s.N - 1)
    result.add x

proc report(s: Sim, tStop: float): JsonNode

proc run(s: Sim): JsonNode =
  for i in 0 ..< s.N:
    if s.p.syncInt > 0: s.at(s.rng.rand(s.p.syncInt), evSync, i)
    if s.p.variant == "B": s.at(s.rng.rand(s.p.regossipInt), evRegossip, i)
  s.at(SWEEP, evSweep)
  let senders = s.chooseSenders()
  for j, snd in senders: s.at(float(j) * s.p.interval, evMain, snd)
  let lastMain = float(senders.len - 1) * s.p.interval
  let wall0 = epochTime()
  var tStop = s.p.tEnd
  var nev = 0
  while s.q.len > 0:
    let ev = s.q.pop()
    if ev.t > s.p.tEnd: break
    tStop = ev.t
    inc nev
    if s.p.wall > 0 and nev mod 2000 == 0 and epochTime() - wall0 > s.p.wall:
      s.wallHit = true
      break
    case ev.kind
    of evArrive: s.arrive(s.nodes[ev.node], ev.blob, ev.t)
    of evMain: s.publishMain(s.nodes[ev.node], ev.t)
    of evSync:
      s.syncTick(s.nodes[ev.node], ev.t)
      s.at(ev.t + s.p.syncInt, evSync, ev.node)
    of evRegossip:
      s.regossip(s.nodes[ev.node], ev.t)
      s.at(ev.t + s.p.regossipInt, evRegossip, ev.node)
    of evSweep:
      s.sweep(ev.t)
      s.at(ev.t + SWEEP, evSweep)
    if s.tAllDone < 0 and ev.t >= lastMain and s.nodes.allIt(it.P.len >= s.K):
      s.tAllDone = ev.t
      if not s.p.runToEnd: break
  s.report(tStop)

proc report(s: Sim, tStop: float): JsonNode =
  ## JSON fields:
  ##   safety        no two nodes finalized different mains at the same position
  ##   all_done      every node finalized all K mains
  ##   order         longest finalized sequence, as release indices (1 = first main)
  ##   nodes_done    nodes that finalized all K;  min_fin: fewest positions finalized
  ##   stuck         up to 5 [node, positions finalized] for nodes not done
  ##   stall         why the lowest open position is stuck (dissemination|undecided|split)
  ##   lat           per main: release -> last node finalized (s), null if not all did
  ##   sent, bytes   broadcasts and bytes sent; sent_after_done: after t_all_done
  ##   fin_missing   (node, main) pairs finalized without ever receiving the main's bytes
  ##   wall_timeout  stopped by --wall (partial result)
  var reference: seq[string]
  for n in s.nodes:
    if n.P.len > reference.len: reference = n.P
  var safety = not s.voteInconsistency
  for n in s.nodes:
    if n.P != reference[0 ..< n.P.len]: safety = false
  let allDone = s.nodes.allIt(it.P.len >= s.K)
  var order = newJArray()
  for x in reference: order.add %(parseInt(x[1 .. ^1]) + 1)
  var lat = newJArray()
  for x in s.mainIds:
    if s.nodes.allIt(x in it.finTime):
      lat.add %(s.nodes.mapIt(it.finTime[x]).max - s.mainRelease[x])
    else:
      lat.add newJNull()
  var stuck = newJArray()
  for n in s.nodes:
    if n.P.len < s.K and stuck.len < 5: stuck.add %*[n.i, n.P.len]
  var finMissing, nodesFinMissing = 0
  for n in s.nodes:
    let c = n.P.countIt(it notin n.mainHandled)
    finMissing += c
    if c > 0: inc nodesFinMissing
  var stall = newJNull()
  if not allDone:
    # classify the lowest open position using each member's own latest usable vote
    let k = s.nodes.mapIt(it.P.len).min + 1
    if reference.len >= k - 1:
      let pk = reference[0 ..< k - 1].toHashSet
      var c = initCountTable[string]()
      var voted = 0
      for u in 0 ..< s.N:
        var val = ""
        for v in s.nodes[u].views.getOrDefault(u):
          if v.base <= k - 1:
            for x in v.frontier:
              if x notin pk: val = x; break
          if val.len > 0: break
        if val.len > 0: c.inc(val); inc voted
      let best = if c.len > 0: c.largest[1] else: 0
      stall = if best + (s.N - voted) < s.Q: %"split"
              elif best >= s.Q: %"dissemination"
              else: %"undecided"
  %*{"safety": safety, "all_done": allDone, "order": order,
     "nodes_done": s.nodes.countIt(it.P.len >= s.K), "min_fin": s.nodes.mapIt(it.P.len).min,
     "stuck": stuck, "stall": stall, "lat": lat,
     "sent": s.sent, "bytes": s.bytes, "sent_after_done": s.sentAfterDone,
     "fin_missing": finMissing, "nodes_fin_missing": nodesFinMissing,
     "t_stop": tStop, "t_all_done": (if s.tAllDone >= 0: %s.tAllDone else: newJNull()),
     "wall_timeout": s.wallHit}

proc diagnose(s: Sim, seed: int) =
  ## SIM_DIAG=1: read-only post-mortem of nodes that did not finalize all K mains:
  ## SDS buffer sizes and the root missing messages that block causal delivery.
  let holdersOf = proc(id: string): int =
    for m in s.nodes:
      if id in m.rm.channels[CH].messageHistory: inc result
  for n in s.nodes:
    if n.P.len >= s.K: continue
    let ch = n.rm.channels[CH]
    var roots = initCountTable[string]()   # missing id -> buffered msgs directly waiting on it
    for id, e in ch.incomingBuffer.mpairs:
      for d in e.missingDeps:
        if d notin ch.incomingBuffer and d notin ch.messageHistory: roots.inc d
    stderr.writeLine "seed=", seed, " node=n", n.i, " finalized=", n.P.len, "/", s.K,
      " incomingBuffer=", ch.incomingBuffer.len, " history=", ch.messageHistory.len,
      " outRepair=", ch.outgoingRepairBuffer.len, " rootMissing=", roots.len
    roots.sort()
    var shown = 0
    for id, cnt in roots:
      var pendingResp = 0
      for m in s.nodes:
        if id in m.rm.channels[CH].incomingRepairBuffer: inc pendingResp
      let kind = case id[0]
        of 'M': "MAIN"
        of 'S': "SEEN"
        of 'Y': "SYNC"
        else: "?"
      stderr.writeLine "   root ", id, " (", kind, ")  waitedOnBy=", cnt,
        "  inHistoryOf=", holdersOf(id), "/", s.N,
        "  inMyOutRepair=", id in ch.outgoingRepairBuffer,
        "  pendingRepairResponses=", pendingResp
      inc shown
      if shown >= 8: break

proc usage() =
  echo """finality_sim: SDS-FINALITY on real nim-sds (virtual clock). One JSON line per seed.
 network
  --n=50              nodes (quorum N div 2 + 1)
  --loss=10           packet loss % (per packet x receiver)
  --min-delay=0.05    one-way delay range (s)
  --max-delay=30
  --delay=uniform     uniform | tail (90% in first 10% of range) | geo (correlated by node position)
  --bad-frac=0        fraction of nodes with a bad link ...
  --bad-loss=50       ... whose packets (to or from them) are lost with this %
  --offline-frac=0    fraction of nodes (never n0) offline during [offline-at, offline-at + offline-dur)
  --offline-at=-1     start (s); -1 = interval (right after M1)
  --offline-dur=120
 workload
  --k=5               number of MAIN messages, released every --interval s
  --interval=60
  --senders=random    random (M1 from n0, rest random) | single (all from n0, e.g. one steward)
  --payload=0         MAIN payload bytes
 protocol
  --variant=A         A = spec; B = A + re-gossip of non-finalized mains until finalized
  --regossip=20       re-gossip period for B (s)
  --sync=30           SYNC heartbeat period (s); 0 = no SYNC
  --votes=received    received | delivered (wait for SDS causal delivery before counting a vote)
  --admit=received    received | delivered (wait for SDS causal delivery before admitting a MAIN)
  --causal=0          SDS maxCausalHistory; 0 = max(11, 2N)
  --strip             strip stale repair requests from SDS-R rebroadcasts
 run
  --seed=0 --seeds=1  run seeds [seed, seed + seeds)
  --t-end=0           simulated time limit (s); 0 = (k-1)*interval + 900
  --run-to-end        do not stop when every node has finalized everything
  --wall=0            wall-clock limit per seed (s); stops with a partial result
 env: SIM_DIAG=1 prints a post-mortem of unfinished nodes to stderr"""

when isMainModule:
  var p = Params(N: 50, K: 5, loss: 10, badLoss: 50, interval: 60, minDelay: 0.05,
                 maxDelay: 30, syncInt: 30, regossipInt: 20, offAt: -1, offDur: 120,
                 variant: "A", votes: "received", admit: "received", delayDist: "uniform",
                 senders: "random")
  var seed0 = 0
  var seeds = 1
  for kind, key, val in getopt():
    if kind notin {cmdLongOption, cmdShortOption}: usage(); quit(2)
    case key
    of "n": p.N = parseInt(val)
    of "loss": p.loss = parseFloat(val)
    of "min-delay": p.minDelay = parseFloat(val)
    of "max-delay": p.maxDelay = parseFloat(val)
    of "delay": p.delayDist = val
    of "bad-frac": p.badFrac = parseFloat(val)
    of "bad-loss": p.badLoss = parseFloat(val)
    of "offline-frac": p.offFrac = parseFloat(val)
    of "offline-at": p.offAt = parseFloat(val)
    of "offline-dur": p.offDur = parseFloat(val)
    of "k": p.K = parseInt(val)
    of "interval": p.interval = parseFloat(val)
    of "senders": p.senders = val
    of "payload": p.payload = parseInt(val)
    of "variant": p.variant = val.toUpperAscii
    of "regossip": p.regossipInt = parseFloat(val)
    of "sync": p.syncInt = parseFloat(val)
    of "votes": p.votes = val
    of "admit": p.admit = val
    of "causal": p.C = parseInt(val)
    of "strip": p.strip = true
    of "seed": seed0 = parseInt(val)
    of "seeds": seeds = parseInt(val)
    of "t-end": p.tEnd = parseFloat(val)
    of "run-to-end": p.runToEnd = true
    of "wall": p.wall = parseFloat(val)
    of "h", "help": usage(); quit(0)
    else: usage(); quit(2)
  if p.N < 2 or p.K < 1 or p.loss notin 0.0 .. 99.9 or p.badLoss notin 0.0 .. 100.0 or
     p.maxDelay < p.minDelay or p.variant notin ["A", "B"] or
     p.votes notin ["received", "delivered"] or p.admit notin ["received", "delivered"] or
     p.delayDist notin ["uniform", "tail", "geo"] or p.senders notin ["random", "single"] or
     p.regossipInt <= 0 or p.badFrac notin 0.0 .. 1.0 or p.offFrac notin 0.0 .. 1.0:
    usage(); quit(2)
  p.loss /= 100.0
  p.badLoss /= 100.0
  if p.tEnd <= 0: p.tEnd = float(p.K - 1) * p.interval + 900
  if p.C <= 0: p.C = max(11, 2 * p.N)
  if p.offAt < 0: p.offAt = p.interval
  for seed in seed0 ..< seed0 + seeds:
    p.seed = seed
    let sim = newSim(p)
    let res = sim.run()
    res["seed"] = %seed
    echo $res
    flushFile(stdout)
    if getEnv("SIM_DIAG") == "1": diagnose(sim, seed)
    for n in sim.nodes: waitFor n.rm.cleanup()
