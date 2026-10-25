# Orchestra

**A durable multi-agent runtime.** Orchestra runs multi-agent workflows the way a bank runs transactions: every step is written to an append-only log, nothing is lost when a worker dies, any run can be replayed, a run can pause for a human for days, and a runaway agent is stopped before it burns your budget.

> **Status: under construction.** This README describes the target design and build plan. Check the [roadmap](#roadmap) for what is implemented. Commands and endpoints below are the intended interface; they become real as each phase lands.

---

## Why Orchestra

A multi-agent run is a long-lived, non-deterministic, expensive distributed transaction. Four things you get for free in a normal web request are gone:

| Problem | Example | How Orchestra handles it |
|---|---|---|
| **Outlives the request** | A design review takes 11 minutes and 40 LLM calls; the browser tab closes. | Runs execute in background workers. Clients poll or stream events. |
| **Fails halfway** | A worker is OOM-killed at step 23 of 30. | Steps are leased and acknowledged. Unacknowledged steps are reclaimed by another worker and resume from the log. |
| **Not reproducible** | Same input, different output; a bad run can't be investigated. | Every model and tool response is stored. Replay reads them back instead of calling out. |
| **Runs away** | Two agents pass a task back and forth 400 times. | A governor checks token/cost/step budgets, repeated state hashes, and ping-pong patterns. |

The answer to all four is the same idea:

> **The log is the truth. Everything else is a view of the log.**

A run is not a function call. It is a list of events. Current state, progress, and cost are all computed from that list.

## Features

- **Crash recovery**: kill any worker at any time; the run finishes with exactly one of each side effect.
- **Human-in-the-loop**: a node can raise an interrupt; the run parks in Postgres (no process running) until `POST /resume`.
- **Deterministic replay**: re-run any completed run from its log at zero model cost.
- **Fork**: change one node's prompt, replay everything else from tape, get a before/after diff for the price of one LLM call.
- **Parallel branches**: fan-out to specialist agents and merge results with declared reducers in a deterministic order.
- **Budget governor**: hard limits on tokens, USD, steps, wall time, and node visits, plus loop and ping-pong detection.
- **Inspector UI**: live event timeline, click any event to see state at that moment, fork from there.

## Architecture

```
  client                        CONTROL PLANE
  POST /runs {graph_id, input}  -->  insert runs row + event #1 run.started
                                      enqueue first step --> Redis Stream "steps"
                                                                    |
          +---------------------------------------------------------+
          v   (consumer group "workers": each step goes to one worker)
   +-----------+   +-----------+   +-----------+
   | worker 1  |   | worker 2  |   | worker N  |
   +-----+-----+   +-----------+   +-----------+
         | 1. claim step (XREADGROUP), start heartbeat
         | 2. load state = latest checkpoint + events after it
         | 3. governor.check()  -> may abort
         | 4. run the node (model call via gateway, tool call via sandbox)
         | 5. ONE DB transaction: append events + enqueue next steps
         | 6. XACK  (only after the commit succeeds)
         v
   +------------------------- Postgres --------------------------+
   | runs | events (append-only, seq per run) | checkpoints       |
   +-------+---------------------+-----------------+-------------+
           |                     |                 |
       projector             SSE stream        OTel spans
    (state snapshots)      (live inspector)   (Langfuse traces)

   reaper: every few seconds XAUTOCLAIM steps idle > 45 s (their worker died)
           and hand them to a live worker.

   human-in-the-loop:
     node returns interrupt -> interrupt.raised, run = awaiting_approval, lease released
     POST /runs/{id}/resume  -> interrupt.resolved, step re-enqueued
```

| Component | Role |
|---|---|
| **API** (FastAPI) | Create runs, read events/state, resume, fork, stream. Never runs nodes itself. |
| **Workers** | Stateless loops that execute one node per step. Kill one at any time; nothing is lost. |
| **Reaper** | Notices dead workers and recovers their steps. |
| **Postgres** | Single source of truth. Atomic appends make everything safe. |
| **Redis Streams** | The "who does the next step" queue. Can be rebuilt from Postgres. |

## Core concepts

| Term | Meaning |
|---|---|
| **Graph** | Nodes (do work) connected by edges (decide what is next). |
| **Node** | A function `(state, ctx, io) -> patch`. One agent or one step. |
| **State** | A dict of everything known so far in a run. Never mutated directly. |
| **Event** | One immutable row: "this happened". |
| **Fold** | Rebuild state by replaying events: `state = fold(events)`. |
| **Step** | One unit of work for a worker: "run node X for run R". |
| **Lease** | A worker's temporary claim on a step, kept alive with heartbeats. |
| **Idempotency key** | A label proving "this side effect was already done". |

### The five mechanisms

1. **Fold**: nodes return patches, patches become `state.patched` events, state is derived by folding events. Checkpoints every N events keep this cheap.
2. **Lease + heartbeat**: a worker promises "I'm alive" every 10 s. If it goes silent for 45 s, the reaper reassigns the step.
3. **At-least-once + idempotency**: ack happens *after* commit, so a step may run twice. Every side effect carries an `idem_key` (run + node + call index, **never** the attempt number), so the second run reuses the first result.
4. **Determinism**: anything unpredictable (model replies, tool output, time, random, UUIDs) is recorded as an event before use. Nodes use `ctx.now()`, `ctx.uuid()`, `ctx.random()` and `io.model()` / `io.tool()`, never the real ones.
5. **Governor**: after every step, compare spend to budget, hash the state to catch no-progress loops (same node + same state hash 3 times), and detect A-B-A-B ping-pong.

## Quickstart

### Prerequisites

- Python 3.12+
- Docker (for Postgres and Redis)

### Setup

```bash
git clone https://github.com/<you>/orchestra.git
cd orchestra

docker compose up -d                       # postgres:16 + redis:7
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

orchestra migrate                          # create tables from schema.sql
```

### Run the services

```bash
orchestra api                              # http://localhost:8000
orchestra worker                           # run 3+ of these in separate terminals
orchestra reaper                           # run 1
```

### Start a run

```bash
curl -X POST localhost:8000/runs \
  -H 'content-type: application/json' \
  -d '{
        "graph_id": "design_consultant",
        "input": {"q": "Design a URL shortener for 50M DAU"},
        "budget": {"tokens": 120000, "usd": 2, "steps": 40, "wall_seconds": 900}
      }'
# -> {"run_id": "..."}

curl -N localhost:8000/runs/<run_id>/stream     # watch events live
```

## Writing a graph

Each agent is a node. Orchestra handles retries, crashes, pausing, and budgets around it.

```python
# examples/design_consultant/graph.py
from orchestra.graph import Graph
from orchestra.registry import register
from orchestra.types import Fanout, Interrupt

def requirements(state, ctx, io):
    out = io.model("requirements", prompt=f"List requirements for: {state['input']['q']}")
    return {"requirements": out}

def clarifier(state, ctx, io):
    if "ratio" not in state:                                   # first time: ask a human
        return Interrupt("Read:write ratio?", ["100:1", "10:1", "unknown"])
    return {}                                                  # second time: continue

def supervisor(state, ctx, io):
    return Fanout(["db_expert", "cache_expert", "api_expert", "cdn_expert"], then="review")

def db_expert(state, ctx, io):
    out = io.model("db_expert", prompt=f"Pick a database for: {state['requirements']}")
    return {"findings": [out], "confidence": 0.71}

g = (Graph("design_consultant", "v1")
     .node("requirements", requirements)
     .node("clarifier", clarifier)
     .node("supervisor", supervisor)
     .node("db_expert", db_expert)
     .edge("requirements", "clarifier")
     .edge("clarifier", "supervisor")
     .start("requirements"))
register(g)
```

### Rules for node code

- Every model call goes through `io.model(...)`; every tool call through `io.tool(...)`. Never call an LLM SDK directly, or recovery and replay break.
- Never call `time.time()`, `datetime.now()`, `uuid4()`, or `random.*` directly. Use `ctx`.
- Return a patch (dict); never mutate `state` in place.
- Workers import your graphs, so agent code must be installed wherever workers run.

### State merging with reducers

When branches run in parallel, every state key declares how values merge:

```python
STATE_SCHEMA = {
    "findings":   append_list,   # concatenate
    "cost_usd":   sum_,
    "confidence": min_,          # weakest link
    "open_qs":    set_union,
    "design":     last_write,    # only the supervisor writes this
}
```

Joins merge results sorted by node name, never by arrival time, so replay stays deterministic.

## HTTP API

| Method + path | Description |
|---|---|
| `POST /runs` | Create a run `{graph_id, input, budget?}`; returns `{run_id}`. |
| `GET /runs/{id}` | Status, spent vs budget, current node. |
| `GET /runs/{id}/events?after=N` | Events with `seq > N`, ordered. |
| `GET /runs/{id}/state?seq=N` | State as of event N (latest if omitted). |
| `GET /runs/{id}/stream` | Server-sent events feed, live. |
| `POST /runs/{id}/resume` | Body `{decision}`. Resolves an interrupt and re-enqueues. |
| `POST /runs/{id}/fork` | Body `{from_seq, override}`. New run with a parent link; clean parts replay from tape. |
| `POST /runs/{id}/cancel` | Writes `run.aborted`; workers skip further steps for it. |
| `GET /runs/{a}/diff/{b}` | State and cost differences between two runs. |

### Fork example

```bash
curl -X POST localhost:8000/runs/<run_id>/fork \
  -H 'content-type: application/json' \
  -d '{"from_seq": 10, "override": {"db_expert": {"prompt_v": "v3"}}}'
```

```
replay seqs 1-10 from tape          cost $0.00, ~340 ms
re-execute db_expert with v3        cost: 1 model call
later events                        tape reused where inputs unchanged

diff:  db_expert   Cassandra -> DynamoDB + shard-local counters
       confidence  .71 -> .79      review  3 risks -> 2 risks
```

## Data model

```sql
runs        (id, graph_id, graph_version, status, input, budget, spent,
             parent_run, forked_at_seq, created_at, ended_at)
events      (run_id, seq, type, node, payload, idem_key, ts)   -- PK (run_id, seq)
checkpoints (run_id, seq, state)                               -- state as of seq
```

Events are never updated or deleted. A unique index on `(run_id, idem_key)` is the exactly-once guard.

### Event types

```
run.started          node.started        node.finished
model.requested      model.responded     model.cached
tool.requested       tool.responded      tool.failed
state.patched        edge.taken          branch.forked
branch.joined        interrupt.raised    interrupt.resolved
budget.warned        budget.exceeded     step.retried
run.finished         run.failed          run.aborted
```

## Repository layout

```
orchestra/
  docker-compose.yml
  schema.sql
  pyproject.toml
  src/orchestra/
    graph.py          # Graph, END
    registry.py       # graph_id@version -> Graph
    types.py          # Fanout, Interrupt
    ctx.py            # recorded now()/uuid()/random()
    store.py          # append_events, load_events, checkpoints
    fold.py           # fold(events) -> state
    reducers.py       # merge functions
    engine.py         # execute_node, load_state
    worker.py         # worker loop + heartbeat
    reaper.py         # XAUTOCLAIM loop
    replay.py         # ReplayTape, replay(), fork(), diff()
    governor.py       # budgets + loop detection
    api.py            # FastAPI app + SSE
    cli.py            # orchestra api | worker | reaper | migrate
  examples/
    design_consultant/
  tests/
    test_fold.py  test_chaos.py  test_replay.py  test_governor.py
  ui/                 # Next.js inspector
```

## Testing

These are systems tests, not model-quality tests.

| Property | Test | Pass condition |
|---|---|---|
| Fold correctness | `fold(events) == live state` after every run | identical |
| Crash recovery | `SIGKILL` a worker at each of 30 step boundaries | all 30 runs complete |
| Exactly-once effects | force duplicate delivery of every step | one `model.responded` per idem key |
| Replay fidelity | replay 100 finished runs | final states byte-identical |
| Interrupt durability | park a run, restart every process, resume | run completes |
| Budget enforcement | deliberately looping graph | aborts within its step budget, every time |
| Merge determinism | 50 fan-outs with random branch latency | identical joined state |
| Throughput | 1 to 8 workers | report steps/s and the Postgres bottleneck |

```bash
pytest                       # unit tests
pytest tests/test_chaos.py   # kills workers at random; needs docker compose up
```

## Roadmap

- [ ] **Phase 0**: synchronous in-memory graph executor
- [ ] **Phase 1**: event log, fold, checkpoints, `/events` and `/state` endpoints
- [ ] **Phase 2**: Redis workers, leases, reaper, idempotency, chaos test in CI
- [ ] **Phase 3**: replay tape, fork API, diff view
- [ ] **Phase 4**: parallel branches with reducers, interrupts, budget governor
- [ ] **Phase 5**: inspector UI (live timeline, state at any event, fork button)
- [ ] **Phase 6**: port the same workflow to LangGraph and Temporal, publish the comparison

Each phase has a "done when" checklist; do not start the next phase until it is ticked.

## Known failure modes

| Symptom | Cause | Fix |
|---|---|---|
| Replay gives different state | direct clock/random/uuid, set iteration | use `ctx`, sort sets, lint for direct calls |
| Checkpoints grow to MBs | state holds large artefacts | store blobs externally, keep references |
| Slow model call runs twice | call outlives the lease | heartbeat during the call; lease > slowest node p99 |
| Joins conflict | all branches write the join | counter in DB; only the branch reaching 0 enqueues the join |
| Resumed run crashes: node not found | graph redeployed mid-run | pin `graph_version`, keep old versions loadable |
| Duplicate spend after retry | attempt number in idem key | key from run + node + call index only |

## Using it in a multi-agent system

Your app talks only to the HTTP API. Workers run your agent nodes. Model calls go through `io.model` (point it at your LLM gateway), tool calls through `io.tool` (a sandbox or Docker).

```
Your app  --HTTP-->  Orchestra API --> Postgres + Redis
                                           |
                                      Workers run agent nodes
                                        |            |
                                  LLM gateway     Sandbox
```

## References

Read these after building your own version:

- [LangGraph](https://github.com/langchain-ai/langgraph): checkpointer and Postgres saver; compare its reducers with `STATE_SCHEMA`.
- [Temporal Python SDK](https://github.com/temporalio/sdk-python) and [determinism rules](https://docs.temporal.io/workflows): the canonical treatment of deterministic replay.
- [Redis Streams](https://redis.io/docs/latest/develop/data-types/streams/): consumer groups, pending entries, `XAUTOCLAIM`.
- [Event Sourcing (Martin Fowler)](https://martinfowler.com/eaaDev/EventSourcing.html): the pattern this project rests on.
- [OpenAI Agents SDK](https://github.com/openai/openai-agents-python): the handoff primitive.

## License

MIT (add a `LICENSE` file before publishing).
