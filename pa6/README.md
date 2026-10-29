# PA6 — Contract-first API and a hand-rolled saga

| | |
|---|---|
| **Session** | S6 — APIs and contracts · 2026-10-21, C211 |
| **Scaffold language** | TypeScript |
| **Published** | with S6 on 2026-10-21 |
| **Deadline** | **2026-11-02, 20:00 Europe/Riga** |
| **Hard cut-off** | **2026-11-09, 20:00** — miss it and the capstone is not graded |
| **Weight** | one seventh of the homework half — about 7.1% of the final grade |
| **Mark** | 70% automated tests, 30% manual (ADR quality, self-assessment honesty) |
| **ADR** | `docs/adr-005.md` |

---

## 1) Objective

Two things, in this order, and the order is the point:

1. **Write the contract before you write the code.** `openapi.yaml` describes
   `POST /checkout` — what it accepts, what it returns, and what its errors
   look like — before a single line of the orchestrator is filled in. A
   containerised linter (Spectral) checks that the contract is real: named
   consistently, documented, and honest about its error shapes and its
   bounds. You cannot skip this by writing vague YAML — the ruleset checks
   for exactly the kind of contract that is easy to fake.
2. **Then implement the saga.** The orchestrator composes four downstream
   services — payment, inventory, shipping, notification — into one business
   transaction: strict sequencing, timeout control, idempotency, and
   compensation (rollback) when something downstream fails partway through.

This is the same orchestration exercise that has run in this course before,
ported from plain JavaScript to TypeScript, with the contract-first stage
added in front of it. If you have seen a "saga pattern" diagram in a lecture,
this is the assignment where you find out what the diagram leaves out.

---

## 2) What is provided

```
pa6/
  openapi.yaml            worked example contract for POST /checkout — READ FIRST, see §3
  .spectral.yaml          the lint ruleset that grades your contract
  examples/
    openapi.bad.yaml      deliberately broken spec — instructor verification only, not a starting point
  mock-services/          payment, inventory, shipping, notification (TypeScript, unchanged behavior)
  orchestrator/           the scaffold you complete — TODOs are in src/server.ts
  tests/public/           tests you can run yourself, as often as you like
  grading/schema/         JSON Schemas the tests validate your responses and stores against
  docker-compose.yml      the five services this assignment needs
  docs/adr-005.md         your architecture decision record (copy and fill in)
```

Hidden tests are also run at grading time. They are never published, they
test the same requirements as the public ones, and they exist so that code
written to pass the visible tests specifically does not score well.

---

## 3) Stage 1 — the contract (`openapi.yaml`)

`openapi.yaml` in this folder is a **worked example**, not the answer key.
It documents one reasonable version of the `POST /checkout` contract —
request shape, all four response codes, error schemas, bounded arrays — so
you have something concrete to read before writing your own. Read it, then
**replace it with your own contract** for the orchestrator you are about to
build. Copying it verbatim and changing nothing will not teach you anything
the lint step is checking for, and a manual reviewer will notice.

Lint your contract locally, exactly as it is graded:

```bash
docker run --rm -v $PWD:/w stoplight/spectral lint /w/pa6/openapi.yaml --ruleset /w/pa6/.spectral.yaml
```

On Windows PowerShell, `$PWD` needs to resolve to a path Docker can bind-mount:

```powershell
docker run --rm -v "${PWD}:/w" stoplight/spectral lint /w/pa6/openapi.yaml --ruleset /w/pa6/.spectral.yaml
```

`.spectral.yaml` extends the standard OpenAPI ruleset and adds four things
this assignment specifically cares about:

1. **Naming conventions** — `operationId` and JSON property names are
   camelCase, path segments are kebab-case.
2. **Required descriptions** — `info`, every operation, every parameter,
   every request body and every response must say what it does. A contract
   nobody can read from is not a contract.
3. **Error-response schemas present** — every 4xx/5xx response must
   reference a JSON schema, not just a prose description. If you cannot say
   what shape an error takes, callers cannot handle it.
4. **No unbounded arrays** — every array schema declares `maxItems`. An
   array with no upper bound is a pagination or DoS problem waiting to
   happen; deciding the bound is part of designing the contract.

`examples/openapi.bad.yaml` is not something to fix or learn from stylistically
— it exists so you (and I) can prove the ruleset actually rejects a spec that
breaks all four rules. Lint it and see it fail:

```bash
docker run --rm -v $PWD:/w stoplight/spectral lint /w/pa6/examples/openapi.bad.yaml --ruleset /w/pa6/.spectral.yaml
```

If that command exits `0`, something is wrong with your ruleset changes, not
with the example.

**Only after your contract lints clean** should you move on to §4 and start
filling in `orchestrator/src/server.ts`. The manual review checks whether the
implementation actually matches what you wrote in `openapi.yaml` — a
contract that describes one thing while the code does another is worse than
no contract.

---

## 4) Stage 2 — the saga (`orchestrator/src/server.ts`)

### Prerequisites

- Docker Desktop with Compose support
- Node.js 20+ (only needed if you want to run a service outside Docker)
- Available ports: `3000`, `4001`, `4002`, `4003`, `4004`

### Run it

```bash
cd pa6
docker compose up -d --build --wait
npm test
```

Health checks, if you want to see the stack up by hand:

```bash
curl http://localhost:3000/health
curl http://localhost:4001/health
curl http://localhost:4002/health
curl http://localhost:4003/health
curl http://localhost:4004/health
```

Stop it:

```bash
docker compose down       # keep the persisted stores
docker compose down -v    # also wipe /data volumes, for a clean restart
```

### Required orchestrator endpoints

1. `GET /health` → `200 { "status": "ok" }`
2. `POST /checkout` (graded)
3. Optional `GET /debug/trace/:orderId` — see §6

### Required header

`Idempotency-Key` is mandatory on `POST /checkout`.

### The saga, in order

```
1) payment authorize
2) inventory reserve
3) shipping create
4) notification send
```

Each step must run only after the previous one has succeeded — no
parallelism, and the tests check for it by timestamp, not just by response
content. If a step fails or times out, compensate the steps that already
completed, **in reverse order**. Only payment (`/payment/refund`) and
inventory (`/inventory/release`) have a compensating action on the mock
services — shipping and notification do not expose one, so a shipping or
notification failure still rolls back inventory and payment, in that order.

### Required response contract (core fields)

```json
{
  "orderId": "ord-123",
  "status": "completed",
  "trace": [
    {
      "step": "payment",
      "status": "success",
      "startedAt": "2026-03-01T10:00:00.000Z",
      "finishedAt": "2026-03-01T10:00:00.120Z",
      "durationMs": 120
    }
  ]
}
```

`trace` must preserve real execution order and include every step that ran,
compensation steps included.

### Deterministic behavior rules

Mandatory HTTP mapping:

- `completed` → `200`
- business failure / compensation failure → `422`
- timeout-triggered flow → `504`

Mandatory machine codes where applicable:

- `timeout`
- `idempotency_conflict`
- `compensation_failed`
- `idempotency_payload_mismatch`

### Idempotency rules

1. Same key + same payload: replay the prior result exactly (same HTTP
   status, same body).
2. Same key + different payload: `409` + `idempotency_payload_mismatch`.
3. Same key while the first request is still mid-flight: `409` +
   `idempotency_conflict`.
4. Records must survive an orchestrator container restart.

Persistence files (inside the container, and bind-mounted to
`orchestrator/data/` on your host so you can inspect them):

- `/data/idempotency-store.json`
- `/data/saga-store.json`

### Downstream URLs and environment

The orchestrator must use environment variables, never hardcoded URLs:

- `ORCHESTRATOR_PORT`
- `PAYMENT_URL`, `INVENTORY_URL`, `SHIPPING_URL`, `NOTIFICATION_URL`
- `REQUEST_TIMEOUT_MS`

`docker-compose.yml` already wires these up. Hardcoding downstream URLs is
penalized even if the tests happen to pass anyway.

---

## 5) What is tested

Public tests (`tests/public/`, run them yourself via `npm test`) check:

1. Services start and health checks pass
2. Happy path sequence and `completed` result
3. Payment failure short-circuits downstream calls
4. Inventory failure triggers refund compensation
5. Shipping timeout triggers compensation
6. Compensation failure maps to `422` + `compensation_failed`
7. Idempotency replay (same key + same payload)
8. Idempotency mismatch (`409`)
9. Strict trace fields, persisted-store schema validation
10. Bonus (advisory, cannot reduce your score): a small repeated-run stress check

**Hidden tests** also run at grading time — the same requirements, different
cases, plus explicit anti-gaming checks: strict step ordering verified by
timestamp (not just by array position), metamorphic payload variations that
must not change the outcome, and a randomized-input pass that a
fingerprinted (hardcoded per-test) implementation would fail. If your
implementation is correct rather than tuned to the public fixtures, you will
not notice these exist.

---

## 6) Optional debug UI

An ungraded debug shell is available at `http://localhost:3000/debug.html`
once the stack is up. It reads `GET /debug/trace/:orderId`. You may extend it
for your own diagnostics; nothing about it is graded.

---

## 7) Submission checklist

- [ ] `openapi.yaml` replaced with your own contract for `POST /checkout`
- [ ] `docker run … stoplight/spectral lint …` exits `0` on your contract
- [ ] `POST /checkout` fully implemented with strict sequencing
- [ ] Compensation implemented, in reverse order, for every failure path that has one
- [ ] Timeout handling implemented (`REQUEST_TIMEOUT_MS`, mapped to `504` + `timeout`)
- [ ] Idempotency contract fully implemented (replay, mismatch, in-flight conflict)
- [ ] File persistence survives a container restart
- [ ] Trace order and schema are correct
- [ ] `npx tsc --noEmit` is clean
- [ ] `docs/adr-005.md` completed
- [ ] AI-usage note added (if applicable) with what you changed/understood

---

## Where your work goes

In **your** repository (`eai-2026-<surname>`), not this one:

```
eai-2026-<surname>/
  pa6/
    openapi.yaml           your contract
    orchestrator/          your implementation
    docs/adr-005.md        required — a missing ADR fails a public test
```

See [how an assignment works](../README.md#how-an-assignment-works) in the
root README for the submission procedure, the late penalty and the
progression gate. Read [`../CONTRIBUTING.md`](../CONTRIBUTING.md) before
asking a question — it will usually be faster.

---

## Common ways to lose marks

| | |
|---|---|
| Copying `openapi.yaml` verbatim | It is a worked example, not the answer key — the manual review checks it against your actual implementation |
| An unbounded array "because the mock never returns many items" | The rule is about the contract, not today's test data. Pick a real bound and justify it if asked |
| Calling downstream services in parallel to "save time" | The hidden tests detect this by timestamp, not by trace order alone |
| Compensating shipping or notification | Neither mock exposes a compensating endpoint. Compensate inventory and payment only, in reverse order |
| Hardcoding `http://payment:4001` etc. | Breaks outside Docker Compose, and is penalized even when it happens to pass |
| Treating `422` and `504` as interchangeable | `504` is reserved for the timeout-triggered path specifically |
| An ADR that restates this README | 30% of the mark, and I have read this README |
