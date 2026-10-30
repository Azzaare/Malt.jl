# Lifecycle verification and limits

This SDK release builds on upstream Malt 1.5.0, commit
`7e102c84d2428dce4f682d7fb48b452a8d96350b`. It adds an explicit lifecycle
contract for `Malt.Worker`; the other worker adapters retain their own lifetime
rules. The [lifecycle guide](lifecycle.md) describes the public contract and
migration from `stop`.

## What the tests exercise

The functional suite retains the upstream RPC, nesting, exceptions, output and
interrupt tests. Its additional lifecycle tests exercise real child processes,
sockets and response cells, including:

- complete, repeated and concurrent shutdown, bounded escalation and retry;
- monitored output through EOF, final partial lines and caller-owned raw pipes;
- active senders, an incomplete output task and cleanup which cannot yet finish;
- response delivery competing with worker death or local waiter abandonment;
- serializers which write part of a frame before failing, in either direction;
- constructor failures after spawn, invalid connections, readiness and rollback;
- garbage collection of an abandoned handle and retention by an active promise;
- safe display which does not expand inherited environment variables or results;
- known-idle interrupts, activity retained after local cancellation, and internal
  completion acknowledgements for successful, failed or rejected `remote_do`;
- a low-level response received before child exit and consumed afterwards;
- an actual received readiness reply accepted after its budget, and an actual
  finished readiness task finalized after the same budget, both refused with
  rollback.

The last two deadline cases use the production acceptance and finalization
phases. They do not replace the clock, extend the startup allowance or add a
readiness delay to the runtime. The known-idle interrupt can also be observed
externally on Linux by tracing an owned controller's signals to its worker; this
optional observation is separate from the functional suite.

Readiness adds a protocol round trip to construction, and `remote_do` now emits
an internal completion frame. Shutdown also performs ownership bookkeeping and
joins its managed tasks. These changes have real startup, protocol and cleanup
costs; the functional checks do not measure them or claim a performance gain.

The performance campaign in `test/benchmark.jl` is excluded by default.
`MALT_RUN_BENCHMARKS=true` opts into it explicitly. Functional interrupt tests
still use CPU and allocation workloads to verify cancellation. Their elapsed
times are test observations, not comparative performance qualifications.

## Defects distinguished during qualification

The upstream 1.5.0 control reproduced an orphaned expected-reply entry after a
host serializer wrote six bytes and threw. Nineteen assertions established the
failure and bounded cleanup. No subsequent RPC was attempted on that malformed
stream, and this control does not establish the cause of an earlier crash.

The SDK qualification also found and corrected a constructor which treated an
asynchronous connection attempt as a completed connection; response ownership
which could lose an already received low-level result after process exit; and
startup checks which could accept a completed acknowledgement after its deadline.
Separate garbage-collection tests cover both premature loss of an active owner
and retention through completed tasks or exceptions.

An earlier Julia 1.6.7 run failed with `InterruptException` in the runtime's
`task_done_hook`. This was an interrupt-related worker failure, not a SIGSEGV.
Readiness proves that the serving loop answered, while remote-activity tracking
prevents native interruption of a worker known to be idle. Neither changes
Julia's signal runtime or proves that every asynchronous signal race is gone.

In particular, admission can precede completion of serialization and a response
can arrive after execution ended. The activity set deliberately represents
possible remote work, not an exact execution interval. Local cancellation
cannot promise that remote effects stopped. See the guide before using
interruption as an application cancellation policy.

## Source and functional qualification

Runtime and tests are committed as
`e90cdf72ad021157ffdc4f4e28c09fd39a82d355`. The following SHA-256 values were
checked before and after the functional runs:

| File | SHA-256 |
|---|---|
| `src/Malt.jl` | `38550a770378606fe30eb891d7c7f6c2b4991eaee1077959d2de55db0e4651f3` |
| `src/lifecycle.jl` | `ee6f6021be4c81ecb347a5ab9dc83066cc5c4193de7f9ba00c425fe442cf91b5` |
| `src/shared.jl` | `8676859cee3b257d6d47c3c44917258df94a3c16061616a573106dd875c1332d` |
| `src/worker.jl` | `cc130f02e53eec97de93cb602ef51d477021fcf05b9fe5fef4757724a6228eb9` |
| `test/lifecycle_contracts.jl` | `a8e0227fa11082f42a7fe144b869fe2adb9819eeb1447e10d9d09909ea20125c` |
| `Project.toml` | `04908188b794ec8f474d86240f6d8ca944e3e7b41dac780f44b27877d4930624` |

Full functional runs on 29 October 2026 use four manager threads, one BLAS
thread, explicit child projects and `--color=yes`. The lifecycle fixtures use
one thread in each child; newer runtimes use one GC thread, while Julia 1.6
uses its serial collector. Both performance opt-in and external tracing are
disabled in these complete runs.

| Julia | PASS / failed / errors / broken | UTC start–finish | UTC interval | Peak RSS |
|---|---|---|---|---|
| 1.6.7 | 1151 / 0 / 0 / 1 | 22:21:51–22:24:11 | 139.93 s | 397264 KiB |
| 1.12.7 | 1185 / 0 / 0 / 1 | 22:19:14–22:22:06 | 171.87 s | 564440 KiB |
| 1.13.1 | 1308 / 0 / 0 / 1 | 22:28:29–22:31:08 | 158.90 s | 521548 KiB |

These are functional-test intervals and reported process peaks, not comparative
benchmarks or an aggregate memory envelope for all descendants. Every outer
controller exits normally with code zero. Observed descendant PID inventories
are checked absent after termination; passing assertions alone do not establish
that exit boundary.

The lifecycle-contract subtotal is 820 in all three runs. The inherited tests
account for 331, 365 and 488 respectively. The 34 additional counted assertions
after Julia 1.6 are 28 CPU-interrupt assertions guarded by `VERSION > 1.10` and
six error-message assertions guarded by `VERSION >= 1.8`. The additional 123
counted passes in Julia 1.13 come from the asynchronous channel task in
`test/basic.jl`: 41 assertions for each of three backends. Older Test versions
execute these assertions under a fallback test set; Julia 1.13 inherits the
current test set through a scoped value and includes them in its parent's
count. The task is awaited in every version, so a failure would still propagate.

The inherited broken assertion is the `DistributedStdlibWorker` channel's
`AbstractChannel` expectation; no lifecycle test is newly skipped or filtered.

The two environments are identified by their complete Project and Manifest
hashes. The Julia 1.6 environment is also used by the 1.12 run:

| Environment | Project SHA-256 | Manifest SHA-256 |
|---|---|---|
| Julia 1.6-compatible | `22501a23b602429fd8e9851c171218e4db377e723a8d62d6be1cdadebce03bf3` | `d851f6b1ee323276e6c43fa21d13fb07c95eadbcaae0b3ff600b386b5d9d9b81` |
| Julia 1.13 tooling | `80b69580ee537946c3c2c3848efa491e5cafc665dde15ac5d9a1d13138e7925c` | `f02d4cb0149df6c24e06954ace9e8f7cd4579eeae33b88b73865de522e0a43eb` |

The separate Linux trace qualification passes 217 assertions on seven new
groups. It observes no SIGINT syscall targeting the three workers used only
for idle phases over their complete observed lifetimes. Its positive control
observes a real SIGINT after local waiter cancellation, the remote callback's
interruption and a subsequent exact RPC response. The trace is not a proof
that all admission or completion races are impossible.

## Scope of the evidence

These checks concern local processes on the tested Linux x86-64 host. They do
not qualify Windows console signals, macOS, ARM, every native extension, all
Julia releases admitted by the compatibility range or children launched by
application code. The Julia 1.6 checks use Scratch 1.2.1 because Scratch 1.3.0
requires a newer Julia; the newer-runtime environment uses Scratch 1.3.0.

Aqua checks package hygiene. The fresh JET checks target `shutdown`, `stop`,
`interrupt` and low-level response waiting; their zero diagnostics do not mean
that a whole-package JET analysis was performed. The manual is built strictly
with Documenter, and its two Julia examples are executed separately from the
rendering step.

No acquired Render or Fleet performance campaign is replayed for this work.
Their historical controller SIGSEGV incidents remain unattributed. A fully
qualified worker shutdown closes a real ownership gap, but does not certify the
manager's own future exit hooks. Integrators must also check the outer
controller's actual exit code and signal, and publish successful results only
after the relevant shutdown boundary has been qualified.
