# Closing a worker and qualifying its exit

A worker has several lifetimes: its operating system process, the RPC socket,
its output pipes, and the manager tasks that read messages and output. A dead
process alone does not prove that these other resources have finished. A result
received before shutdown also does not prove that the process exited normally.

## A terminal report

`Malt.shutdown` closes the resources owned by a `Malt.Worker` and returns a
`Malt.WorkerShutdownReport`. Use the report when a controller must qualify its
worker's exit before publishing a result.

```julia
import Malt

worker = Malt.Worker()
answer = Malt.remote_eval_fetch(worker, :(6 * 7))
report = Malt.shutdown(worker; timeout=30.0, exit_timeout=5.0, term_timeout=5.0)

@assert answer == 42
@assert report.complete
@assert report.process_exited
@assert report.exitcode == 0 && report.termsignal == 0
@assert report.socket_closed && report.stdout_closed && report.stderr_closed
@assert report.tasks_done && report.pending_replies == 0
@assert isempty(report.errors)
```

The report records the original PID, the shutdown stage actually reached, the
real exit code and termination signal, connection state, manager-task completion,
pending RPC count and cleanup errors. Code and signal are unavailable until the
process has been reaped. A complete report is a resource-lifetime statement;
it does not turn an abnormal exit, an output error or a failed application
computation into success.

`tasks_done` includes tasks which finished by throwing. Inspect the `failed`
flags in `tasks` when distinguishing successful manager tasks from terminated
ones. Finished asynchronous reply tasks are summarized by `completed_replies`
and `failed_replies`; an application must account for any expected RPC failures
separately from a successful cleanup observation.

For a measurement controller, check the worker report before accepting its
result or writing a successful cache. The controller's **parent** must separately
check the controller process's exit code and signal. A controller cannot certify
that its own future `atexit` hooks will succeed.

## One deadline, with escalation and retry

The total `timeout` is measured with a monotonic clock. It covers waiting for a
concurrent shutdown, sending the graceful exit request, waiting for process exit,
escalating through TERM/KILL when necessary, draining managed output, closing
connections and joining manager tasks. `exit_timeout` and `term_timeout` are phase
allowances inside that total budget; they do not restart it.

An incomplete close of the process, RPC connection or manager tasks raises
`Malt.ShutdownIncompleteException`. Its `.report` retains the state that was
actually observed. The worker's internal state keeps
ownership of unfinished tasks and connections so another call can retry.
Repeated shutdown of an already closed worker is safe.

This is a cooperative deadline. Julia code in an application serializer or an
output sink cannot be forcibly preempted safely. If such a task can still act,
the report must expose it as unfinished. A timeout is not permission to forget
that task or publish a successful cleanup result.

Run shutdown in a `finally` block when the application operation can fail. Keep
the original operation failure and the shutdown failure if both occur; neither
is evidence that the other succeeded.

## Output ownership and the legacy `stop` result

Monitored output is forwarded with Malt's line prefixes. Shutdown waits for the
managed readers to reach EOF, including the final partial line, before claiming
their completion. Forwarded output is not a byte-for-byte raw pipe: prefixes and
line endings are part of its presentation.

With `monitor_stdout=false` or `monitor_stderr=false`, the caller owns reading
that pipe. Read it concurrently when output may fill the operating system's pipe
buffer. `Malt.stop` preserves these borrowed pipes so existing code can read the
final bytes after process exit:

```julia
import Malt

worker = Malt.Worker(; monitor_stdout=false)
Malt.remote_eval_wait(worker, :(begin print("final bytes"); flush(stdout) end))
@assert Malt.stop(worker)
@assert read(worker.stdout, String) == "final bytes"

report = Malt.shutdown(worker; timeout=5.0)
@assert report.complete && report.stdout_closed
@assert report.exitcode == 0 && report.termsignal == 0
```

The legacy Boolean reports whether the serialized shutdown entry observed a
running process. Concurrent `stop` callers can therefore observe one `true`
transition followed by `false` results. It is not a normal-exit verdict.
`stop` still joins the manager's owned
tasks and closes the RPC connection; a borrowed pipe left open is not reported
as a fully closed connection.

The new `shutdown` defaults to `close_unmonitored=true`. It closes borrowed pipes
too; unread bytes can then be discarded. Drain any output you need before that
final close. Passing `close_unmonitored=false` can return a deliberately partial
report without an exception when only the caller-owned pipes remain open. Its
`complete` field stays false. Unfinished managed tasks, RPC connections or the
process still raise an incomplete-close exception.

Reading a pipe through EOF may itself close its endpoint in Julia's standard
library. Inspect the borrow before that read when distinguishing caller reads
from a discard requested by shutdown.

## RPC failure is a transport event

Each pending request has one response cell. Response delivery, worker death and
local abandonment compete for that cell once. Cancelling a manager-side waiter
removes its pending entry; a later response cannot publish a second result or
block another listener. Cancelling the waiter does not undo remote application
effects or guarantee that the remote computation stopped.

Malt tracks possible remote application activity separately from local response
waiters. Receiving the actual response retires that activity. `remote_do` has no
public result, but its internal completion acknowledgement serves the same
purpose. Cancelling a local waiter does not retire its remote activity, and the
startup readiness exchange does not count as application work.

`Malt.interrupt` does nothing while no remote application work is known to be
active. While work may be active it keeps the native interrupt mechanism. This
accounting is conservative: an admitted frame may still be serializing or
waiting to flush before the application starts, and a result can be in transit
after its computation ended. A native signal targets a process thread, not a
message ID. The activity check therefore prevents signals sent to a worker known
to be idle; it cannot eliminate every race between admission, completion and
signal delivery. It does not mask signals globally or change Julia's runtime
handling of SIGINT.

The internal `_send_msg` and `_wait_for_response(worker, id)` pair preserves an
already acquired response even if the process exits before the caller starts
waiting. Its response handle belongs to the public worker until that wait
consumes or abandons it. Background state and the global cleanup registry do not
own that result. Code using these internal helpers must finish or abandon its
wait rather than accumulate unconsumed response handles.

Serialization is streamed. A serializer can fail after writing part of a frame,
so deleting the request's dictionary entry is insufficient. Such an error poisons
the transport: new requests are refused, outstanding requests are resolved and
the worker's tracked shutdown closes the unusable connection. The package does
not advertise that partially written stream as healthy. Start a new worker when
the application needs to continue after this failure.

Construction also owns its partial resources. Its initial `connect_timeout`
budget covers the port announcement, the actual TCP connection and a protocol
round trip with an exact nonce response. A connected socket alone is not a
ready worker. The four manager loops and the readiness task are tracked before
the handle is published. The same budget is checked when the readiness reply is
accepted and immediately before publication; a late acknowledgement does not
extend the startup allowance.

An invalid port, failed connection, missing readiness acknowledgement or startup
timeout triggers rollback of the process, startup tasks, socket and pipes. That
rollback has its own bounded cleanup allowance; a constructor's wall time can
therefore exceed its connection allowance. An incomplete rollback retains
ownership and exposes its state rather than losing the remaining handles.

Errors after spawning the child are wrapped in `Malt.WorkerStartupException`, with the original
`.cause`, captured `.stderr` and rollback `.report`. Update application catches
that assumed every startup failure was an `ErrorException`; the cause and
cleanup observation are separate information.

The handle's finalizer is a nondeterministic safety net. Active calls and
promises keep their worker alive until their result is settled. Finished
asynchronous reply-task observations do not retain the task, result or exception
in Malt's state; their
counts are exposed as `completed_replies` and `failed_replies`. An error or task
that application code deliberately retains can still hold its worker. Explicit
shutdown remains the way to obtain a terminal report at a known boundary.

## Process and platform boundary

These lifecycle guarantees concern `Malt.Worker`. An `InProcessWorker` has no
separate process to reap, and a `DistributedStdlibWorker` delegates its lifecycle
to Distributed. Closing a worker does not qualify all descendants launched by
application code, every native library, or the manager's own shutdown.

Malt requires trusted local RPC peers and trusted serialized code. It does not
change the operating system account or filesystem permissions. Its child launch
inherits environment variables with explicit overrides; select child projects
and credentials deliberately. A separate address space contains many process
failures, but is not an authentication or security sandbox.

## API

`Worker` now holds its managed resources in an internal state object. Existing
property access such as `worker.proc`, `worker.stdout`, `worker.stderr` and
`worker.current_socket` is preserved. Code which inspects the old concrete field
layout or uses `getfield` for those properties must migrate to property access
or the lifecycle API. A shutdown report contains scalar task observations and
counts rather than task or result handles.

The internal completion frame is part of the matching host and worker sources.
Keep `worker.jl` and `shared.jl` from the same package revision. These functional
runs use matching Julia versions in manager and worker; they do not qualify a
mixed-version serialization protocol.

The manual builds locally with `doc/make.jl`. Publishing is an explicit, separate
operation: set `MALT_DEPLOY_DOCS=true` and configure `MALT_DOCS_REPO` for the
repository that owns the generated pages. A documentation build does not deploy
to the upstream project's website by default.

```@docs
Malt.shutdown
Malt.WorkerShutdownReport
Malt.ShutdownIncompleteException
Malt.WorkerStartupException
Malt.WorkerTaskStatus
```

## Maintainer protocol reference

These helpers implement frame admission and response ownership. Their names and
signatures are internal implementation details and can change between releases.

```@docs
Malt._send_msg
Malt._wait_for_response
Malt._send_receive
Malt._send_receive_async
```
