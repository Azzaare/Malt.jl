"""The observed terminal state of one task owned by a `Worker`."""
struct WorkerTaskStatus
    name::Symbol
    done::Bool
    failed::Bool
end

"""
An immutable observation returned by [`shutdown`](@ref). `complete` concerns
resource ownership, not whether the child exited successfully: inspect both
`exitcode` and `termsignal` before accepting a computation's result.
"""
struct WorkerShutdownReport
    pid::Int32
    stage::Symbol
    process_exited::Bool
    exitcode::Union{Nothing,Int64}
    termsignal::Union{Nothing,Int32}
    socket_closed::Bool
    stdout_closed::Bool
    stderr_closed::Bool
    tasks_done::Bool
    pending_replies::Int
    errors::Tuple{Vararg{String}}
    complete::Bool
    tasks::Tuple{Vararg{WorkerTaskStatus}}
    active_senders::Int
    unmonitored_discarded::Tuple{Bool,Bool}
    completed_replies::Int
    failed_replies::Int
end

"""A shutdown deadline expired; `.report` records the resources still owned."""
struct ShutdownIncompleteException <: Exception
    report::WorkerShutdownReport
end

function Base.showerror(io::IO, error::ShutdownIncompleteException)
    report = error.report
    print(io, "Worker shutdown incomplete for PID ", report.pid,
        " (stage=", report.stage, ", process_exited=", report.process_exited,
        ", tasks_done=", report.tasks_done, ", pending_replies=",
        report.pending_replies, ")")
end

"""Worker construction failed. Its rollback observation is available as `.report`."""
struct WorkerStartupException <: Exception
    cause::Any
    report::WorkerShutdownReport
    stderr::String
end

function Base.showerror(io::IO, error::WorkerStartupException)
    print(io, "Worker process exited before we could connect: ")
    showerror(io, error.cause)
    isempty(error.stderr) || print(io, "\nStderr:\n", error.stderr)
    error.report.complete ||
        print(io, "; rollback is incomplete for PID ", error.report.pid)
end

# Background tasks retain this state, not the public handle. A caller retaining
# an unmonitored Pipe can still read it after stop; only full shutdown discards it.
mutable struct _ReplyTaskState
    task::WeakRef
    failed::Bool
end

mutable struct WorkerState
    port::UInt16
    proc::Base.Process
    proc_pid::Int32
    current_socket::Sockets.TCPSocket
    current_message_id::MsgID
    expected_replies::Dict{MsgID,Channel{WorkerResult}}
    remote_calls::Set{MsgID}
    stdout::Pipe
    stderr::Pipe
    lock::ReentrantLock
    shutdown_gate::ReentrantLock
    admission_open::Bool
    monitor_stdout::Bool
    monitor_stderr::Bool
    tasks::Vector{Pair{Symbol,Task}}
    reply_tasks::Vector{_ReplyTaskState}
    completed_replies::Int
    failed_replies::Int
    active_senders::Dict{Task,Int}
    cleanup_task::Union{Nothing,Task}
    cleanup_full::Bool
    handle::WeakRef
    stage::Symbol
    stage_started::UInt64
    errors::Vector{String}
    unmonitored_discarded::Tuple{Bool,Bool}
end

function WorkerState(proc, pid, socket, output, errors, monitor_stdout, monitor_stderr)
    WorkerState(UInt16(0), proc, pid, socket, MsgID(0),
        Dict{MsgID,Channel{WorkerResult}}(), Set{MsgID}(), output, errors,
        ReentrantLock(), ReentrantLock(), true, monitor_stdout, monitor_stderr,
        Pair{Symbol,Task}[], _ReplyTaskState[], 0, 0,
        Dict{Task,Int}(), nothing, false, WeakRef(nothing),
        :running, time_ns(), String[], (false, false))
end

function Base.summary(io::IO, state::WorkerState)
    port, pid = lock(state.lock) do
        state.port, state.proc_pid
    end
    print(io, "Malt.WorkerState on port ", port, " with PID ", pid)
end
Base.show(io::IO, state::WorkerState) = summary(io, state)
Base.show(io::IO, ::MIME"text/plain", state::WorkerState) = summary(io, state)

struct _WorkerDescription <: AbstractWorker
    port::UInt16
    pid::Int32
end
Base.summary(io::IO, worker::_WorkerDescription) =
    print(io, "Malt.Worker on port ", worker.port, " with PID ", worker.pid)

const _worker_states = Set{WorkerState}()
const _worker_states_lock = ReentrantLock()

function _record_error!(state::WorkerState, where::Symbol, error)
    message = try
        string(where, ": ", sprint(showerror, error))
    catch
        string(where, ": error while formatting exception")
    end
    lock(state.lock) do
        push!(state.errors, message)
    end
    nothing
end

function _track_task!(state::WorkerState, name::Symbol, f::Function)
    task = Task(f)
    lock(state.lock) do
        push!(state.tasks, name => task)
    end
    schedule(task)
    task
end
_track_task!(f::Function, state::WorkerState, name::Symbol) = _track_task!(state, name, f)

function _reap_reply_tasks!(state::WorkerState)
    # Called only under state.lock. A weak task which has disappeared cannot
    # execute again; an extant task must actually be done before it is retired.
    filter!(state.reply_tasks) do entry
        task = entry.task.value
        if task === nothing || istaskdone(task)
            state.completed_replies += 1
            state.failed_replies += entry.failed
            false
        else
            true
        end
    end
    nothing
end

function _reply_task!(state::WorkerState, worker::AbstractWorker, id::MsgID,
    cell::Channel{WorkerResult}, transform)
    owner = Ref{Union{Nothing,AbstractWorker}}(worker)
    entry = _ReplyTaskState(WeakRef(nothing), false)
    task = Task() do
        active_owner = owner[]
        try
            GC.@preserve active_owner begin
                transform(_wait_for_response(state, id, cell))
            end
        catch
            lock(state.lock) do
                entry.failed = true
            end
            rethrow()
        finally
            # The closure retained by a finished Task must not itself keep the
            # handle alive. A user-retained RemoteException remains user-owned.
            owner[] = nothing
        end
    end
    entry.task = WeakRef(task)
    lock(state.lock) do
        _reap_reply_tasks!(state)
        push!(state.reply_tasks, entry)
    end
    schedule(task)
    task
end

function _resolve_reply!(state::WorkerState, id::MsgID, result::WorkerResult)
    cell = lock(state.lock) do
        pop!(state.expected_replies, id, nothing)
    end
    cell === nothing && return false
    # One producer wins under the lock; the capacity-one cell cannot block it.
    put!(cell, result)
    true
end

function _forget_reply!(state::WorkerState, id::MsgID, cell::Channel{WorkerResult})
    lock(state.lock) do
        if get(state.expected_replies, id, nothing) === cell
            delete!(state.expected_replies, id)
        end
    end
    nothing
end

function _terminate_replies!(state::WorkerState)
    cells = lock(state.lock) do
        state.admission_open = false
        pending = collect(values(state.expected_replies))
        empty!(state.expected_replies)
        empty!(state.remote_calls)
        pending
    end
    for cell in cells
        put!(cell, WorkerResult(MsgType.special_worker_terminated, nothing))
    end
    nothing
end

function _next_message_id!(state::WorkerState)
    lock(state.lock) do
        state.current_message_id == typemax(MsgID) && error("Worker message IDs exhausted")
        state.current_message_id += MsgID(1)
    end
end

function _admit_message!(state::WorkerState, expect_reply::Bool, application::Bool)
    lock(state.lock) do
        state.admission_open && Base.process_running(state.proc) &&
            isopen(state.current_socket) || throw(TerminatedWorkerException())
        id = _next_message_id!(state)
        cell = expect_reply ? Channel{WorkerResult}(1) : nothing
        cell === nothing || (state.expected_replies[id] = cell)
        application && push!(state.remote_calls, id)
        task = current_task()
        state.active_senders[task] = get(state.active_senders, task, 0) + 1
        id, cell
    end
end

function _finish_sender!(state::WorkerState)
    lock(state.lock) do
        task = current_task()
        count = state.active_senders[task] - 1
        count == 0 ? delete!(state.active_senders, task) :
        (state.active_senders[task] = count)
    end
    nothing
end

function _begin_close!(state::WorkerState, name::Symbol, resource::IO)
    lock(state.lock) do
        any(entry -> first(entry) === name, state.tasks) && return nothing
        _track_task!(state, name) do
            try
                close(resource)
            catch error
                _record_error!(state, name, error)
            end
        end
    end
    nothing
end

function _poison_transport!(state::WorkerState, error)
    _record_error!(state, :transport, error)
    _terminate_replies!(state)
    _begin_close!(state, :close_socket, state.current_socket)
    _request_cleanup!(state; close_unmonitored=false)
    nothing
end

function _send_message!(state::WorkerState, msg_type::UInt8, data, expect_reply::Bool;
    application::Bool=true)
    business =
        application && (
            msg_type === MsgType.from_host_call_with_response ||
                msg_type === MsgType.from_host_call_without_response
        )
    id, cell = _admit_message!(state, expect_reply, business)
    try
        _serialize_msg(state.current_socket, msg_type, id, data)
    catch error
        # A serializer may already have written part of the frame. Removing one
        # reply is insufficient: this connection can no longer carry valid RPCs.
        _poison_transport!(state, error)
        rethrow()
    finally
        _finish_sender!(state)
    end
    id, cell
end

function _wait_for_response(state::WorkerState, id::MsgID, cell::Channel{WorkerResult})
    result = try
        take!(cell)
    finally
        _forget_reply!(state, id, cell)
    end
    worker = state.handle.value
    owner =
        worker isa AbstractWorker ? worker : _WorkerDescription(state.port, state.proc_pid)
    unwrap_worker_result(owner, result)
end

function _seconds_ns(value::Real, name::Symbol)
    value isa Bool && throw(ArgumentError("$name must be a finite, nonnegative duration"))
    seconds = Float64(value)
    isfinite(seconds) && seconds >= 0 ||
        throw(ArgumentError("$name must be a finite, nonnegative duration"))
    ns = seconds * 1.0e9
    ns < Float64(typemax(Int64)) || throw(ArgumentError("$name is too large"))
    UInt64(round(Int64, ns))
end

_remaining_ns(started::UInt64, budget::UInt64) =
    budget - min(budget, time_ns() - started)

function _wait_bounded(predicate::Function, started::UInt64, budget::UInt64)
    while !predicate()
        remaining = _remaining_ns(started, budget)
        remaining == 0 && return false
        sleep(min(0.005, Float64(remaining) / 1.0e9))
    end
    true
end

function _report(state::WorkerState)
    lock(state.lock) do
        _reap_reply_tasks!(state)
        exited = Base.process_exited(state.proc)
        statuses = WorkerTaskStatus[
            WorkerTaskStatus(name, istaskdone(task), istaskfailed(task))
            for (name, task) in state.tasks
        ]
        for entry in state.reply_tasks
            task = entry.task.value
            push!(
                statuses,
                WorkerTaskStatus(:reply,
                    task === nothing || istaskdone(task), entry.failed)
            )
        end
        cleanup = state.cleanup_task
        cleanup === nothing || push!(statuses,
            WorkerTaskStatus(:cleanup, istaskdone(cleanup), istaskfailed(cleanup)))
        tasks_done = all(status -> status.done, statuses) && isempty(state.active_senders)
        socket_closed = !isopen(state.current_socket)
        stdout_closed = !isopen(state.stdout)
        stderr_closed = !isopen(state.stderr)
        pending = length(state.expected_replies)
        complete =
            exited && socket_closed && stdout_closed && stderr_closed &&
                tasks_done && pending == 0
        WorkerShutdownReport(state.proc_pid, state.stage, exited,
            exited ? Int64(state.proc.exitcode) : nothing,
            exited ? Int32(state.proc.termsignal) : nothing, socket_closed,
            stdout_closed, stderr_closed, tasks_done, pending, Tuple(state.errors),
            complete, Tuple(statuses), length(state.active_senders),
            state.unmonitored_discarded, state.completed_replies, state.failed_replies)
    end
end

function _tasks_finished(state::WorkerState, coordinator)
    lock(state.lock) do
        _reap_reply_tasks!(state)
        cleanup = state.cleanup_task
        all(entry -> istaskdone(last(entry)), state.tasks) &&
            isempty(state.reply_tasks) &&
            isempty(state.active_senders) &&
            (cleanup === nothing || cleanup === coordinator || istaskdone(cleanup))
    end
end

function _forget_closed_state!(state::WorkerState; coordinator=nothing)
    report = _report(state)
    if report.process_exited && report.socket_closed && report.stdout_closed &&
        report.stderr_closed && report.pending_replies == 0 &&
        _tasks_finished(state, coordinator)
        lock(_worker_states_lock) do
            delete!(_worker_states, state)
        end
    end
    if report.process_exited
        lock(procs_lock) do
            delete!(__iNtErNaL_running_procs, state.proc)
        end
    end
    nothing
end

function _set_stage!(state::WorkerState, stage::Symbol)
    lock(state.lock) do
        state.stage = stage
        state.stage_started = time_ns()
    end
    nothing
end

function _signal_worker!(state::WorkerState, signal, stage::Symbol)
    _set_stage!(state, stage)
    try
        Base.process_running(state.proc) && Base.kill(state.proc, signal)
    catch error
        _record_error!(state, stage, error)
    end
    nothing
end

function _start_exit_request!(state::WorkerState)
    _set_stage!(state, :graceful)
    _track_task!(state, :send_exit) do
        lock(state.lock) do
            task = current_task()
            state.active_senders[task] = get(state.active_senders, task, 0) + 1
        end
        try
            id = _next_message_id!(state)
            _serialize_msg(state.current_socket, MsgType.from_host_call_without_response,
                id, _new_do_msg(Base.exit, (), pairs((;))))
        catch error
            _record_error!(state, :send_exit, error)
            _begin_close!(state, :close_socket, state.current_socket)
        finally
            _finish_sender!(state)
        end
    end
    nothing
end

function _shutdown_locked!(state::WorkerState, started::UInt64, budget::UInt64,
    exit_budget::UInt64, term_budget::UInt64, close_unmonitored::Bool;
    coordinator=nothing)
    was_running = Base.process_running(state.proc)
    _terminate_replies!(state)
    if !was_running && state.stage === :running
        _set_stage!(state, :already_exited)
    elseif was_running && state.stage === :running && _remaining_ns(started, budget) > 0
        _start_exit_request!(state)
    end

    if Base.process_running(state.proc) && state.stage === :graceful
        phase_remaining = _remaining_ns(state.stage_started, exit_budget)
        _wait_bounded(() -> Base.process_exited(state.proc), time_ns(),
            min(phase_remaining, _remaining_ns(started, budget)))
        if Base.process_running(state.proc) && _remaining_ns(started, budget) > 0
            _signal_worker!(state, Base.SIGTERM, :term)
        end
    end
    if Base.process_running(state.proc) && state.stage === :term
        phase_remaining = _remaining_ns(state.stage_started, term_budget)
        _wait_bounded(() -> Base.process_exited(state.proc), time_ns(),
            min(phase_remaining, _remaining_ns(started, budget)))
        if Base.process_running(state.proc) && _remaining_ns(started, budget) > 0
            _signal_worker!(state, Base.SIGKILL, :kill)
        end
    end
    _wait_bounded(() -> Base.process_exited(state.proc), started, budget)

    if Base.process_exited(state.proc)
        _begin_close!(state, :close_socket, state.current_socket)
        if close_unmonitored
            if !state.monitor_stdout
                lock(state.lock) do
                    state.unmonitored_discarded =
                        (isopen(state.stdout),
                            state.unmonitored_discarded[2]) .| state.unmonitored_discarded
                end
                _begin_close!(state, :close_stdout, state.stdout)
            end
            if !state.monitor_stderr
                lock(state.lock) do
                    state.unmonitored_discarded =
                        (state.unmonitored_discarded[1],
                            isopen(state.stderr)) .| state.unmonitored_discarded
                end
                _begin_close!(state, :close_stderr, state.stderr)
            end
        end
    end
    _wait_bounded(() -> _tasks_finished(state, coordinator), started, budget)
    report = _report(state)
    _forget_closed_state!(state; coordinator)
    owned_closed =
        report.process_exited && report.socket_closed &&
            report.pending_replies == 0 && _tasks_finished(state, coordinator) &&
            (!state.monitor_stdout || report.stdout_closed) &&
            (!state.monitor_stderr || report.stderr_closed)
    acceptable =
        owned_closed && (!close_unmonitored ||
            (report.stdout_closed && report.stderr_closed))
    acceptable || throw(ShutdownIncompleteException(report))
    report, was_running
end

function _shutdown_state!(state::WorkerState; exit_timeout::Real=15.0,
    term_timeout::Real=15.0, timeout::Real=exit_timeout + term_timeout + 20.0,
    close_unmonitored::Bool=true)
    started = time_ns()
    budget = _seconds_ns(timeout, :timeout)
    exit_budget = _seconds_ns(exit_timeout, :exit_timeout)
    term_budget = _seconds_ns(term_timeout, :term_timeout)
    if budget == 0
        report = _report(state)
        report.complete && return report, false
        throw(ShutdownIncompleteException(report))
    end
    acquired = _wait_bounded(() -> trylock(state.shutdown_gate), started, budget)
    acquired || throw(ShutdownIncompleteException(_report(state)))
    try
        _shutdown_locked!(state, started, budget, exit_budget, term_budget,
            close_unmonitored)
    finally
        unlock(state.shutdown_gate)
    end
end

function _request_cleanup!(state::WorkerState; close_unmonitored::Bool=true)
    lock(state.lock) do
        state.cleanup_full |= close_unmonitored
        previous = state.cleanup_task
        previous !== nothing && !istaskdone(previous) && return nothing
        task = Task() do
            # Never wait for a shutdown caller that would then have to join us.
            trylock(state.shutdown_gate) || return nothing
            try
                started = time_ns()
                while true
                    full = lock(state.lock) do
                        state.cleanup_full
                    end
                    _shutdown_locked!(state, started, UInt64(50_000_000_000),
                        UInt64(15_000_000_000), UInt64(15_000_000_000),
                        full; coordinator=current_task())
                    upgraded = lock(state.lock) do
                        state.cleanup_full && !full
                    end
                    upgraded || break
                end
            catch error
                _record_error!(state, :cleanup, error)
            finally
                unlock(state.shutdown_gate)
            end
            nothing
        end
        state.cleanup_task = task
        schedule(task)
    end
    nothing
end

function _finalize_worker(worker)
    state = getfield(worker, :state)
    # Finalizers must never wait for a lock held by another task. Re-register
    # the same callback for a later GC instead of creating an untracked waiter.
    if !trylock(state.lock)
        finalizer(_finalize_worker, worker)
        return nothing
    end
    try
        _request_cleanup!(state)
    finally
        unlock(state.lock)
    end
    nothing
end

function _shutdown_at_exit()
    states = lock(_worker_states_lock) do
        collect(_worker_states)
    end
    for state in states
        try
            _shutdown_state!(state)
        catch error
            # No result is silently accepted during a failed cleanup. A handle
            # still retained in the registry remains available until exit.
            try
                showerror(stderr, error)
                println(stderr)
            catch
            end
        end
    end
    nothing
end

function __init__()
    atexit(_shutdown_at_exit)
end

function _construct_worker_state(; env, exename, exeflags, monitor_stdout,
    monitor_stderr, connect_timeout::Real)
    budget = _seconds_ns(connect_timeout, :connect_timeout)
    started = time_ns()
    output, errors = Pipe(), Pipe()
    cmd = _get_worker_cmd(exename; env, exeflags)
    proc = try
        run(
            pipeline(Cmd(cmd; detach=true, windows_hide=true),
                stdout=output, stderr=errors); wait=false)
    catch
        close(output)
        close(errors)
        rethrow()
    end
    pid = Int32(getpid(proc))
    state = WorkerState(proc, pid, Sockets.TCPSocket(), output, errors,
        monitor_stdout, monitor_stderr)
    lock(procs_lock) do
        push!(__iNtErNaL_running_procs, proc)
    end
    lock(_worker_states_lock) do
        push!(_worker_states, state)
    end
    try
        # Only the child owns these write ends. Keeping them open in the parent
        # would prevent the readers from ever observing EOF after child exit.
        close(output.in)
        close(errors.in)
        yield() # https://github.com/fonsp/Pluto.jl/issues/3423
        port_task = _track_task!(state, :startup_port) do
            parse(UInt16, readline(output))
        end
        _wait_bounded(() -> istaskdone(port_task), started, budget) ||
            error("Worker port announcement timed out")
        state.port = fetch(port_task)
        connect_task = _track_task!(state, :startup_connect) do
            Sockets.connect(state.current_socket, Sockets.localhost, state.port)
            _buffer_writes(state.current_socket)
        end
        _wait_bounded(() -> istaskdone(connect_task), started, budget) ||
            error("Worker connection timed out")
        fetch(connect_task)
    catch error
        throw(_rollback_startup!(state, error))
    end
    state, started, budget
end

function _start_startup_ready!(state::WorkerState, started::UInt64, budget::UInt64)
    _ensure_startup_budget!(started, budget)
    nonce = time_ns()
    ready_task = _track_task!(state, :startup_ready) do
        # TCP connection alone does not prove that main has finished flushing
        # its port announcement. Require a request handled by the serving loop.
        id, cell = _send_message!(state, MsgType.from_host_call_with_response,
            _new_call_msg(true, identity, (nonce,), pairs((;))), true;
            application=false)
        _accept_startup_reply!(
            state,
            id,
            cell::Channel{WorkerResult},
            nonce,
            started,
            budget
        )
    end
    ready_task
end

function _accept_startup_reply!(state::WorkerState, id::MsgID,
    cell::Channel{WorkerResult}, nonce::UInt64, started::UInt64, budget::UInt64)
    observed = _wait_for_response(state, id, cell)
    _ensure_startup_budget!(started, budget)
    observed === nonce || error("Worker readiness response did not match its nonce")
    nothing
end

function _ensure_startup_budget!(started::UInt64, budget::UInt64)
    _remaining_ns(started, budget) > 0 || error("Worker readiness timed out")
    nothing
end

function _rollback_startup!(state::WorkerState, cause)
    # Nothing has been published to the caller. Own unmonitored Pipes too, but
    # do not install a second reader beside a managed stderr listener.
    rollback_started = time_ns()
    _signal_worker!(state, Base.SIGKILL, :kill)
    _terminate_replies!(state)
    output_managed, errors_managed = lock(state.lock) do
        any(entry -> first(entry) === :stdout, state.tasks),
        any(entry -> first(entry) === :stderr, state.tasks)
    end
    stderr_text = ""
    if !errors_managed
        stderr_task = _track_task!(state, :startup_stderr) do
            String(read(state.stderr))
        end
        _wait_bounded(
            () -> istaskdone(stderr_task),
            rollback_started,
            UInt64(1_000_000_000)
        )
        if istaskdone(stderr_task) && !istaskfailed(stderr_task)
            stderr_text = fetch(stderr_task)
        end
    end
    _begin_close!(state, :close_socket, state.current_socket)
    output_managed || _begin_close!(state, :close_stdout, state.stdout)
    errors_managed || _begin_close!(state, :close_stderr, state.stderr)
    report = try
        first(
            _shutdown_state!(state; exit_timeout=0, term_timeout=0,
                timeout=Float64(_remaining_ns(rollback_started,
                        UInt64(20_000_000_000))) / 1.0e9)
        )
    catch cleanup_error
        cleanup_error isa ShutdownIncompleteException ? cleanup_error.report :
        (_record_error!(state, :startup_rollback, cleanup_error); _report(state))
    end
    WorkerStartupException(cause, report, stderr_text)
end

function _monitor_pipe!(state::WorkerState, pipe::Pipe, stream::Symbol)
    try
        # Drain to EOF even after process exit; a dead process can still have
        # buffered output. Sink code is caller code and may require a later retry.
        while isopen(pipe) && !eof(pipe)
            bytes = readline(pipe)
            sink = stream === :stdout ? stdout : stderr
            color = get(sink, :color, false) === true
            escape = stream === :stdout ? "\e[34m" : "\e[33m"
            prefix =
                color ? "$escape[Worker $(state.proc_pid)]:\e[39m " :
                "[Worker $(state.proc_pid)]: "
            write(sink, prefix, bytes, '\n')
            flush(sink)
        end
    catch error
        isopen(pipe) && _record_error!(state, stream, error)
    finally
        try
            close(pipe)
        catch error
            _record_error!(state, stream, error)
        end
    end
    nothing
end

function _receive_worker!(state::WorkerState)
    io = state.current_socket
    try
        for _ in Iterators.countfrom(1)
            isopen(io) && !eof(io) || break
            msg_type = read(io, UInt8)
            msg_id = read(io, MsgID)
            data, success = try
                deserialize(io), true
            catch error
                error, false
            finally
                _discard_until_boundary(io)
            end
            success || (msg_type = MsgType.special_serialization_failure)
            lock(state.lock) do
                delete!(state.remote_calls, msg_id)
            end
            if msg_type === MsgType.from_worker_call_finished
                data === nothing || error("Invalid worker completion acknowledgement")
                continue
            end
            # A reply to a cancelled wait is deliberately discarded. It must
            # never refill another cell or block this receive loop.
            _resolve_reply!(state, msg_id, WorkerResult(msg_type, data))
            data = nothing
        end
    catch error
        if isopen(io) && Base.process_running(state.proc)
            _record_error!(state, :receive, error)
        end
    finally
        _terminate_replies!(state)
        _begin_close!(state, :close_socket, io)
        Base.process_running(state.proc) &&
            _request_cleanup!(state; close_unmonitored=false)
    end
    nothing
end

function _start_worker_loops!(state::WorkerState)
    state.monitor_stdout && _track_task!(state, :stdout) do
        _monitor_pipe!(state, state.stdout, :stdout)
    end
    state.monitor_stderr && _track_task!(state, :stderr) do
        _monitor_pipe!(state, state.stderr, :stderr)
    end
    _track_task!(state, :exit) do
        try
            wait(state.proc)
        catch error
            _record_error!(state, :exit, error)
        finally
            _terminate_replies!(state)
            _begin_close!(state, :close_socket, state.current_socket)
            lock(procs_lock) do
                delete!(__iNtErNaL_running_procs, state.proc)
            end
        end
        nothing
    end
    _track_task!(state, :receive) do
        _receive_worker!(state)
    end
    nothing
end
