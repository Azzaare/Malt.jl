"""
The Malt module doesn't export anything, use qualified names instead.
Internal functions are marked with a leading underscore,
these functions are not stable.
"""
module Malt

using Logging: Logging, @debug
using Serialization: serialize, deserialize
using Sockets: Sockets

using RelocatableFolders: RelocatableFolders

include("./shared.jl")

# ENV["JULIA_DEBUG"] = @__MODULE__





abstract type AbstractWorker end

"""
Malt will raise a `TerminatedWorkerException` when a `remote_call` is made to a `Worker`
that has already been terminated.
"""
struct TerminatedWorkerException <: Exception end

struct RemoteException <: Exception
    worker::AbstractWorker
    message::String
end

function Base.showerror(io::IO, e::RemoteException)
    print(io, "Remote exception from $(summary(e.worker)):\n\n$(e.message)")
end

struct WorkerResult
    msg_type::UInt8
    value::Any
end

function unwrap_worker_result(worker::AbstractWorker, result::WorkerResult)
    if result.msg_type == MsgType.special_serialization_failure
        throw(
            ErrorException(
                "Error deserializing data from $(summary(worker)):\n\n$(sprint(Base.showerror, result.value))"
            )
        )
    elseif result.msg_type == MsgType.special_worker_terminated
        throw(TerminatedWorkerException())
    elseif result.msg_type == MsgType.from_worker_call_failure
        throw(RemoteException(worker, result.value))
    else
        result.value
    end
end

include("DistributedStdlibWorker.jl")

"""
    Malt.InProcessWorker(mod::Module=Main)

This implements the same functions as `Malt.Worker` but runs in the same
process as the caller.
"""
mutable struct InProcessWorker <: AbstractWorker
    host_module::Module
    latest_request_task::Task
    running::Bool

    function InProcessWorker(mod=Main)
        task = schedule(Task(() -> nothing))
        new(mod, task, true)
    end
end

Base.summary(io::IO, w::InProcessWorker) =
    write(io, "Malt.InProcessWorker in module $(w.host_module)")

const __iNtErNaL_running_procs = Set{Base.Process}()
const procs_lock = ReentrantLock()
__iNtErNaL_get_running_procs() =
    Base.@lock procs_lock filter!(Base.process_running, __iNtErNaL_running_procs)

_default_connect_timeout() = parse(Float64, get(ENV, "JULIA_WORKER_TIMEOUT", "30.0"))

include("lifecycle.jl")

"""
    Malt.Worker()

Create a new `Worker`. A `Worker` struct is a handle to a (separate) Julia process.

# Examples

```julia-repl
julia> w = Malt.Worker()
Malt.Worker on port 9000 with PID 12345
```

# Keyword arguments
- `env::Vector{String}`: Environment variables to set in the worker process.
- `exename::String`: Path to the Julia executable to use for the worker process.
- `exeflags::Vector{String}`: Additional command-line flags to pass to the Julia executable.
- `monitor_stdout::Bool=true`: Whether to print the stdout of the worker process to the main process's stdout. When set to `false`, you can access the stdout `Pipe` via the `worker.stdout` field after creation.
- `monitor_stderr::Bool=true`: Same for `stderr`.
- `connect_timeout::Real`: One monotonic budget in seconds for startup, port announcement, TCP connection and an exact protocol readiness round trip before publishing the handle. Defaults to the `JULIA_WORKER_TIMEOUT` environment variable, which `Distributed` (and so `Malt.DistributedStdlibWorker`) also uses, or 30 seconds if it is not set. A failed or expired startup kills the child and performs tracked rollback with its own bounded cleanup allowance; `WorkerStartupException.report` exposes any incomplete cleanup.
"""
mutable struct Worker <: AbstractWorker
    state::WorkerState
    _reply_handles::Dict{MsgID,Channel{WorkerResult}}

    function Worker(; env=String[], exename=Base.julia_cmd()[1], exeflags=[],
        monitor_stdout::Bool=true, monitor_stderr::Bool=true,
        connect_timeout::Real=_default_connect_timeout())
        state, started, budget =
            _construct_worker_state(; env, exename, exeflags, monitor_stdout,
                monitor_stderr, connect_timeout)
        worker = new(state, Dict{MsgID,Channel{WorkerResult}}())
        state.handle = WeakRef(worker)
        GC.@preserve worker begin
            ready_task = try
                _start_worker_loops!(state)
                _start_startup_ready!(state, started, budget)
            catch error
                throw(_rollback_startup!(state, error))
            end
            _complete_worker_startup!(worker, ready_task, started, budget)
        end
    end
end

function _complete_worker_startup!(worker::Worker, ready_task::Task,
    started::UInt64, budget::UInt64)
    GC.@preserve worker begin
        try
            _wait_bounded(() -> istaskdone(ready_task), started, budget) ||
                error("Worker readiness timed out")
            fetch(ready_task)
            _ensure_startup_budget!(started, budget)
            finalizer(_finalize_worker, worker)
            _ensure_startup_budget!(started, budget)
            worker
        catch error
            throw(_rollback_startup!(worker.state, error))
        end
    end
end

# Preserve documented field access (notably the two caller-readable Pipes)
# while listeners retain only the internal state rather than the public handle.
function Base.getproperty(worker::Worker, name::Symbol)
    (name === :state || name === :_reply_handles) && return getfield(worker, name)
    getproperty(getfield(worker, :state), name)
end
function Base.setproperty!(worker::Worker, name::Symbol, value)
    (name === :state || name === :_reply_handles) && return setfield!(worker, name, value)
    setproperty!(getfield(worker, :state), name, value)
end
function Base.propertynames(::Worker, private::Bool=false)
    visible = (:state, :port, :proc, :proc_pid, :current_socket, :current_message_id,
        :expected_replies, :stdout, :stderr)
    private ? (visible..., :_reply_handles) : visible
end

Base.summary(io::IO, worker::Worker) =
    print(io, "Malt.Worker on port ", worker.port, " with PID ", worker.proc_pid)
Base.show(io::IO, worker::Worker) = summary(io, worker)
Base.show(io::IO, ::MIME"text/plain", worker::Worker) = summary(io, worker)

# The entire `src` dir should be relocatable, so that worker.jl can include("MsgType.jl").
const src_path = RelocatableFolders.@path @__DIR__

function _get_worker_cmd(exename=Base.julia_cmd()[1]; env, exeflags)
    return addenv(
        `$exename --startup-file=no $exeflags $(joinpath(src_path, "worker.jl"))`,
        String["OPENBLAS_NUM_THREADS=1", Base.byteenv(env)...]
    )
end





## We use tuples instead of structs for messaging so the worker doesn't need to load additional modules.

_new_call_msg(send_result::Bool, f::Function, args, kwargs) = (
    f,
    args,
    kwargs,
    !send_result,
)

_new_do_msg(f::Function, args, kwargs) = (
    f,
    args,
    kwargs,
    true,
)




# function _ensure_connected(w::Worker)
#     # TODO: check if process running?
#     # TODO: `while` instead of `if`?
#     if w.current_socket === nothing || !isopen(w.current_socket)
#         w.current_socket = connect(w.port)
#         @async _receive_loop(w)
#     end
#     return w
# end





# GENERIC COMMUNICATION PROTOCOL

"""
Low-level: send a message to a worker. Returns a `msg_id::MsgID` (`UInt64`), which can be used to wait for a response with `_wait_for_response`.
"""
function _send_msg(
    worker::Worker,
    msg_type::UInt8,
    msg_data,
    expect_reply::Bool=true
)::MsgID
    GC.@preserve worker begin
        id, cell = _send_message!(worker.state, msg_type, msg_data, expect_reply)
        if cell !== nothing
            lock(worker.state.lock) do
                worker._reply_handles[id] = cell
            end
        end
        id
    end
end

"""
Low-level: wait for a response to a previously sent message. Returns the response. Blocking call.
"""
function _wait_for_response(worker::Worker, msg_id::MsgID)
    cell = lock(worker.state.lock) do
        get(worker._reply_handles, msg_id) do
            get(worker.expected_replies, msg_id, nothing)
        end
    end
    cell === nothing && throw(TerminatedWorkerException())
    try
        _wait_for_response(worker, msg_id, cell)
    finally
        lock(worker.state.lock) do
            if get(worker._reply_handles, msg_id, nothing) === cell
                delete!(worker._reply_handles, msg_id)
            end
        end
    end
end

function _wait_for_response(worker::Worker, id::MsgID, cell::Channel{WorkerResult})
    GC.@preserve worker begin
        _wait_for_response(worker.state, id, cell)
    end
end

"""
`_wait_for_response ∘ _send_msg`
"""
function _send_receive(w::Worker, msg_type::UInt8, msg_data)
    GC.@preserve w begin
        id, cell = _send_message!(w.state, msg_type, msg_data, true)
        _wait_for_response(w, id, cell::Channel{WorkerResult})
    end
end

"""
`@async(_wait_for_response) ∘ _send_msg`
"""
function _send_receive_async(
    w::Worker,
    msg_type::UInt8,
    msg_data,
    output_transformation=identity
)::Task
    state = w.state
    id, cell = _send_message!(state, msg_type, msg_data, true)
    _reply_task!(state, w, id, cell::Channel{WorkerResult}, output_transformation)
end





"""
    Malt.remote_call(f, w::Worker, args...; kwargs...)

Evaluate `f(args...; kwargs...)` in worker `w` asynchronously.
Returns a task that acts as a promise; the result value of the task is the
result of the computation.

The function `f` must already be defined in the namespace of `w`.

# Examples

```julia-repl
julia> promise = Malt.remote_call(uppercase ∘ *, w, "I ", "declare ", "bankruptcy!");

julia> fetch(promise)
"I DECLARE BANKRUPTCY!"
```
"""
function remote_call(f, w::Worker, args...; kwargs...)
    _send_receive_async(
        w,
        MsgType.from_host_call_with_response,
        _new_call_msg(true, f, args, kwargs),
    )
end
function remote_call(f, w::InProcessWorker, args...; kwargs...)
    w.latest_request_task = @async remote_call_fetch(f, w, args...; kwargs...)
end
function remote_call_fetch(f, w::InProcessWorker, args...; kwargs...)
    try
        f(args...; kwargs...)
    catch ex
        throw(
            RemoteException(
                w,
                sprint() do io
                    Base.invokelatest(showerror, io, ex, catch_backtrace())
                end
            )
        )
    end
end
function remote_call_wait(f, w::InProcessWorker, args...; kwargs...)
    remote_call_fetch(f, w, args...; kwargs...)
    nothing
end

"""
    Malt.remote_call_fetch(f, w::Worker, args...; kwargs...)

Shorthand for `fetch(Malt.remote_call(…))`. Blocks and then returns the result of the remote call.
"""
function remote_call_fetch(f, w::AbstractWorker, args...; kwargs...)
    fetch(remote_call(f, w, args...; kwargs...))
end
function remote_call_fetch(f, w::Worker, args...; kwargs...)
    _send_receive(
        w,
        MsgType.from_host_call_with_response,
        _new_call_msg(true, f, args, kwargs)
    )
end

"""
    Malt.remote_call_wait(f, w::Worker, args...; kwargs...)

Shorthand for `wait(Malt.remote_call(…))`. Blocks and discards the resulting value.
"""
function remote_call_wait(f, w::AbstractWorker, args...; kwargs...)
    wait(remote_call(f, w, args...; kwargs...))
end
function remote_call_wait(f, w::Worker, args...; kwargs...)
    _send_receive(
        w,
        MsgType.from_host_call_with_response,
        _new_call_msg(false, f, args, kwargs)
    )
end


"""
    Malt.remote_do(f, w::Worker, args...; kwargs...)

Start evaluating `f(args...; kwargs...)` in worker `w` asynchronously, and return `nothing`.

Unlike `remote_call`, no reference to the remote call is available. This means:
- You cannot wait for the call to complete on the worker.
- The value returned by `f` is not available.
"""
function remote_do(f, w::Worker, args...; kwargs...)
    _send_msg(
        w,
        MsgType.from_host_call_without_response,
        _new_do_msg(f, args, kwargs),
        false
    )
    nothing
end
function remote_do(f, ::InProcessWorker, args...; kwargs...)
    @async f(args...; kwargs...)
    nothing
end


## Eval variants

"""
    Malt.remote_eval(mod::Module=Main, w::Worker, expr)

Evaluate expression `expr` under module `mod` on the worker `w`.
`Malt.remote_eval` is asynchronous, like `Malt.remote_call`.

The module `m` and the type of the result of `expr` must be defined in both the
main process and the worker.

# Examples

```julia-repl
julia> Malt.remote_eval(w, quote
    x = "x is a global variable"
end)

julia> Malt.remote_eval_fetch(w, :x)
"x is a global variable"
```

"""
remote_eval(mod::Module, w::AbstractWorker, expr) = remote_call(Core.eval, w, mod, expr)
remote_eval(w::AbstractWorker, expr) = remote_eval(Main, w, expr)


"""
Shorthand for `fetch(Malt.remote_eval(…))`. Blocks and returns the resulting value.
"""
remote_eval_fetch(mod::Module, w::AbstractWorker, expr) =
    remote_call_fetch(Core.eval, w, mod, expr)
remote_eval_fetch(w::AbstractWorker, expr) = remote_eval_fetch(Main, w, expr)


"""
Shorthand for `wait(Malt.remote_eval(…))`. Blocks and discards the resulting value.
"""
remote_eval_wait(mod::Module, w::AbstractWorker, expr) =
    remote_call_wait(Core.eval, w, mod, expr)
remote_eval_wait(w::AbstractWorker, expr) = remote_eval_wait(Main, w, expr)


"""
    Malt.worker_channel(w::AbstractWorker, expr)

Create a channel to communicate with worker `w`. `expr` must be an expression
that evaluates to an `AbstractChannel`. `expr` should assign the channel to a (global) variable
so the worker has a handle that can be used to send messages back to the manager.
"""
function worker_channel(w::Worker, expr)
    RemoteChannel(w, expr)
end
function worker_channel(w::InProcessWorker, expr)
    remote_call_fetch(w) do
        result = Core.eval(w.host_module, expr)
        @assert result isa AbstractChannel
        result
    end
end


struct RemoteChannel{T} <: AbstractChannel{T}
    worker::Worker
    id::UInt64

    function RemoteChannel{T}(worker::Worker, expr) where {T}

        id = _next_message_id!(worker.state)
        remote_eval_wait(Main, worker, quote
            Main._channel_cache[$id] = $expr
        end)
        new{T}(worker, id)
    end

    RemoteChannel(w::Worker, expr) = RemoteChannel{Any}(w, expr)
end

Base.take!(rc::RemoteChannel) =
    remote_eval_fetch(Main, rc.worker, :(take!(Main._channel_cache[$(rc.id)])))::eltype(rc)

Base.put!(rc::RemoteChannel, v) =
    remote_eval_wait(Main, rc.worker, :(put!(Main._channel_cache[$(rc.id)], $v)))

Base.isready(rc::RemoteChannel) =
    remote_eval_fetch(Main, rc.worker, :(isready(Main._channel_cache[$(rc.id)])))::Bool

Base.wait(rc::RemoteChannel) =
    remote_eval_wait(Main, rc.worker, :(wait(Main._channel_cache[$(rc.id)])))::Bool


## Signals & Termination

"""
    Malt.isrunning(w::Worker)::Bool

Check whether the worker process `w` is running.
"""
isrunning(w::Worker)::Bool = Base.process_running(w.proc)
isrunning(w::InProcessWorker) = w.running

_assert_is_running(w::Worker) = isrunning(w) || throw(TerminatedWorkerException())


"""
    Malt.stop(w::Worker; exit_timeout::Real=15.0, term_timeout::Real=15.0)::Bool

Terminate the worker process `w` in the nicest possible way. We first try using `Base.exit`, then SIGTERM, then SIGKILL. Waits for the worker process to be terminated.

If `w` is still alive, and now terminated, `stop` returns true.
If `w` is already dead, `stop` returns `false`.
If `w` failed to terminate, throw an exception.
"""
function stop(w::Worker; exit_timeout::Real=15.0, term_timeout::Real=15.0,
    timeout::Real=exit_timeout + term_timeout + 20.0)
    GC.@preserve w begin
        _, was_running = _shutdown_state!(w.state; exit_timeout, term_timeout, timeout,
            close_unmonitored=false)
        was_running
    end
end

"""
    Malt.shutdown(w::Worker; exit_timeout=15.0, term_timeout=15.0,
                  timeout=exit_timeout + term_timeout + 20.0,
                  close_unmonitored=true)::WorkerShutdownReport

Stop admission, settle outstanding RPCs, request `Base.exit`, then escalate to
TERM and KILL if necessary. One monotonic `timeout` (seconds) includes waiting
for another shutdown, sending the exit request, closing connections and joining
all retained tasks. The two phase timeouts are caps within that total budget.

The immutable report includes the actual exit code and signal. A resource-complete
shutdown does not imply a successful computation. Incomplete cleanup throws
`ShutdownIncompleteException` carrying `.report`; a later call can retry it.
With `timeout=0`, only an already complete observation is returned, without
starting work. Arbitrary noncooperative serializer or output-sink code cannot
be forcibly preempted and prevents a complete report while it is still active.

Unmonitored Pipes are closed and their unread contents discarded by default.
With `close_unmonitored=false`, caller-owned Pipes remain readable: an otherwise
finished shutdown returns a report with `complete=false` until they are closed.
Legacy `stop` preserves those Pipes and returns its historical entry-state Bool.
"""
function shutdown(w::Worker; exit_timeout::Real=15.0, term_timeout::Real=15.0,
    timeout::Real=exit_timeout + term_timeout + 20.0, close_unmonitored::Bool=true)
    GC.@preserve w begin
        first(
            _shutdown_state!(w.state; exit_timeout, term_timeout, timeout,
                close_unmonitored)
        )
    end
end
function stop(w::InProcessWorker)
    w.running = false
    true
end

"""
    kill(w::Malt.Worker, signum=Base.SIGTERM)

Terminate the worker process `w` forcefully by sending a `SIGTERM` signal (unless otherwise specified).

This is not the recommended way to terminate the process. See `Malt.stop`.
"""
Base.kill(w::Worker, signum=Base.SIGTERM) = Base.kill(w.proc, signum)
Base.kill(::InProcessWorker, signum=Base.SIGTERM) = nothing


function _poll(f::Function; interval::Real=0.01, timeout_s::Real=Inf64)
    tstart = time()
    while true
        f() && return true
        if time() - tstart >= timeout_s
            return false
        end
        sleep(interval)
    end
end

_wait_for_exit(::AbstractWorker; timeout_s::Real=20.0) = nothing
function _wait_for_exit(w::Worker; timeout_s::Real=20.0)
    if !_poll(() -> !isrunning(w); timeout_s)
        error("HOST: Worker did not exit after $timeout_s seconds")
    end
end


"""
    Malt.interrupt(w::Worker)

Request an interrupt while an application call may still be executing remotely.
With no known remote application work, this is a no-op. Local abandonment of a
waiter does not make its remote call idle; `remote_do` remains active until its
internal completion acknowledgement arrives. Startup readiness is not application
work. Native SIGINT is asynchronous and does not target a particular message ID:
the last result may still be in transit, so this is not a general proof that a
signal cannot arrive after the remote computation finished.
"""
function interrupt(w::Worker)
    GC.@preserve w begin
        state = w.state
        outcome = lock(state.lock) do
            Base.process_running(state.proc) || return :dead
            isempty(state.remote_calls) && return :idle
            if Sys.iswindows()
                ccall(
                    (:GenerateConsoleCtrlEvent, "Kernel32"),
                    Bool,
                    (UInt32, UInt32),
                    UInt32(1),
                    UInt32(state.proc_pid)
                )
            else
                Base.kill(state.proc, Base.SIGINT)
            end
            :sent
        end
        outcome === :dead &&
            @warn "Tried to interrupt a worker that has already shut down." summary(w)
    end
    nothing
end
function interrupt(w::InProcessWorker)
    istaskdone(w.latest_request_task) ||
        schedule(w.latest_request_task, InterruptException(); error=true)
    nothing
end




# Based on `Base.task_done_hook`
function _rethrow_to_repl(e::InterruptException; rethrow_regular::Bool=false)
    if isdefined(Base, :active_repl_backend) &&
        isdefined(Base.active_repl_backend, :backend_task) &&
        isdefined(Base.active_repl_backend, :in_eval) &&
        Base.active_repl_backend.backend_task.state === :runnable &&
        (isdefined(Base, :Workqueue) || isempty(Base.Workqueue)) &&
        Base.active_repl_backend.in_eval

        @debug "HOST: Rethrowing interrupt to REPL"
        @async Base.schedule(Base.active_repl_backend.backend_task, e; error=true)
    elseif rethrow_regular
        @debug "HOST: Don't know what to do with this interrupt, rethrowing" exception =
            (e, catch_backtrace())
        rethrow(e)
    end
end

end # module
