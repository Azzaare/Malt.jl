module MaltLifecycleContracts

using Test, Serialization, Sockets, IOCapture
import Malt
const M = Malt

# These are correctness deadlines, not performance assertions. The outer suite
# also runs under a process watchdog so a broken callback/lock cannot hang CI.
function await_task(task; timeout = 20.0)
    status = timedwait(() -> istaskdone(task), timeout; pollint = 0.01)
    @test status == :ok
    status == :ok || error("lifecycle rendezvous exceeded its deadline")
    return task
end

function worker_flags()
    project = Base.active_project()
    project === nothing && error("lifecycle tests require an explicit Julia project")
    flags = ["--history-file=no", "--threads=1", "--project=$(dirname(project))"]
    VERSION >= v"1.10" && push!(flags, "--gcthreads=1")
    return flags
end

function new_worker(; kwargs...)
    defaults = (exeflags = worker_flags(), connect_timeout = 15.0)
    return M.Worker(; merge(defaults, (; kwargs...))...)
end

tasks(worker) = Task[last(pair) for pair in worker.state.tasks]
function physical_status(worker)
    return (
        process_exited = !Base.process_running(worker.proc),
        tasks_done = all(istaskdone, tasks(worker)),
        pending_empty = isempty(worker.expected_replies),
        handles_closed = !isopen(worker.current_socket) &&
                         !isopen(worker.stdout) &&
                         !isopen(worker.stderr),
    )
end
function task_exceptions(task)
    @static if isdefined(Base, :current_exceptions)
        return [item.exception for item in Base.current_exceptions(task)]
    else
        # Julia 1.6 provides the same task-specific observation through the
        # earlier catch_stack API, with exception objects returned directly.
        return Base.catch_stack(task; include_bt = false)
    end
end
function has_task_exception(task, T)
    istaskfailed(task) || return false
    return any(error -> error isa T, task_exceptions(task))
end

function remote_calls_snapshot(worker)
    lock(worker.state.lock) do
        copy(worker.state.remote_calls)
    end
end

function traced_interrupt(worker, phase)
    tracing = get(ENV, "MALT_TRACE_INTERRUPT_PHASES", "false") == "true"
    if tracing
        println(
            stderr,
            "MALT_INTERRUPT_TRACE_BEGIN worker=",
            worker.proc_pid,
            " phase=",
            phase,
        )
        flush(stderr)
    end
    result = M.interrupt(worker)
    if tracing
        println(
            stderr,
            "MALT_INTERRUPT_TRACE_END worker=",
            worker.proc_pid,
            " phase=",
            phase,
        )
        flush(stderr)
    end
    return result
end

function shutdown_outcome(worker; kwargs...)
    try
        return M.shutdown(
            worker;
            timeout = 20.0,
            exit_timeout = 2.0,
            term_timeout = 2.0,
            kwargs...,
        ),
        nothing
    catch error
        error isa M.ShutdownIncompleteException || rethrow()
        return error.report, error
    end
end

function with_worker(operation; kwargs...)
    worker = new_worker(; kwargs...)
    try
        return operation(worker)
    finally
        report, failure = shutdown_outcome(worker)
        # A deliberate poisoned transport can report its error even after every
        # resource is closed. Normal tests assert their own error-free report.
        @test failure === nothing
        @test report.complete
        if !report.process_exited
            Base.process_running(worker.proc) && Base.kill(worker.proc, Base.SIGKILL)
            await_task(@async wait(worker.proc))
        end
        for handle in (worker.current_socket, worker.stdout, worker.stderr)
            isopen(handle) && close(handle)
        end
        actual = physical_status(worker)
        @test actual.process_exited
        @test actual.tasks_done
        @test actual.pending_empty
        @test actual.handles_closed
        failure === nothing || @test failure.report === report
    end
end

function assert_complete(worker, report; allow_errors = false)
    @test report isa M.WorkerShutdownReport
    @test report.pid == worker.proc_pid
    @test report.process_exited
    @test report.socket_closed && report.stdout_closed && report.stderr_closed
    @test report.tasks_done
    @test report.pending_replies == 0
    @test report.complete
    allow_errors || @test isempty(report.errors)
    actual = physical_status(worker)
    @test actual.process_exited
    @test actual.handles_closed
    @test actual.tasks_done
    @test actual.pending_empty
    if Sys.islinux()
        @test !ispath("/proc/$(worker.proc_pid)")
    end
end

function gated_expression(port, payload)
    return quote
        control = Sockets.connect(Sockets.IPv4(0x7f000001), $port)
        try
            write(control, UInt8(0x51))
            flush(control)
            read(control, UInt8)
            $payload
        finally
            close(control)
        end
    end
end

function control_rpc(worker, payload)
    server = Sockets.listen(ip"127.0.0.1", 0)
    port = Int(last(Sockets.getsockname(server)))
    accepted = @async Sockets.accept(server)
    promise = M.remote_eval(worker, gated_expression(port, payload))
    socket = fetch(await_task(accepted))
    @test fetch(await_task(@async read(socket, UInt8))) == 0x51
    close(server)
    return socket, promise
end

struct PartialSerializationFailure
    nonce::UInt32
end
const serializer_calls = Ref(0)
const serializer_bytes = Ref(0)
function Serialization.serialize(
    serializer::Serialization.AbstractSerializer,
    ::PartialSerializationFailure,
)
    serializer_calls[] += 1
    serializer_bytes[] += write(serializer.io, UInt8[0xd3, 0x5a, 0x00, 0xfe, 0x71, 0x99])
    error("deliberate_partial_serialization_failure")
end

struct BlockingSerializationFailure
    nonce::UInt32
end
const serializer_entered = Ref{Union{Nothing,Channel{Nothing}}}(nothing)
const serializer_release = Ref{Union{Nothing,Channel{Nothing}}}(nothing)
function Serialization.serialize(
    ::Serialization.AbstractSerializer,
    ::BlockingSerializationFailure,
)
    put!(serializer_entered[]::Channel{Nothing}, nothing)
    take!(serializer_release[]::Channel{Nothing})
    error("deliberate_released_serialization_failure")
end

@testset "Shutdown drains the final unterminated stdout/stderr bytes" begin
    # Monitor output is line-formatted by Malt. Its prefix and trailing newline
    # are part of that presentation; the payload itself must arrive exactly once.
    payload = repeat("tail-0123456789", 6000) * "\0FINAL"
    for stream in (:stdout, :stderr)
        observed_pid = Ref(0)
        capture = IOCapture.capture(; color = false) do
            with_worker() do worker
                observed_pid[] = Int(worker.proc_pid)
                M.remote_eval_wait(worker, quote
                    write($stream, $payload)
                    flush($stream)
                end)
                report = M.shutdown(worker; timeout = 20.0)
                assert_complete(worker, report)
            end
        end
        expected = "[Worker $(observed_pid[])]: " * payload * "\n"
        @test Vector{UInt8}(codeunits(capture.output)) == Vector{UInt8}(codeunits(expected))
    end
end

@testset "Legacy stop preserves borrowed pipes; full shutdown closes them" begin
    out_bytes = UInt8[mod(37i + 11, 256) for i = 1:70003]
    err_bytes = UInt8[mod(53i + 7, 256) for i = 1:70007]
    out_tail = UInt8[0xde, 0xad, 0x00, 0x71, 0xff]
    err_tail = UInt8[0xbe, 0xef, 0x7f, 0x19, 0x00]
    with_worker(; monitor_stdout = false, monitor_stderr = false) do worker
        # Read a known prefix while the child is alive, leaving a small unread
        # tail for the borrowed-Pipe observation after stop. A read through EOF
        # may itself close a PipeEndpoint in Base, independently of Malt.
        out_task = @async read(worker.stdout, length(out_bytes))
        err_task = @async read(worker.stderr, length(err_bytes))
        M.remote_eval_wait(worker, quote
            write(stdout, $out_bytes)
            write(stdout, $out_tail)
            flush(stdout)
            write(stderr, $err_bytes)
            write(stderr, $err_tail)
            flush(stderr)
        end)
        @test fetch(await_task(out_task)) == out_bytes
        @test fetch(await_task(err_task)) == err_bytes
        @test M.stop(worker; exit_timeout = 2.0, term_timeout = 2.0)
        @test isopen(worker.stdout) && isopen(worker.stderr)
        borrowed = M.shutdown(worker; timeout = 20.0, close_unmonitored = false)
        @test borrowed.process_exited && borrowed.socket_closed && borrowed.tasks_done
        @test !borrowed.stdout_closed && !borrowed.stderr_closed
        @test borrowed.pending_replies == 0 && isempty(borrowed.errors)
        @test !borrowed.complete
        @test fetch(await_task(@async read(worker.stdout))) == out_tail
        @test fetch(await_task(@async read(worker.stderr))) == err_tail
        @test !M.stop(worker; exit_timeout = 2.0, term_timeout = 2.0)
        full = M.shutdown(worker; timeout = 20.0)
        assert_complete(worker, full)
        again = M.shutdown(worker; timeout = 20.0)
        assert_complete(worker, again)
        @test (full.pid, full.exitcode, full.termsignal) ==
              (again.pid, again.exitcode, again.termsignal)
    end
end

@testset "Concurrent stop callers serialize one observed transition" begin
    with_worker() do worker
        gate = Channel{Nothing}(4)
        callers = [
            Threads.@spawn(begin
                take!(gate)
                M.stop(worker; exit_timeout = 2.0, term_timeout = 2.0)
            end) for _ = 1:4
        ]
        foreach(_ -> put!(gate, nothing), callers)
        results = [fetch(await_task(task)) for task in callers]
        @test count(identity, results) == 1
        @test count(!, results) == 3
        close(gate)
        report = M.shutdown(worker; timeout = 20.0)
        assert_complete(worker, report)
    end
end

@testset "Replies and terminal notification resolve once" begin
    with_worker() do worker
        payload = (UInt64(0x123456789abcdef0), UInt8[mod(29i + 3, 256) for i = 1:65539])
        @test M.remote_call_fetch(identity, worker, payload) == payload
        @test isempty(worker.expected_replies)
        # Intrinsic admission for the private resolver seam. The normal waiter
        # owns removal, so this oracle never repairs the production tables.
        id = typemax(M.MsgID) - M.MsgID(1)
        cell = Channel{M.WorkerResult}(1)
        lock(worker.state.lock) do
            @test !haskey(worker.expected_replies, id)
            worker.expected_replies[id] = cell
        end
        waiter = @async M._wait_for_response(worker, id, cell)
        gate = Channel{Nothing}(2)
        valid = M.WorkerResult(M.MsgType.from_worker_call_result, payload)
        dead = M.WorkerResult(M.MsgType.special_worker_terminated, nothing)
        producers = [
            Threads.@spawn(begin
                take!(gate)
                M._resolve_reply!(worker.state, id, result)
            end) for result in (valid, dead)
        ]
        put!(gate, nothing)
        put!(gate, nothing)
        delivered = [fetch(await_task(task)) for task in producers]
        @test count(identity, delivered) == 1
        @test count(!, delivered) == 1
        await_task(waiter)
        if istaskfailed(waiter)
            @test has_task_exception(waiter, M.TerminatedWorkerException)
            @test delivered == [false, true]
        else
            @test fetch(waiter) == payload
            @test delivered == [true, false]
        end
        @test !isready(cell)
        @test isempty(worker.expected_replies)
        close(gate)
        close(cell)
    end

    # A separate real RPC is in flight when its process dies. No return value
    # can be manufactured by a terminal notification or left waiting forever.
    with_worker(; monitor_stdout = false, monitor_stderr = false) do worker
        payload = (UInt64(0x3141592653589793), UInt8[mod(19i, 256) for i = 1:65541])
        control, promise = control_rpc(worker, payload)
        try
            @test !istaskdone(promise)
            Base.kill(worker.proc, Base.SIGKILL)
            report = M.shutdown(worker; timeout = 20.0)
            assert_complete(worker, report; allow_errors = true)
            if Sys.isunix()
                @test report.termsignal == Int(Base.SIGKILL)
            else
                @test_skip "The POSIX signal number is qualified on Unix only"
            end
            await_task(promise)
            @test has_task_exception(promise, M.TerminatedWorkerException)
        finally
            close(control)
        end
    end
end

@testset "Cancelled waiters drop admission and ignore late replies" begin
    with_worker() do worker
        payload = (UInt64(0xfedcba9876543210), UInt8[0x00, 0xff, 0x61, 0x7f])
        control, promise = control_rpc(worker, payload)
        try
            @test length(worker.expected_replies) == 1
            @test !istaskdone(promise)
            schedule(promise, InterruptException(); error = true)
            await_task(promise)
            @test has_task_exception(promise, InterruptException)
            @test isempty(worker.expected_replies)
            write(control, UInt8(0x52))
            flush(control)
            @test M.remote_call_fetch(identity, worker, payload) == payload
            @test isempty(worker.expected_replies)
            @test M.isrunning(worker)
        finally
            close(control)
        end
    end
end

@testset "Partial serialization failure poisons transport and settles all waiters" begin
    with_worker(; monitor_stdout = false, monitor_stderr = false) do worker
        control, first_promise = control_rpc(worker, UInt64(0xabcdef0123456789))
        serializer_calls[] = 0
        serializer_bytes[] = 0
        try
            @test length(worker.expected_replies) == 1
            @test_throws ErrorException M.remote_call(
                identity,
                worker,
                PartialSerializationFailure(UInt32(0x12345678)),
            )
            @test serializer_calls[] == 1 && serializer_bytes[] == 6
            await_task(first_promise)
            @test has_task_exception(first_promise, M.TerminatedWorkerException)
            @test isempty(worker.expected_replies)
            @test timedwait(() -> !isopen(worker.current_socket), 5.0; pollint = 0.01) ==
                  :ok
            @test !isopen(worker.current_socket)
            @test_throws M.TerminatedWorkerException M.remote_call(identity, worker, 1)
            report, failure = shutdown_outcome(worker)
            @test report.process_exited && report.socket_closed
            @test report.stdout_closed && report.stderr_closed && report.tasks_done
            @test report.pending_replies == 0
            @test all(istaskdone, tasks(worker))
            @test !isempty(report.errors)
            failure === nothing || @test failure.report === report
        finally
            close(control)
        end
    end
end

@testset "A shutdown deadline observes blocked caller code without preempting it" begin
    with_worker(; monitor_stdout = false, monitor_stderr = false) do worker
        entered = Channel{Nothing}(1)
        release = Channel{Nothing}(1)
        serializer_entered[] = entered
        serializer_release[] = release
        sender = @async M.remote_call(
            identity,
            worker,
            BlockingSerializationFailure(UInt32(0x31415926)),
        )
        try
            fetch(await_task(@async take!(entered)))
            @test !istaskdone(sender)
            incomplete = try
                M.shutdown(worker; timeout = 0.25, exit_timeout = 0.0, term_timeout = 0.0)
                nothing
            catch error
                error
            end
            @test incomplete isa M.ShutdownIncompleteException
            @test !incomplete.report.complete
            @test !incomplete.report.tasks_done
            @test !istaskdone(sender)
        finally
            put!(release, nothing)
            await_task(sender)
            @test has_task_exception(sender, ErrorException)
            serializer_entered[] = nothing
            serializer_release[] = nothing
            close(entered)
            close(release)
        end
        report = M.shutdown(worker; timeout = 20.0, exit_timeout = 0.0, term_timeout = 0.0)
        assert_complete(worker, report; allow_errors = true)
        @test !isempty(report.errors)
    end
    with_worker() do worker
        initial_id = worker.current_message_id
        incomplete = try
            M.shutdown(worker; timeout = 0.0)
            nothing
        catch error
            error
        end
        @test incomplete isa M.ShutdownIncompleteException
        @test !incomplete.report.complete && incomplete.report.stage == :running
        @test M.isrunning(worker)
        @test worker.current_message_id == initial_id
        @test M.remote_call_fetch(identity, worker, UInt32(0x71abcdef)) == 0x71abcdef
        for invalid in (-1.0, Inf, NaN, true)
            @test_throws ArgumentError M.shutdown(worker; timeout = invalid)
        end
        report = M.shutdown(worker; timeout = 20.0)
        assert_complete(worker, report)
        @test M.shutdown(worker; timeout = 0.0).complete
    end
end

@testset "Constructor errors reap only their owned process" begin
    for stage in (:invalid_port, :refused_connection, :no_port, :connected_without_ack)
        server = Sockets.listen(ip"127.0.0.1", 0)
        port = Int(last(Sockets.getsockname(server)))
        accepted = @async Sockets.accept(server)
        publication = if stage == :invalid_port
            "println(stdout, \"notaport\"); flush(stdout)"
        elseif stage == :refused_connection
            "println(stdout, 0); flush(stdout)"
        elseif stage == :connected_without_ack
            "server=Sockets.listen(Sockets.IPv4(0x7f000001),0); " *
            "println(stdout,last(Sockets.getsockname(server))); flush(stdout); " *
            "peer=Sockets.accept(server); request=read(peer,UInt8); " *
            "println(control,\"connected:\",request); flush(control)"
        else
            "nothing"
        end
        code =
            "using Sockets; control=Sockets.connect(Sockets.IPv4(0x7f000001), $port); " *
            "println(control,getpid()); flush(control); read(control,UInt8); " *
            publication *
            "; read(control,UInt8)"
        constructor = @async new_worker(;
            exeflags = vcat(worker_flags(), ["-e", code]),
            connect_timeout = stage in (:no_port, :connected_without_ack) ? 8.0 : 15.0,
            monitor_stdout = false,
            monitor_stderr = false,
        )
        control = nothing
        owned_proc = nothing
        try
            control = fetch(await_task(accepted))
            pid = parse(Int, fetch(await_task(@async readline(control))))
            owned_proc = only(
                filter(
                    proc -> Base.process_running(proc) && Int(getpid(proc)) == pid,
                    collect(M.__iNtErNaL_get_running_procs()),
                ),
            )
            write(control, UInt8(0x61))
            flush(control)
            if stage == :connected_without_ack
                # A real peer accepted the connection and consumed a request;
                # lack of readiness ACK, not a failed TCP connect, is the fault.
                announcement = fetch(await_task(@async readline(control)))
                @test announcement == "connected:1"
            end
            await_task(constructor; timeout = 35.0)
            @test istaskfailed(constructor)
            if !istaskfailed(constructor)
                # Keep the failed verdict, but close the unexpected public
                # handle before advancing to the next controlled fixture.
                unexpected = fetch(constructor)
                M.shutdown(unexpected; timeout = 20.0)
                continue
            end
            startup = only(
                error for error in task_exceptions(constructor) if
                error isa M.WorkerStartupException
            )
            @test startup.report.pid == pid
            @test startup.report.complete
            @test startup.report.process_exited && startup.report.tasks_done
            @test startup.report.socket_closed &&
                  startup.report.stdout_closed &&
                  startup.report.stderr_closed &&
                  startup.report.pending_replies == 0
            child_exited = !Base.process_running(owned_proc)
            registry_empty = isempty(M.__iNtErNaL_get_running_procs())
            @test child_exited
            @test registry_empty
            @test fetch(await_task(@async eof(control)))
            Sys.islinux() && @test !ispath("/proc/$pid")
        finally
            close(server)
            control === nothing || close(control)
            if owned_proc !== nothing && Base.process_running(owned_proc)
                Base.kill(owned_proc, Base.SIGKILL)
                await_task(@async wait(owned_proc))
            end
        end
    end
end

function readiness_rendezvous(worker, nonce)
    server = Sockets.listen(ip"127.0.0.1", 0)
    port = Int(last(Sockets.getsockname(server)))
    accepted = @async Sockets.accept(server)
    control = nothing
    expected = UInt8[mod(31i + Int(nonce & 0xff), 256) for i = 1:513]
    promise = M.remote_eval(
        worker,
        quote
            control = Sockets.connect(Sockets.IPv4(0x7f000001), $port)
            try
                println(control, $nonce, ":", getpid())
                flush(control)
                read(control, UInt8)
                (
                    $nonce,
                    getpid(),
                    UInt8[(UInt64(31) * UInt64(i) + $(nonce & 0xff)) & 0xff for i = 1:513],
                )
            finally
                close(control)
            end
        end,
    )
    try
        control = fetch(await_task(accepted))
        announcement = fetch(await_task(@async readline(control)))
        @test announcement == string(nonce, ":", worker.proc_pid)
        write(control, UInt8(0x73))
        flush(control)
        @test fetch(await_task(promise)) == (nonce, Int(worker.proc_pid), expected)
        @test isempty(worker.expected_replies)
    finally
        close(server)
        control === nothing || close(control)
    end
end

@testset "Immediate startup interrupt leaves the published worker usable" begin
    for cycle = 1:4
        with_worker() do worker
            # No probe or other RPC precedes this signal. Reusing the callback
            # over several fresh children also exercises its already compiled path.
            M.interrupt(worker)
            nonce = UInt64(0x3141592653589700) + UInt64(cycle)
            first_payload = (nonce, UInt8[0x00, 0xff, 0x51, 0x73])
            first_promise = M.remote_call(identity, worker, first_payload)
            await_task(first_promise)
            if istaskfailed(first_promise)
                # POSIX delivery is asynchronous: one real request may receive
                # this signal. Admit that precise error once, never a terminated
                # worker, arbitrary remote failure, or an unbounded retry.
                remote_errors = filter(
                    error -> error isa M.RemoteException,
                    task_exceptions(first_promise),
                )
                @test length(remote_errors) == 1
                @test !has_task_exception(first_promise, M.TerminatedWorkerException)
                if length(remote_errors) == 1
                    @test occursin("InterruptException", only(remote_errors).message)
                end
            else
                @test fetch(first_promise) == first_payload
            end
            alive = M.isrunning(worker)
            @test alive
            readiness_rendezvous(worker, nonce)
            second_payload = (nonce + UInt64(1), UInt8[0x7f, 0x01, 0x00, 0xfe])
            @test M.remote_call_fetch(identity, worker, second_payload) == second_payload
            @test isempty(worker.expected_replies)
            report = M.shutdown(worker; timeout = 20.0)
            assert_complete(worker, report)
            @test report.exitcode == 0 && report.termsignal == 0
        end
    end
end

@testset "Abnormal process status remains visible after complete cleanup" begin
    with_worker(; monitor_stdout = false, monitor_stderr = false) do worker
        M.remote_do(Base.exit, worker, 17)
        await_task(@async wait(worker.proc))
        report = M.shutdown(worker; timeout = 20.0)
        assert_complete(worker, report; allow_errors = true)
        @test report.exitcode == 17
        @test report.termsignal == 0
    end
    with_worker(; monitor_stdout = false, monitor_stderr = false) do worker
        Base.kill(worker.proc, Base.SIGKILL)
        report = M.shutdown(worker; timeout = 20.0)
        # A request can race the already-delivered fatal signal and record a
        # failed send_exit. Keep that error; resource completeness is independent.
        assert_complete(worker, report; allow_errors = true)
        if Sys.isunix()
            @test report.termsignal == Int(Base.SIGKILL)
        else
            @test_skip "The POSIX signal number is qualified on Unix only"
        end
    end
end

@testset "Environment overrides and Linux session identity are observed" begin
    withenv(
        "MALT_TEST_INHERITED_SENTINEL" => "parent-nonsensitive-value",
        "MALT_TEST_OVERRIDE_SENTINEL" => "parent-before-override",
    ) do
        with_worker(; env = ["MALT_TEST_OVERRIDE_SENTINEL=child-explicit-value"]) do worker
            observed = M.remote_eval_fetch(
                worker,
                quote
                    (
                        ENV["MALT_TEST_INHERITED_SENTINEL"],
                        ENV["MALT_TEST_OVERRIDE_SENTINEL"],
                        Base.active_project(),
                        getpid(),
                    )
                end,
            )
            @test observed[1] == "parent-nonsensitive-value"
            @test observed[2] == "child-explicit-value"
            @test realpath(observed[3]) == realpath(Base.active_project())
            @test observed[4] == worker.proc_pid
            if Sys.islinux()
                manager_pid = getpid()
                manager_group = ccall(:getpgid, Cint, (Cint,), manager_pid)
                manager_session = ccall(:getsid, Cint, (Cint,), manager_pid)
                worker_group = ccall(:getpgid, Cint, (Cint,), worker.proc_pid)
                worker_session = ccall(:getsid, Cint, (Cint,), worker.proc_pid)
                remote_identity = M.remote_eval_fetch(
                    worker,
                    quote
                        (ccall(:getpgid, Cint, (Cint,), 0), ccall(:getsid, Cint, (Cint,), 0))
                    end,
                )
                @test manager_group > 0 && manager_session > 0
                @test worker_group == worker.proc_pid && worker_session == worker.proc_pid
                @test worker_group != manager_group && worker_session != manager_session
                @test remote_identity == (worker_group, worker_session)
            else
                # Cmd(detach=true) is requested on every OS; these concrete
                # getpgid/getsid observations qualify Linux only.
                @test_skip "Linux process-group/session observation is not available on this OS"
            end
        end
    end
end

@testset "Worker displays do not expose inherited environment" begin
    sentinel_name = "MALT_TEST_DISPLAY_SENTINEL"
    sentinel_value = "public-nonsensitive-worker-display-sentinel"
    withenv(sentinel_name => sentinel_value) do
        with_worker() do worker
            rendered = (
                repr(worker),
                repr(worker.state),
                repr(MIME"text/plain"(), worker),
                repr(MIME"text/plain"(), worker.state),
            )
            # Assertions see only scalar booleans; even a regression must not
            # print a captured Process/Worker or its real inherited environment.
            hides_name = all(text -> !occursin(sentinel_name, text), rendered)
            hides_value = all(text -> !occursin(sentinel_value, text), rendered)
            shows_identity = all(text -> occursin(string(worker.proc_pid), text), rendered)
            @test hides_name
            @test hides_value
            @test shows_identity
        end
    end
end

function closed_worker_weakref()
    worker = new_worker()
    process = worker.proc
    observed_tasks = tasks(worker)
    report = M.shutdown(worker; timeout = 20.0)
    assert_complete(worker, report)
    return WeakRef(worker), process, observed_tasks
end

@testset "Closed workers are not retained by process-exit hooks" begin
    handles = [closed_worker_weakref() for _ = 1:3]
    status = timedwait(10.0; pollint = 0.05) do
        GC.gc()
        all(item -> item[1].value === nothing, handles)
    end
    @test status == :ok
    collected = all(item -> item[1].value === nothing, handles)
    exited = all(item -> !Base.process_running(item[2]), handles)
    joined = all(item -> all(istaskdone, item[3]), handles)
    registry_empty = isempty(M.__iNtErNaL_get_running_procs())
    @test collected
    @test exited
    @test joined
    @test registry_empty
end

function active_worker_observation()
    worker = new_worker()
    @test M.remote_call_fetch(identity, worker, UInt32(0x12345678)) == 0x12345678
    return (
        WeakRef(worker),
        worker.proc,
        tasks(worker),
        (worker.current_socket, worker.stdout, worker.stderr),
        Int(worker.proc_pid),
    )
end

@testset "Dropping an active handle requests finalizer cleanup" begin
    weak, process, owned_tasks, handles, pid = active_worker_observation()
    try
        status = timedwait(20.0; pollint = 0.05) do
            GC.gc()
            weak.value === nothing &&
                !Base.process_running(process) &&
                all(istaskdone, owned_tasks) &&
                all(handle -> !isopen(handle), handles)
        end
        @test status == :ok
        collected = weak.value === nothing
        exited = !Base.process_running(process)
        joined = all(istaskdone, owned_tasks)
        closed = all(handle -> !isopen(handle), handles)
        registry_empty = isempty(M.__iNtErNaL_get_running_procs())
        @test collected
        @test exited
        @test joined
        @test closed
        @test registry_empty
        Sys.islinux() && @test !ispath("/proc/$pid")
    finally
        # Safety cleanup never changes the verdict recorded above.
        if Base.process_running(process)
            Base.kill(process, Base.SIGKILL)
            await_task(@async wait(process))
        end
        foreach(handle -> isopen(handle) && close(handle), handles)
    end
end

function weak_observation(worker)
    return (
        weak = WeakRef(worker),
        process = worker.proc,
        state = worker.state,
        handles = (worker.current_socket, worker.stdout, worker.stderr),
        pid = Int(worker.proc_pid),
    )
end

function state_tasks_done(state)
    lock(state.lock) do
        all(entry -> istaskdone(last(entry)), state.tasks) &&
            (state.cleanup_task === nothing || istaskdone(state.cleanup_task))
    end
end

function assert_collected_and_closed(observation)
    try
        status = timedwait(20.0; pollint = 0.05) do
            GC.gc()
            observation.weak.value === nothing &&
                !Base.process_running(observation.process) &&
                state_tasks_done(observation.state) &&
                all(handle -> !isopen(handle), observation.handles)
        end
        @test status == :ok
        # Do not hand Worker/Process/State operands to Test. Besides making
        # diagnostics unsafe, captured evaluation data can root a frontend
        # during the very GC observation under test.
        collected = observation.weak.value === nothing
        exited = !Base.process_running(observation.process)
        joined = state_tasks_done(observation.state)
        closed = all(handle -> !isopen(handle), observation.handles)
        pending_empty = isempty(observation.state.expected_replies)
        registry_empty = isempty(M.__iNtErNaL_get_running_procs())
        @test collected
        @test exited
        @test joined
        @test closed
        @test pending_empty
        @test registry_empty
        Sys.islinux() && @test !ispath("/proc/$(observation.pid)")
    finally
        if Base.process_running(observation.process)
            Base.kill(observation.process, Base.SIGKILL)
            await_task(@async wait(observation.process))
        end
        foreach(handle -> isopen(handle) && close(handle), observation.handles)
    end
end

function temporary_sync_rpc(port, payload, observation)
    worker = new_worker()
    observation[] = weak_observation(worker)
    return M.remote_eval_fetch(worker, gated_expression(port, payload))
end

function temporary_async_rpc(port, payload, observation)
    worker = new_worker()
    observation[] = weak_observation(worker)
    return M.remote_eval(worker, gated_expression(port, payload))
end

function complete_temporary_rpc(kind)
    server = Sockets.listen(ip"127.0.0.1", 0)
    port = Int(last(Sockets.getsockname(server)))
    accepted = @async Sockets.accept(server)
    observation = Ref{Any}(nothing)
    payload = (kind, UInt64(0x2718281828459045), UInt8[0x00, 0x51, 0xff, 0x7f])
    promise =
        kind === :synchronous ? @async(temporary_sync_rpc(port, payload, observation)) :
        temporary_async_rpc(port, payload, observation)
    control = nothing
    completed = false
    try
        control = fetch(await_task(accepted))
        @test fetch(await_task(@async read(control, UInt8))) == 0x51
        present = observation[] !== nothing
        @test present
        for _ = 1:3
            GC.gc()
            yield()
            owner_alive = observation[].weak.value isa M.Worker
            process_alive = Base.process_running(observation[].process)
            promise_pending = !istaskdone(promise)
            @test owner_alive
            @test process_alive
            @test promise_pending
        end
        write(control, UInt8(0x72))
        flush(control)
        @test fetch(await_task(promise)) == payload
        completed = true
        return observation[]
    finally
        close(server)
        control === nothing || close(control)
        if !completed &&
           observation[] !== nothing &&
           Base.process_running(observation[].process)
            Base.kill(observation[].process, Base.SIGKILL)
            await_task(@async wait(observation[].process))
        end
    end
end

@testset "Active synchronous and asynchronous RPCs retain their temporary owner" begin
    for kind in (:synchronous, :asynchronous)
        # All frontend handles, promises and Test evaluation temporaries belong
        # to the returned function frame. Only weak/OS/resource observations
        # cross this boundary before finalizer collection is tested.
        observation = complete_temporary_rpc(kind)
        assert_collected_and_closed(observation)
    end
end

function failed_promise_observation()
    worker = new_worker()
    observation = weak_observation(worker)
    promise = M.remote_eval(worker, :(error("deliberate_gc_remote_exception")))
    await_task(promise)
    @test has_task_exception(promise, M.RemoteException)
    caught = try
        fetch(promise)
        nothing
    catch error
        error
    end
    @test caught isa TaskFailedException
    @test occursin("deliberate_gc_remote_exception", sprint(showerror, caught))
    # Neither the user-visible error nor its failed promise leaves this scope.
    return observation
end

@testset "Abandoned RemoteException promises do not retain their Worker" begin
    observation = failed_promise_observation()
    assert_collected_and_closed(observation)
end

@testset "Known idle interrupts exclude readiness and completed application calls" begin
    with_worker() do worker
        # Constructor readiness is internal traffic, not a remote application
        # invocation. No public RPC precedes the first observed idle interrupt.
        @test isempty(remote_calls_snapshot(worker))
        @test traced_interrupt(worker, :idle_new) === nothing
        @test isempty(remote_calls_snapshot(worker))
        first_payload = (UInt64(0x0123456789abcdef), UInt8[0x00, 0xff, 0x31, 0x77])
        @test M.remote_call_fetch(identity, worker, first_payload) == first_payload
        @test isempty(worker.expected_replies)
        @test isempty(remote_calls_snapshot(worker))
        @test traced_interrupt(worker, :idle_after_result) === nothing
        second_payload = (UInt64(0xfedcba9876543210), UInt8[0x10, 0x71, 0xfe, 0x00])
        @test M.remote_call_fetch(identity, worker, second_payload) == second_payload
        @test isempty(remote_calls_snapshot(worker))
        assert_complete(worker, M.shutdown(worker; timeout = 20.0))
    end
end

@testset "Cancelling a waiter does not forget its running remote invocation" begin
    with_worker() do worker
        control, promise = control_rpc(worker, UInt64(0x7733115599bbddff))
        id = worker.current_message_id
        try
            @test remote_calls_snapshot(worker) == Set([id])
            @test length(worker.expected_replies) == 1
            schedule(promise, InterruptException(); error = true)
            await_task(promise)
            @test has_task_exception(promise, InterruptException)
            @test isempty(worker.expected_replies)
            @test remote_calls_snapshot(worker) == Set([id])
            # There is deliberately no new RPC here: a probe could become the
            # latest request and conceal loss of the abandoned remote call.
            @test traced_interrupt(worker, :active_cancelled_waiter) === nothing
            @test fetch(await_task(@async eof(control)))
            @test timedwait(
                () -> isempty(remote_calls_snapshot(worker)),
                20.0;
                pollint = 0.01,
            ) == :ok
            payload = (UInt64(0x1020304050607080), UInt8[0x81, 0x00, 0x51, 0xff])
            @test M.remote_call_fetch(identity, worker, payload) == payload
            @test isempty(worker.expected_replies)
            @test isempty(remote_calls_snapshot(worker))
            assert_complete(worker, M.shutdown(worker; timeout = 20.0))
        finally
            close(control)
        end
    end
end

@testset "remote_do remains active until its real FIN after success or failure" begin
    @test M.MsgType.from_worker_call_finished === UInt8(82)
    with_worker() do worker
        for fail in (false, true)
            server = Sockets.listen(ip"127.0.0.1", 0)
            port = Int(last(Sockets.getsockname(server)))
            accepted = @async Sockets.accept(server)
            control = nothing
            result = M.remote_do(
                Core.eval,
                worker,
                Main,
                quote
                    control = Sockets.connect(Sockets.IPv4(0x7f000001), $port)
                    try
                        write(control, UInt8(0x51))
                        flush(control)
                        read(control, UInt8)
                        $fail && error("deliberate_remote_do_fin_failure")
                        nothing
                    finally
                        close(control)
                    end
                end,
            )
            id = worker.current_message_id
            try
                @test result === nothing
                control = fetch(await_task(accepted))
                @test fetch(await_task(@async read(control, UInt8))) == 0x51
                @test remote_calls_snapshot(worker) == Set([id])
                @test isempty(worker.expected_replies)
                @test isempty(worker._reply_handles)
                write(control, UInt8(0x74))
                flush(control)
                @test fetch(await_task(@async eof(control)))
                @test timedwait(
                    () -> isempty(remote_calls_snapshot(worker)),
                    20.0;
                    pollint = 0.01,
                ) == :ok
                phase = fail ? :idle_after_failed_do_fin : :idle_after_do_fin
                @test traced_interrupt(worker, phase) === nothing
                payload = (UInt64(fail ? 0x77 : 0x33), UInt8[0x00, 0x61, 0xfe, 0xff])
                @test M.remote_call_fetch(identity, worker, payload) == payload
                @test isempty(remote_calls_snapshot(worker))
            finally
                close(server)
                control === nothing || close(control)
            end
        end
        assert_complete(worker, M.shutdown(worker; timeout = 20.0))
    end
end

# This type exists only in the host test module. Its complete serialization is
# legal, but a fresh worker cannot resolve that module while decoding the frame.
struct HostOnlyDecodeRecord
    nonce::UInt64
end

@testset "A rejected remote_do frame still delivers FIN without running application code" begin
    with_worker() do worker
        M.remote_eval_wait(worker, :(global lifecycle_decode_application_count = 0))
        expression = quote
            global lifecycle_decode_application_count += 1
            $(QuoteNode(HostOnlyDecodeRecord(UInt64(0xabcdeffedcba9876))))
            nothing
        end
        @test M.remote_do(Core.eval, worker, Main, expression) === nothing
        @test timedwait(
            () -> isempty(remote_calls_snapshot(worker)),
            20.0;
            pollint = 0.01,
        ) == :ok
        @test traced_interrupt(worker, :idle_after_rejected_do_fin) === nothing
        @test M.remote_eval_fetch(worker, :lifecycle_decode_application_count) == 0
        payload = (UInt64(0x7766554433221100), UInt8[0x11, 0x22, 0xff, 0x00])
        @test M.remote_call_fetch(identity, worker, payload) == payload
        @test isempty(worker.expected_replies)
        @test isempty(remote_calls_snapshot(worker))
        assert_complete(worker, M.shutdown(worker; timeout = 20.0))
    end
end

function lowlevel_identity(worker, payload; application = true)
    message = M._new_call_msg(true, identity, (payload,), pairs((;)))
    if application
        id = M._send_msg(worker, M.MsgType.from_host_call_with_response, message, true)
        cell = lock(worker.state.lock) do
            worker._reply_handles[id]
        end
        return id, cell
    end
    return M._send_message!(
        worker.state,
        M.MsgType.from_host_call_with_response,
        message,
        true;
        application = false,
    )
end

@testset "A received low-level response survives child exit until its two-argument wait" begin
    with_worker(; monitor_stdout = false, monitor_stderr = false) do worker
        payload = UInt8[mod(23i + 17, 256) for i = 1:65543]
        id, cell = lowlevel_identity(worker, payload)
        @test timedwait(() -> isready(cell), 20.0; pollint = 0.01) == :ok
        @test length(worker._reply_handles) == 1
        @test !haskey(worker.expected_replies, id)
        @test !(id in remote_calls_snapshot(worker))
        Base.kill(worker.proc, Base.SIGKILL)
        await_task(@async wait(worker.proc))
        @test isready(cell)
        @test haskey(worker._reply_handles, id)
        @test M._wait_for_response(worker, id) == payload
        @test isempty(worker._reply_handles)
        @test isempty(worker.expected_replies)
        @test_throws M.TerminatedWorkerException M._wait_for_response(worker, id)
        assert_complete(worker, M.shutdown(worker; timeout = 20.0); allow_errors = true)
    end
end

function lowlevel_cached_observation()
    worker = new_worker()
    payload = UInt8[mod(47i + 13, 256) for i = 1:4099]
    id, cell = lowlevel_identity(worker, payload)
    @test timedwait(() -> isready(cell), 20.0; pollint = 0.01) == :ok
    @test !haskey(worker.expected_replies, id)
    @test haskey(worker._reply_handles, id)
    @test isempty(remote_calls_snapshot(worker))
    received = fetch(cell).value
    @test received == payload
    distinct_payload = received !== payload
    @test distinct_payload
    return weak_observation(worker), WeakRef(received)
end

@testset "An abandoned frontend reply cache does not retain its Worker or result in State" begin
    observation, result = lowlevel_cached_observation()
    assert_collected_and_closed(observation)
    status = timedwait(10.0; pollint = 0.05) do
        GC.gc()
        result.value === nothing
    end
    collected_result = result.value === nothing
    @test status == :ok
    @test collected_result
end

@testset "Real readiness ACK and publication reject expired budgets with rollback" begin
    with_worker() do worker
        nonce = UInt64(0xcafef00d12345678)
        id, cell = lowlevel_identity(worker, nonce; application = false)
        @test timedwait(() -> isready(cell), 20.0; pollint = 0.01) == :ok
        @test isempty(remote_calls_snapshot(worker))
        @test isempty(worker._reply_handles)
        # A genuine typed identity reply is already available. The independent
        # monotonic deadline is in the past; no clock function is replaced.
        budget = UInt64(1_000_000)
        started = time_ns() - UInt64(10_000_000)
        @test time_ns() - started > budget
        @test_throws ErrorException M._accept_startup_reply!(
            worker.state,
            id,
            cell,
            nonce,
            started,
            budget,
        )
        @test !isready(cell)
        @test isempty(worker.expected_replies)
    end
    with_worker() do worker
        ready_task = only(
            last(entry) for entry in worker.state.tasks if first(entry) === :startup_ready
        )
        @test istaskdone(ready_task) && !istaskfailed(ready_task)
        @test isempty(remote_calls_snapshot(worker))
        budget = UInt64(1_000_000)
        started = time_ns() - UInt64(10_000_000)
        @test time_ns() - started > budget
        rejected = try
            M._complete_worker_startup!(worker, ready_task, started, budget)
            nothing
        catch error
            error
        end
        @test rejected isa M.WorkerStartupException
        if rejected isa M.WorkerStartupException
            @test occursin("readiness timed out", sprint(showerror, rejected.cause))
            assert_complete(worker, rejected.report; allow_errors = true)
            if Sys.isunix()
                @test rejected.report.termsignal == Int(Base.SIGKILL)
            else
                @test rejected.report.stage === :kill
            end
        end
    end
end

end
