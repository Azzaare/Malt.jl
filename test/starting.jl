

@testset "Starting exceptions" begin
    # A child that exits before publishing a port must fail construction. There is
    # no required thirty-second delay: connect_timeout is an explicit upper bound
    # for the connection phase, and lifecycle_contracts.jl checks owned cleanup.
    failure = try
        m.Worker(; exeflags = ["-t invalid"], connect_timeout = 2.0)
        nothing
    catch error
        error
    end
    @test failure isa m.WorkerStartupException
    @test failure.cause isa Exception
    @test occursin("exited before we could connect", sprint(showerror, failure))
    @test occursin(r"threads?|-t"i, sprint(showerror, failure))
    @test failure.report.complete
    @test failure.report.process_exited && failure.report.tasks_done
end
