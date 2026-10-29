import Malt as m
using Test


v() = @assert isempty(m.__iNtErNaL_get_running_procs())

v()
include("interrupt.jl")
v()
include("basic.jl")
v()
include("exceptions.jl")
v()
include("nesting.jl")
v()
include("starting.jl")
v()
include("stdout.jl")
v()
include("lifecycle_contracts.jl")
v()
if get(ENV, "MALT_RUN_BENCHMARKS", "false") == "true"
    include("benchmark.jl")
else
    @info "Performance campaign excluded from functional tests; set MALT_RUN_BENCHMARKS=true to run test/benchmark.jl explicitly."
end
v()
