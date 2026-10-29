# Malt.jl

This SDK branch builds on upstream Malt 1.5.0 and adds an explicit worker
lifecycle API described below. Its changes are maintained in the
[SDK repository](https://gitlab.naze.baffier.fr/etendu3d/Malt.jl); the original
upstream project and history remain [JuliaPluto/Malt.jl](https://github.com/JuliaPluto/Malt.jl).

The SDK requires Julia 1.13. The package version `Malt 1.6.0` identifies this
library release; it does not declare support for the Julia 1.6 runtime.

Malt is a multiprocessing package for Julia. It is used by [Pluto.jl](https://plutojl.org/) to manage the Julia process that notebook code is executed in, as a [replacement to Distributed](https://github.com/fonsp/Pluto.jl/pull/2240).

The [upstream API manual](https://juliapluto.github.io/Malt.jl) describes the
original package. The SDK lifecycle additions are documented in this repository.

For shutdown deadlines, output ownership, pending RPCs and checking code/signal
before accepting a result, see the [worker lifecycle guide](doc/src/lifecycle.md).
The [verification scope and limits](doc/src/verification.md) distinguish the
functional tests, platform coverage and historical incidents.

```julia
julia> import Malt

julia> worker = Malt.Worker();

julia> Malt.remote_eval_fetch(worker, :(1 + 1))
2

julia> Malt.remote_eval_fetch(worker, :(rand(5))) |> sum
3.0618168580350966
```

Example of running code asynchronously, and interrupting the process:

```julia
julia> task = Malt.remote_eval(worker, :(sleep(100)))
Task (runnable) @0x0000023539e7f460

julia> Malt.interrupt(worker)

julia> wait(task)
ERROR: TaskFailedException
Stacktrace:
 ...
    nested task error: Remote exception from Malt.Worker on port 9584 with PID 17584:

    InterruptException:
    Stacktrace:
      ...

julia> Malt.stop(worker);

julia> Malt.isrunning(worker)
false
```

## **Malt.jl** vs **Distributed**

Malt.jl is inspired by the [`Distributed standard library`](https://docs.julialang.org/en/v1/stdlib/Distributed/), with a focus on managing isolated local Julia processes. Important differences are:


### API changes
Malt.jl has different function names, see our [**documentation**](https://juliapluto.github.io/Malt.jl).

One important addition is public API for **evaluating an `Expr`**: 

```julia
worker = Malt.Worker()
Malt.remote_eval_fetch(worker, :(sqrt(123)))
```

### Nested use
With Malt.jl, any **worker** process can also be a **host** process to its own workers. 

In Distributed, only "process 1 can add or remove workers". Malt.jl does not have this limitation. *This means that Malt.jl workers can use Distributed (and Malt.jl) like a regular Julia process.*

### Process boundaries and environment

A `Malt.Worker` has its own Julia address space. It runs with the same operating
system account and filesystem permissions as its manager; this is not a security
sandbox or an authentication protocol for untrusted RPC peers. Child processes
created by application code require their own lifecycle policy.

The launch command uses Julia's ordinary inherited environment, with
`OPENBLAS_NUM_THREADS=1` and the supplied `env` entries applied as overrides.
Do not use process separation as a way to hide parent environment variables.
The manager's active Pkg project and command-line flags are not automatically
copied into the child command. Select a project explicitly with `exeflags`, and
account for variables such as `JULIA_PROJECT` using the usual Julia rules.
Workers start with `--startup-file=no`.

`Malt.InProcessWorker` executes in the manager's process and provides no separate
address space. `Malt.DistributedStdlibWorker` delegates process management to
Distributed; guarantees specific to `Malt.Worker` do not automatically apply to it.

### Interrupt on Windows
Malt.jl supports **interrupting a worker process on Windows**, not just on UNIX.

### Homogenous computing
Malt.jl does not have API like `@everywhere` or `Distributed.procs`: Malt is **not the right tool for homogenous computing**.

### Exception handling
Exceptions in Malt.jl workers are converted to plaintext before being rethrown in the host. 

The original exception object is only available to the worker. In Distributed, the original exception object is serialized and rethrown to the host.

### Historical launch measurements

The following upstream comparison predates the lifecycle changes described in
the documentation. It is retained as historical context, not a qualification of
the current revision or a guarantee for another machine.

```
julia> @time Distributed.addprocs(1);
  2.064801 seconds (11.63 k allocations: 1.093 MiB, 1.08% compilation time)

julia> @time Malt.Worker();
  0.964955 seconds (537 allocations: 308.734 KiB)
```

### Limitations

In contrast to Distributed.jl, Malt.jl currently does not support launching workers on another machine (e.g. SSH remote workers).

# Sponsors

Development of Malt.jl is sponsored by:


| | |
|----|----|
| <a href="https://juliahub.com/"><img title="JuliaHub" src="https://i.imgur.com/IGdcVt7.png" width=200 alt="JuliaHub logo"></a> | [**JuliaHub**](https://juliahub.com) enables the creation and editing of Pluto notebooks *on the cloud*! |
| <a href="https://summerofcode.withgoogle.com/"><img title="Google Summer of Code" src="https://summerofcode.withgoogle.com/assets/media/logo.svg" width=200 alt="Google Summer of Code logo"></a> | [**Google Summer of Code 2022**](https://summerofcode.withgoogle.com/) allowed [Sergio A. Vargas](https://github.com/savq) to join us for a summer to develop Malt.jl! More details [here](https://github.com/savq/gsoc-2022). |

