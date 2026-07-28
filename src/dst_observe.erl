-module(dst_observe).

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-if(?DOCATTRS).
-moduledoc """
Reading a process's state **without asking it** — the observability half of the DST
framework (design: `docs/design.md`).

An invariant runs against a system the scheduler has frozen. Every process it owns
is suspended, so anything that sends a message and waits cannot be served. That
rules out the natural way to observe a `gen_server`, and it does so in the worst
possible manner: a client API that catches its own call timeout answers plausibly
rather than failing. A registry `status/1` that returns `undefined` on `exit:_`
leaves a split-brain invariant built on it computing over "no member believes it leads" and
passes, having checked nothing.

The fix is for the process to *publish* rather than be asked.
`dst_transform`'s observability pass makes that automatic: a module declares what to expose and
every callback return republishes it, into the process dictionary, where
`erlang:process_info/2` can read it from outside — including while suspended, in a
couple of microseconds, regardless of mailbox depth.

```erlang
-ifdef(DST).
-compile({parse_transform, dst_transform}).
-endif.
-dst_observe([leader, epoch]).
```

```erlang
#{leader := L, epoch := E} = dst_observe:read(my_registry).
```

## Why the process dictionary

It is the only place that satisfies all of the constraints at once. ETS needs a
table, and a table needs a name — deriving one per process grows the atom table
without bound, which no long-running system can afford, while sharing an
existing table means putting foreign rows in a structure that is often load-bearing
(the registry's names table *is* its replication payload). `persistent_term` writes
trigger a global scan. The process dictionary needs no name, belongs to exactly the
process being observed, dies with it, and costs ~7ns to write a term already on the
heap.

## Cost, and why it is acceptable

Publishing happens on **every** callback return, which is what makes staleness
impossible — there is no assignment site to forget, because the transform does not
track assignments. Measured at ~7ns per publish for a large state, because `put/2`
stores a reference rather than copying.

And it is absent from production entirely: the transform is applied under
`-ifdef(DST)`, so a release build has no publishing in it at all. The `-dst_observe`
attribute itself is inert — an ordinary module attribute the compiler ignores.

## What to declare

`-dst_observe(all)` publishes the whole state record, which costs nothing to write
but makes every `read/1` copy the entire state to the reader. Naming fields is
usually better: `read/1` is called after every step of a simulation, and a state
holding an inverted index or a queue is not something to copy thousands of times.
""".
-endif.

-export([read/1, key/0]).

%% One fixed atom for every observed process — deliberately not derived from the
%% module or the process name, so this creates no atoms at runtime.
-define(KEY, '$dst_observed').

-if(?DOCATTRS).
-doc """
What `Target` last published, or `undefined` if it has published nothing or is not
running.

Works on a suspended process: `erlang:process_info/2` reads the target's heap
rather than asking it anything. That is the entire point — see the module doc.
""".
-endif.
-spec read(pid() | atom()) -> term() | undefined.
read(Pid) when is_pid(Pid) ->
    case erlang:process_info(Pid, dictionary) of
        {dictionary, Dict} -> proplists:get_value(?KEY, Dict, undefined);
        undefined -> undefined
    end;
read(Name) when is_atom(Name) ->
    case whereis(Name) of
        undefined -> undefined;
        Pid -> read(Pid)
    end.

-if(?DOCATTRS).
-doc "The process-dictionary key the transform publishes under.".
-endif.
-spec key() -> atom().
key() ->
    ?KEY.
