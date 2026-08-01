-module(eta_observe).

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
`eta_transform`'s observability pass makes that automatic: a module declares what to expose and
every callback return republishes it, into the process dictionary, where
`erlang:process_info/2` can read it from outside — including while suspended, in a
couple of microseconds, regardless of mailbox depth.

```erlang
-ifdef(DST).
-compile({parse_transform, eta_transform}).
-endif.
-eta_observe({state, [leader, epoch]}).
```

```erlang
#{leader := L, epoch := E} = eta_observe:read(my_registry).
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
`-ifdef(DST)`, so a release build has no publishing in it at all. The `-eta_observe`
attribute itself is inert — an ordinary module attribute the compiler ignores.

## What to declare

Two forms, and neither guesses.

`{RecordName, Fields}` publishes those fields of that record as a map. **Name the
record.** It is not inferred, so a state record called `#st{}` works the same as
one called `#state{}`, and a field that does not exist is a compile error rather
than a silently wrong `element/2` offset. This is the form to prefer.

`all` publishes whatever the callback returned, whatever its shape, including a
map or a bare term. It costs nothing to write and makes every `read/1` copy the
entire state to the reader, which is usually the wrong trade: `read/1` is called
after every step of a simulation, and a state holding an inverted index or a
queue is not something to copy thousands of times.

There used to be a third form, a bare field list, which meant "these fields of
`#state{}`". It is gone. A module whose record was named anything else failed at
compile time complaining about a record it had never declared, and a reader had
to know the convention to see why.
""".
-endif.

-export([read/1, key/0]).

%% One fixed atom for every observed process — deliberately not derived from the
%% module or the process name, so this creates no atoms at runtime.
-define(KEY, '$eta_observed').

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
