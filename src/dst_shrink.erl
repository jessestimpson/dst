-module(dst_shrink).

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-if(?DOCATTRS).
-moduledoc """
Delta-debugging a failing trace down to one a human can read — Phase 4 of the DST
framework (design: `docs/design.md`).

A run that finds a violation hands back the trace that produced it, and for a real
system that is not a bug report: a three-member registry cluster produces around
1900 entries, of
which almost all are heartbeats and unrelated members making progress. Shrinking
searches for the shortest trace that still fails.

```erlang
#{outcome := {violation, _}, trace := Trace} = dst_run:run(my_sut, #{seed => 3}),
#{trace := Minimal} = dst_shrink:shrink(my_sut, Trace, #{seed => 3}).
```

## The oracle has three answers, not two

This is the part that decides whether a shrinker is any good.

A candidate trace is tested by replaying it, and the result is one of:

| Result | Meaning |
|---|---|
| the same violation | the removal was safe — keep the shorter trace |
| a clean run | the removed entries mattered — put them back |
| a *divergence* | the candidate is not a valid schedule; **no evidence either way** |

Conflating the last two is the classic way to build a shrinker that appears to work
and quietly discards the minimal case: a candidate that diverges gets recorded as
"still fails? no", so a removal that was in fact safe is reverted.

Here divergence is avoided rather than classified. `dst_run:replay/3` runs candidates
in `lenient` mode, where a step naming a process that is not currently runnable is
skipped instead of reported. That matters because entries are not independent:
removing an operation necessarily strands the steps belonging to the process it
spawned, so under a strict replay nearly every candidate would diverge and the
search would learn nothing.

## Why the result is re-recorded

A lenient replay is not a faithful one — it silently dropped entries — so the
shrunk *candidate* is not a trace anyone should be handed. What the run actually
executed is, and `dst_run` records exactly that. So the last step of a shrink is to
take the surviving candidate's own recorded trace and verify it **strictly**: a
minimal trace that does not replay is not a repro.

That verification is not a formality. It is the only thing standing between this
and a shrinker that reports a beautifully small trace nobody can reproduce.

## What it shrinks, and what it cannot

Both dimensions at once, since both are entries in the same list: operations
disappear entirely, and so do scheduling decisions. Removing an operation is the
larger win and usually happens first because ddmin works coarse-to-fine; what
survives at the end is close to a minimal interleaving.

**A shrunk trace is not an explanation, and this doc used to claim otherwise.**
It said the result was "nearly a proof sketch of the defect", which is true for
somebody who already knows the protocol and how ids were assigned, and false for
everybody else. `{step, 8}` names a position, not a process, and says nothing
about what that process did. Reading one means reconstructing every mailbox
state by hand.

What shrinking gives you is a trace short enough to be worth narrating.
`dst_log` is what narrates it: run the shrunk trace back through `replay/3` and
`dst_log:analyze/0` prints the schedule and the system's own behaviour on one
timeline, with processes named. Use both.

It cannot shrink below what the system needs to reach the violation at all, and it
does not try to simplify the *contents* of an operation (a smaller name, fewer
participants). That is a separate axis and a plausible extension.

**Step ids are positional, and that bounds how far operations can be removed.**
`dst_sched` assigns ids in registration order, so deleting an operation renumbers
every process created after it and the surviving `{step, Id}` entries stop naming
what they named. Measured on a shrunk 2PC trace: removing the leading — and
entirely irrelevant — operation left the run clean with six steps skipped, because
those six no longer referred to the client that mattered. So a leading operation
can survive shrinking despite contributing nothing, and did in that example.

The trace is still a genuine repro; it is the *minimality* that is approximate, and
knowing which is which matters when reading one. Closing it means remapping ids as
entries are removed, which is a real extension rather than a tuning change.
""".
-endif.

-export([shrink/3]).

-export_type([result/0]).

-type result() :: #{
    trace := [dst_run:entry()],
    original := non_neg_integer(),
    shrunk := non_neg_integer(),
    tests := non_neg_integer(),
    outcome := dst_run:outcome(),
    verified := boolean()
}.

-record(s, {
    mod :: module(),
    opts :: map(),
    match :: fun((dst_harness:violation()) -> boolean()),
    tests = 0 :: non_neg_integer(),
    budget :: non_neg_integer()
}).

%% ---------------------------------------------------------------------------
%% API
%% ---------------------------------------------------------------------------

-if(?DOCATTRS).
-doc """
Shrinks `Trace` to the shortest prefix-free subsequence that still violates.

`Opts` are `dst_run:run/2`'s, plus:

- `match` — a predicate on the violation deciding whether a candidate counts as
  "the same failure". The default is derived from the original: if the violation is
  a map carrying a `property` key, a candidate must report the *same* property;
  otherwise any violation counts.

  Overriding this matters for a system with more than one invariant, where the
  default's fallback would happily shrink one bug into a different one — a smaller
  trace for a failure you were not investigating, which is worse than no shrink
  because it looks like progress.

- `max_tests` (500) — bound on candidate replays. Shrinking is a search, and on a
  large trace it is the expensive part of a run. The default is **binding** on some
  traces: over 30 failing 2PC seeds, several exhausted it, meaning more reduction
  was available and not taken. Raise it when a result looks larger than it should.

Returns the minimal trace *as executed*, verified to replay strictly. `verified =>
false` means the search found something smaller but it did not survive strict
replay, and the reported trace is the original — see the module doc.
""".
-endif.
-spec shrink(module(), [dst_run:entry()], map()) -> result().
shrink(Mod, Trace, Opts) ->
    case classify(Mod, Trace, Opts) of
        {violation, Detail} ->
            S = #s{
                mod = Mod,
                opts = maps:remove(match, Opts),
                match = maps:get(match, Opts, default_match(Detail)),
                budget = maps:get(max_tests, Opts, 500)
            },
            {Minimal, S1} = ddmin(Trace, 2, S),
            finish(Trace, Minimal, S1);
        Other ->
            %% Nothing to shrink. Reporting it beats returning a "minimal" trace
            %% for a failure that did not happen.
            #{
                trace => Trace,
                original => length(Trace),
                shrunk => length(Trace),
                tests => 0,
                outcome => Other,
                verified => false
            }
    end.

%% The surviving candidate is only a *recipe*; the trace it actually executed is
%% the artefact, and it has to replay strictly or it is not a repro.
finish(Original, Minimal, S = #s{mod = Mod, opts = Opts}) ->
    Lenient = dst_run:replay(Mod, Minimal, Opts#{lenient => true}),
    Executed = maps:get(trace, Lenient),
    Strict = dst_run:replay(Mod, Executed, Opts#{lenient => false}),

    case matches(maps:get(outcome, Strict), S) of
        true ->
            #{
                trace => Executed,
                original => length(Original),
                shrunk => length(Executed),
                tests => S#s.tests,
                outcome => maps:get(outcome, Strict),
                verified => true
            };
        false ->
            #{
                trace => Original,
                original => length(Original),
                shrunk => length(Original),
                tests => S#s.tests,
                outcome => maps:get(outcome, Strict),
                verified => false
            }
    end.

%% ---------------------------------------------------------------------------
%% Delta debugging
%% ---------------------------------------------------------------------------

%% Standard ddmin: try ever-finer partitions, preferring a failing *subset* (a big
%% jump) over a failing *complement* (removing one chunk), and stop when the
%% granularity cannot increase.
ddmin(Trace, _N, S = #s{budget = B}) when B =< 0 ->
    {Trace, S};
ddmin(Trace, N, S) when length(Trace) >= 2 ->
    Chunks = partition(Trace, min(N, length(Trace))),
    case first_failing(Chunks, S) of
        {ok, Subset, S1} ->
            ddmin(Subset, 2, S1);
        {none, S1} ->
            Complements = [Trace -- C || C <- Chunks],
            case first_failing(Complements, S1) of
                {ok, Complement, S2} ->
                    ddmin(Complement, max(N - 1, 2), S2);
                {none, S2} when N < length(Trace) ->
                    ddmin(Trace, min(2 * N, length(Trace)), S2);
                {none, S2} ->
                    {Trace, S2}
            end
    end;
ddmin(Trace, _N, S) ->
    {Trace, S}.

first_failing([], S) ->
    {none, S};
first_failing([[] | Rest], S) ->
    first_failing(Rest, S);
first_failing([Candidate | Rest], S = #s{budget = B}) when B > 0 ->
    case still_fails(Candidate, S) of
        {true, S1} -> {ok, Candidate, S1};
        {false, S1} -> first_failing(Rest, S1)
    end;
first_failing(_, S) ->
    {none, S}.

still_fails(Candidate, S = #s{mod = Mod, opts = Opts, tests = T, budget = B}) ->
    Result = dst_run:replay(Mod, Candidate, Opts#{lenient => true}),
    S1 = S#s{tests = T + 1, budget = B - 1},
    {matches(maps:get(outcome, Result), S1), S1}.

matches({violation, Detail}, #s{match = Match}) -> Match(Detail);
matches(_, _) -> false.

%% Split into N roughly equal chunks, order preserved.
partition(List, N) ->
    Len = length(List),
    Size = max(1, Len div N),
    chunks(List, Size).

chunks([], _Size) ->
    [];
chunks(List, Size) when length(List) =< Size ->
    [List];
chunks(List, Size) ->
    {Head, Tail} = lists:split(Size, List),
    [Head | chunks(Tail, Size)].

%% ---------------------------------------------------------------------------
%% Matching
%% ---------------------------------------------------------------------------

classify(Mod, Trace, Opts) ->
    maps:get(outcome, dst_run:replay(Mod, Trace, Opts#{lenient => true})).

%% Same `property` when the system reports one, any violation otherwise. The
%% fallback is the honest default rather than a good one — see `shrink/3` on why a
%% multi-invariant system should supply `match`.
default_match(#{property := Property}) ->
    fun
        (#{property := P}) -> P =:= Property;
        (_) -> false
    end;
default_match(_) ->
    fun(_) -> true end.
