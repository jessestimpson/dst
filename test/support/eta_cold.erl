-module(eta_cold).

%% A module that exists only to be *absent*, like `eta_2pc_lazy`.
%%
%% `eta_sched`'s `cold_code` reports a process still waiting on the code server
%% at the moment a run gave up believing itself quiescent. Testing that needs a
%% process parked in `code_server:call/1`, which needs a module that is not
%% loaded. This is it; the test purges it first. Reached through
%% `eta_cold_probe`, so the frame the detector reports is a stable MFA rather
%% than a test-local fun.

-export([touch/0]).

-spec touch() -> ok.
touch() ->
    ok.
