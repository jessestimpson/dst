-module(dst_2pc_lazy).

%% A module that exists only to be *absent*.
%%
%% `modules_loaded` in a run result reports code loaded while the scheduler was
%% in charge, which is a real-clock dependency and the cause of the "first run
%% in a fresh VM diverges" failure. Testing that detector needs a module the
%% run will demand-load, and it has to be reachable **only from a scheduled
%% process** — anything the harness touches from `init/2` is already loaded by
%% the time a run starts, which is exactly why that case is harmless and this
%% one is not.
%%
%% `dst_2pc` calls this from inside a client when `config.lazy` is set, and the
%% test purges it first. Nothing else uses it.

-export([touch/0]).

-spec touch() -> ok.
touch() ->
    ok.
