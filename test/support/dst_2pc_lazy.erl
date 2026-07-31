-module(dst_2pc_lazy).

%% A module that exists only to be *absent*.
%%
%% `modules_loaded` in a run result reports code loaded while the scheduler was
%% in charge, which is a real-clock dependency and the cause of the "first run
%% in a fresh VM diverges" failure. Testing that detector needs a module the
%% run will demand-load. Anything the harness touches from `init/2` is loaded
%% before the snapshot is taken, so it has to be something reached later.
%%
%% `dst_2pc` calls this from `execute/2` when `config.lazy` is set, and the test
%% purges it first. See `maybe_touch_lazy/2` there for why not from inside a
%% scheduled process, which is the version that actually breaks a seed.

-export([touch/0]).

-spec touch() -> ok.
touch() ->
    ok.
