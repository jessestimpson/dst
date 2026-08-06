-module(eta_cold_probe).

%% The warm half of the pair. This module is loaded; `eta_cold` is not, so the
%% call below goes through `error_handler:undefined_function/3` into the code
%% server — and the frame `eta_sched`'s `cold_code` should name is this one,
%% because it is the last frame before the loading machinery.
%%
%% Deliberately *not* a tail call. `touch() -> eta_cold:touch().` leaves no frame
%% for this module at all, and the detector then has nothing to report but the
%% process — which is a real limitation of reading a stack rather than a fault in
%% the test, and is why `eta_sched`'s `t:cold/0` says `at` may be `undefined`.

-export([touch/0]).

-spec touch() -> ok.
touch() ->
    _ = eta_cold:touch(),
    ok.
