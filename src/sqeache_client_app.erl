
-module('sqeache_client_app').

-behaviour(application).

-export([start/2,stop/1]).

start(_StartType, _StartArgs) ->
    sqeache_client_sup:start_link().
stop(_State) ->
    ok.
