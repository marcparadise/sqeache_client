%% ex: ft=erlang ts=4 sw=4 et
%% -*- erlang-indent-level: 4;indent-tabs-mode: nil; fill-column: 92 -*-
-module('sqeache_client_app').

-behaviour(application).

-export([start/2,stop/1]).

start(_StartType, _StartArgs) ->
    {ok, Pid} = sqeache_client_sup:start_link(),
    {ok, PoolCfg} = application:get_env(sqeache_client, pool),
    % Some things we don't want configurable - pool name and start mfa in particular:
    Unconfigurable =[{start_mfa, {sqeache_client, start_link, []}},
                     {name, sqp}],
    {ok, _} = pooler:new_pool(lists:flatten([Unconfigurable, PoolCfg])),
    {ok, Pid}.

stop(_State) ->
    ok.
