-module(sqeache_client).

-export([
         % for REPL convenience:
         exec/1, exec/2,
         stat/1, stat/2, stat/3, stat/4,
         sel/1, sel/2, sel/3, sel/4,
         % actual interface
         statement/2, statement/3, statement/4, statement/5,
         select/2, select/3, select/4, select/5,
         execute/2, execute/3
        ]).

% The timeout for opening a socket and sending data
% is kept very low so we can quickly fail over if we
% don't get a good response.
-define(COMM_TIMEOUT, 250).
-define(RECV_TIMEOUT, 10000).

exec(Statement) -> execute(any, Statement).
exec(Statement, Args) -> execute(any, Statement, Args).
stat(Statement) -> statement(any, Statement).
stat(Statement, Args) -> statement(any, Statement, Args).
stat(Statement, Args, XFN) -> statement(any, Statement, Args, XFN).
stat(Statement, Args, XFN, XFA) -> statement(any, Statement, Args, XFN, XFA).

sel(Statement) -> select(any, Statement).
sel(Statement, Args) -> select(any, Statement, Args).
sel(Statement, Args, XF) -> select(any, Statement, Args, XF).
sel(Statement, Args, XF, XFA) -> select(any, Statement, Args, XF, XFA).



% TODO maybe just use execute and run parse xforms if any locally?
select(DbId, Statement) ->
    do(DbId, select, Statement, [], identity, []).

select(DbId, Statement, Args) ->
    do(DbId, select, Statement, Args, identity, []).

select(DbId, Statement, Args, XFormName) ->
    do(DbId, select, Statement, Args, XFormName, []).

select(DbId, Statement, Args, XFormName, XFormArgs) ->
    do(DbId, select, Statement, Args, XFormName, XFormArgs).

statement(DbId, Statement) ->
    do(DbId, statement, Statement, [], identity, []).

statement(DbId, Statement, Args) ->
    do(DbId, statement, Statement, Args, identity, []).

statement(DbId, Statement, Args, XFormName) ->
    do(DbId, statement, Statement, Args, XFormName, []).

statement(DbId, Statement, Args, XFormName, XFormArgs) ->
    do(DbId, statement, Statement, Args, XFormName, XFormArgs).


execute(DbId, Statement) ->
    do(DbId, execute, Statement, [], none, none).

execute(DbId, Statement, Args) ->
    do(DbId, excute, Statement, Args, none, none).


do(DbId, Type, Statement, Args, XFormName, XFormArgs) ->

    % Nope, we're not even pooling or keeping the sockets alive for now..
    % ... one step at a time, let's prove the concept before optimizing...
    % Note both our send and connect timeouts are set to 250 ms -
    % these operations should always be fast, and if they fail we need to know this
    % and (ultimately) terminate or otherwise permit a retry against a differnt host.
    % escon note: support round-robin responses where response type is defined as
    % 'one_of' the resolved value. {data_service_host, etcd_resolver, "/data/service/host", [roundrobin]}
    %
    %% note we'll need to be a proc for get_with_watch to be helpful...
    % { Addr, Port }  = escon:get_with_watch(data_service_host),
    Addr = {127,0,0,1},
    Port = 6543,
    Sock = gen_tcp:connect(Addr, Port,
                           [binary, {active, false}, {packet, 0},
                            {send_timeout, ?COMM_TIMEOUT}], ?COMM_TIMEOUT),
    Term = to_term(DbId, Type, Statement, Args, XFormName, XFormArgs),
    Response = maybe_send_and_receive(Sock, Term),
    maybe_close(Sock),
    Response.

to_term(DbId, execute, Statement, Args, _, _) ->
    {DbId, execute, Statement, Args};
to_term(DbId, Type, Statement, Args, XFormName, XFormArgs) ->
    {DbId, Type, Statement, Args, XFormName, XFormArgs}.

maybe_send_and_receive({error, Failure}, _Term) ->
    {error, {socket_open_failed, Failure}};
maybe_send_and_receive({ok, Sock}, Term) ->
    SendReply = gen_tcp:send(Sock, term_to_binary(Term, [compressed])),
    maybe_receive(Sock, SendReply).

maybe_receive(_Sock, {error, Failure}) ->
    {error, {socket_send_failed, Failure}};
maybe_receive(Sock, _) ->
    reply(gen_tcp:recv(Sock, 0, ?RECV_TIMEOUT)).

reply({ok, ValidResponse}) when is_binary(ValidResponse) ->
    % right now the intent is to get back the raw reply, effectively as
    % sqerl has processed it. We will probably want to wrap that in something
    % meaningful?
    binary_to_term(ValidResponse);
reply({error, Failure}) ->
    {error, {socket_receive_failed, Failure}}.

maybe_close({error, _Any}) ->
    ok;
maybe_close({ok, Sock}) ->
    gen_tcp:close(Sock).

