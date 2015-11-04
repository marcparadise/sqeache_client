-module(sqeache_client).

-export([execute/2,
         execute/3,
         execute/4,
         execute/5]).

% The timeout for opening a socket and sending data
% is kept very low so we can quickly fail over if we
% don't get a good response.
-define(COMM_TIMEOUT, 250).
-define(RECV_TIMEOUT, 10000).


execute(DbId, StatementName) ->
    execute(DbId, StatementName, [], identity, []).

execute(DbId, StatementName, Args) ->
    execute(DbId, StatementName, Args, identity, []).

execute(DbId, StatementName, Args, XFormName) ->
    execute(DbId, StatementName, Args, XFormName, []).

execute(DbId, StatementName, Args, XFormName, XFormArgs) ->

    % Nope, we're not even pooling or keeping the sockets alive for now..
    % ... one step at a time, let's prove the concept before optimizing...
    % Note both our send and connect timeouts are set to 250 ms -
    % these operations should always be fast, and if they fail we need to know this
    % and (ultimately) terminate or otherwise permit a retry against a differnt host.
    % escon note: support round-robin responses where response type is defined as
    % 'one_of' the resolved value. {data_service_host, etcd_resolver, "/data/service/host", [roundrobin]}
    %   -
    % { Addr, Port }  = escon:get_with_watch("data_service_host"), % note we'll need to be a proc for get_with_watch to be helpful...
    Addr = {127,0,0,1},
    Port = 6543,
    Sock = gen_tcp:connect(Addr, Port,
                           [binary, {active, false}, {packet, 0},
                            {send_timeout, ?COMM_TIMEOUT}], ?COMM_TIMEOUT),
    Response = maybe_send_and_receive(Sock, {DbId, StatementName, Args, XFormName, XFormArgs}),
    gen_tcp:close(Sock),
    Response.

maybe_send_and_receive({error, Failure}, _Term) ->
    {error, {socket_open_failed, Failure}};
maybe_send_and_receive(Sock, Term) ->
    SendReply = gen_tcp:send(Sock, term_to_binary(Term, [compressed])),
    maybe_receive(Sock, SendReply).

maybe_receive(_Sock, {error, Failure}) ->
    {error, {socket_send_failed, Failure}};
maybe_receive(Sock, _) ->
    reply(gen_tcp:recv(Sock, 0, ?RECV_TIMEOUT)).

reply(ValidResponse) when is_binary(ValidResponse) ->
    % right now the intent is to get back the raw reply, effectively as
    % sqerl has processed it. We will probably want to wrap that in something
    % meaningful?
    binary_to_term(ValidResponse);
reply(Error) ->
    {error, {socket_receive_failed, Error}}.

% Note app  well probably have a layer atop this, eg
% my_data:execute(StatementName) ->
%   my_data:execute(my_known_db_identifier, StatementName).
%
%
