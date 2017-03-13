-module(marina).
-include("marina_internal.hrl").

-export([
    async_execute/4,
    async_execute/5,
    async_execute/6,
    async_prepare/2,
    async_query/4,
    async_reusable_query/5,
    async_reusable_query/6,
    execute/4,
    execute/5,
    prepare/2,
    query/4,
    query/5,
    receive_response/1,
    response/1,
    reusable_query/4,
    reusable_query/5
]).

%% public
-spec async_execute(statement_id(), consistency(), [flag()],
    pid()) -> {ok, reference()} | {error, backlog_full}.

async_execute(StatementId, ConsistencyLevel, Flags, Pid) ->
    async_execute(StatementId, [], ConsistencyLevel, Flags, Pid).

-spec async_execute(statement_id(), [value()], consistency(), [flag()],
    pid()) -> {ok, reference()} | {error, backlog_full}.

async_execute(StatementId, Values, ConsistencyLevel, Flags, Pid) ->
    async_execute(StatementId, Values, ConsistencyLevel, Flags, Pid,
        ?DEFAULT_TIMEOUT).

-spec async_execute(statement_id(), [value()], consistency(), [flag()],
    pid(), timeout()) -> {ok, reference()} | {error, backlog_full}.

async_execute(StatementId, Values, ConsistencyLevel, Flags, Pid, Timeout) ->
    async_call({execute, StatementId, Values, ConsistencyLevel, Flags},
        Pid, Timeout).

-spec async_prepare(query(), pid()) ->
    {ok, reference()} | {error, backlog_full}.

async_prepare(Query, Pid) ->
    async_call({prepare, Query}, Pid).

-spec async_query(query(), consistency(), [flag()], pid()) ->
    {ok, reference()} | {error, backlog_full}.

async_query(Query, ConsistencyLevel, Flags, Pid) ->
    async_query(Query, [], ConsistencyLevel, Flags, Pid).

-spec async_query(query(), [value()], consistency(), [flag()], pid()) ->
    {ok, reference()} | {error, backlog_full}.

async_query(Query, Values, ConsistencyLevel, Flags, Pid) ->
    async_call({query, Query, Values, ConsistencyLevel, Flags}, Pid).

-spec async_reusable_query(query(), consistency(), [flag()], pid(),
    timeout()) -> {ok, reference()} | {error, term()}.

async_reusable_query(Query, ConsistencyLevel, Flags, Pid, Timeout) ->
    async_reusable_query(Query, [], ConsistencyLevel, Flags, Pid, Timeout).

-spec async_reusable_query(query(), [value()], consistency(), [flag()], pid(),
    timeout()) -> {ok, reference()} | {error, term()}.

async_reusable_query(Query, Values, ConsistencyLevel, Flags, Pid, Timeout) ->
    case marina_cache:get(Query) of
        {ok, StatementId} ->
            async_execute(StatementId, Values, ConsistencyLevel, Flags, Pid,
                Timeout);
        {error, not_found} ->
            case prepare(Query, Timeout) of
                {ok, StatementId} ->
                    marina_cache:put(Query, StatementId),
                    async_execute(StatementId, Values, ConsistencyLevel, Flags,
                        Pid, Timeout);
                {error, Reason} ->
                    {error, Reason}
            end
    end.

-spec execute(statement_id(), consistency(), [flag()], timeout()) ->
    {ok, term()} | {error, term()}.

execute(StatementId, ConsistencyLevel, Flags, Timeout) ->
    execute(StatementId, [], ConsistencyLevel, Flags, Timeout).

-spec execute(statement_id(), [value()], consistency(), [flag()],
    timeout()) -> {ok, term()} | {error, term()}.

execute(StatementId, Values, ConsistencyLevel, Flags, Timeout) ->
    call({execute, StatementId, Values, ConsistencyLevel, Flags}, Timeout).

-spec prepare(query(), timeout()) ->
    {ok, term()} | {error, term()}.

prepare(Query, Timeout) ->
    call({prepare, Query}, Timeout).

-spec query(query(), consistency(), [flag()], timeout()) ->
    {ok, term()} | {error, term()}.

query(Query, ConsistencyLevel, Flags, Timeout) ->
    query(Query, [], ConsistencyLevel, Flags, Timeout).

-spec query(query(), [value()], consistency(), [flag()], timeout()) ->
    {ok, term()} | {error, term()}.

query(Query, Values, ConsistencyLevel, Flags, Timeout) ->
    call({query, Query, Values, ConsistencyLevel, Flags}, Timeout).

-spec receive_response(term()) ->
    {ok, term()} | {error, term()}.

receive_response(RequestId) ->
    response(shackle:receive_response(RequestId)).

-spec response({ok, term()} | {error, term()}) ->
    {ok, term()} | {error, term()}.

response({ok, Frame}) ->
    marina_body:decode(Frame);
response({error, Reason}) ->
    {error, Reason}.

-spec reusable_query(query(), consistency(), [flag()], timeout()) ->
    {ok, term()} | {error, term()}.

reusable_query(Query, ConsistencyLevel, Flags, Timeout) ->
    reusable_query(Query, [], ConsistencyLevel, Flags, Timeout).

-spec reusable_query(query(), [value()], consistency(), [flag()], timeout()) ->
    {ok, term()} | {error, term()}.

reusable_query(Query, Values, ConsistencyLevel, Flags, Timeout) ->
    Timestamp = os:timestamp(),
    case marina_cache:get(Query) of
        {ok, StatementId} ->
            Execute = execute(StatementId, Values, ConsistencyLevel, Flags,
                Timeout),
            case Execute of
                {error, {9472, _}} ->
                    marina_cache:erase(Query),
                    Timeout3 = marina_utils:timeout(Timeout, Timestamp),
                    reusable_query(Query, Values, ConsistencyLevel, Flags,
                        Timeout3);
                Response ->
                    Response
            end;
        {error, not_found} ->
            case prepare(Query, Timeout) of
                {ok, StatementId} ->
                    marina_cache:put(Query, StatementId),
                    Timeout2 = marina_utils:timeout(Timeout, Timestamp),
                    execute(StatementId, Values, ConsistencyLevel, Flags,
                        Timeout2);
                {error, Reason} ->
                    {error, Reason}
            end
    end.

%% private
async_call(Msg, Pid) ->
    async_call(Msg, Pid, ?DEFAULT_TIMEOUT).

async_call(Msg, Pid, Timeout) ->
    shackle:cast(?APP, Msg, Pid, Timeout).

call(Msg, Timeout) ->
    response(shackle:call(?APP, Msg, Timeout)).
