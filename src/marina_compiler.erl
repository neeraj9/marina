-module(marina_compiler).

-export([
    ring/1
]).

-spec ring([{{integer(), integer()}, binary()}]) ->
    ok.

% start-exclusive and end-inclusive
% [{{200, -100}, node3}, {{-100, -50}, node1}, {{-50, 0}, node2}, {{0, 100}, node1}, {{100, 200}, node2}]

ring(Ring) ->
    Forms = ring_forms(Ring),
    compile_and_load_forms(Forms),
    ok.

%% private
compile_and_load_forms(Forms) ->
    {ok, Module, Bin} = compile:forms(Forms, [debug_info]),
    code:soft_purge(Module),
    Filename = atom_to_list(Module) ++ ".erl",
    {module, Module} = code:load_binary(Module, Filename, Bin).

lookup_clause(Range, HostId) ->
    Patterns = [erl_syntax:variable('Token')],
    Guard = range_guard(Range),
    Body = [erl_syntax:atom(node_id(HostId))],
    erl_syntax:clause(Patterns, Guard, Body).

range_guard({undefined, End}) ->
    Var = erl_syntax:variable('Token'),
    Guard = erl_syntax:infix_expr(Var, erl_syntax:operator('=<'),
        erl_syntax:integer(End)),
    [Guard];
range_guard({Start, undefined}) ->
    Var = erl_syntax:variable('Token'),
    Guard = erl_syntax:infix_expr(Var, erl_syntax:operator('>'),
        erl_syntax:integer(Start)),
    [Guard];
range_guard({Start, End}) ->
    Var = erl_syntax:variable('Token'),
    Guard1 = erl_syntax:infix_expr(Var, erl_syntax:operator('>'),
        erl_syntax:integer(Start)),
    Guard2 = erl_syntax:infix_expr(Var, erl_syntax:operator('=<'),
        erl_syntax:integer(End)),
    [Guard1, Guard2].

lookup_clauses(Ring) ->
    lookup_clauses(Ring, []).

lookup_clauses([], Acc) ->
    lists:reverse(Acc);
lookup_clauses([{Range, HostId} | T], Acc) ->
    lookup_clauses(T, [lookup_clause(Range, HostId) | Acc]).

node_id(HostId) ->
    HostId2 = marina_utils:uuid_to_string(HostId),
    list_to_atom("marina_" ++ HostId2).

ring_forms(Ring) ->
    Module = erl_syntax:attribute(erl_syntax:atom(module),
        [erl_syntax:atom(marina_ring)]),
    ExportList = [erl_syntax:arity_qualifier(erl_syntax:atom(lookup),
        erl_syntax:integer(1))],
    Export = erl_syntax:attribute(erl_syntax:atom(export),
        [erl_syntax:list(ExportList)]),
    Function = erl_syntax:function(erl_syntax:atom(lookup),
        lookup_clauses(Ring)),
    Mod = [Module, Export, Function],
    [erl_syntax:revert(X) || X <- Mod].
