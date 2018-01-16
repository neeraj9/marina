-module(marina_ring).
-include("marina_internal.hrl").

% ring(Token) when Token =< Token1; Token < Token2 ->
%     node1;
% ring(Token) when Token =< Token2; Token < Token3 ->
%     node2;
% ring(Token) when Token =< Token3; Token < Token4 ->
%     node1;
%
% ...
%
% ring(Token) when Token =< TokenN-1; Token < TokenN ->
%     node2;
