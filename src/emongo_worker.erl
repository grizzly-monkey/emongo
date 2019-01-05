%%%----------------------------------------------------------------------
%%% File    : emongo_nif.erl
%%% @author : Jeet Parmar <jeet@glabbr.com>
%%% Purpose : Emongo poolboy worker.
%%% Created : 15 Dec 2018 by Jeet Parmar <jeet@glabbr.com>
%%%
%%% Copyright (C) 2002-2019 Glabbr India Pvt. Ltd. All Rights Reserved.
%%%
%%% Licensed under the GNU GPL License, Version 3.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%     https://www.gnu.org/licenses/gpl-3.0.en.html
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%%
%%%----------------------------------------------------------------------

-module(emongo_worker).

-author('jeet@glabbr.com').

-behaviour(gen_server).
-behaviour(poolboy_worker).

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).

-record(state, {id, conn}).
-define(DEFAULT_TENANT, default).


start_link(Args) ->
  gen_server:start_link(?MODULE, Args, []).

init(Args) ->
  process_flag(trap_exit, true),
  Uri = get_arg(uri, Args),
  TenantId = get_arg(tenant_id, Args),
  case emongo_nif:connect(Uri) of
    {ok, Conn} ->
      {ok, #state{id = TenantId, conn = Conn}};
    {error, Error} ->
      {error, Error}
  end.
handle_call({find_all, Collection}, _From, #state{conn = Conn} = State) ->
  {reply, emongo_nif:find_all(Conn, Collection), State};
handle_call({list_databases}, _From, #state{conn = Conn} = State) ->
  {reply, emongo_nif:list_databases(Conn), State};
handle_call({list_collections, DbName}, _From, #state{conn = Conn} = State) ->
  {reply, emongo_nif:list_collections(Conn, DbName), State};
handle_call({collection_exists, DbName, Collection}, _From, #state{conn = Conn} = State) ->
  {reply, emongo_nif:collection_exists(Conn, DbName, Collection), State};

handle_call(_Request, _From, State) ->
  {reply, ok, State}.

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, #state{conn = _Conn}) ->
  {ok, disconnect}.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

get_arg(Key, Args) ->
  {Key, Val} = lists:keyfind(Key, 1, Args),
  Val.