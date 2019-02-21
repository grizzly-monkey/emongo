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

init(Args) when length(Args) > 0 ->
  process_flag(trap_exit, true),
  Uri = get_arg(uri, Args),
  TenantId = get_arg(tenant_id, Args),
  case emongo_nif:connect(Uri) of
    {ok, Conn} ->
      {ok, #state{id = TenantId, conn = Conn}};
    {error, Error} ->
      {error, Error}
  end;

init(_Args) ->
  {error, "Illegal arguments passed , needs URI as param"}.

handle_call({find_all, Collection}, _From, #state{conn = Conn} = State) ->
  {reply, emongo_nif:find_all(Conn, Collection), State};
handle_call({find_all, Collection, Opts}, _From, #state{conn = Conn} = State) ->
  {reply, emongo_nif:find_all(Conn, Collection, Opts), State};

handle_call({find_by_id, Collection, Id}, _From, #state{conn = Conn} = State) ->
  {reply, emongo_nif:find_by_id(Conn, Collection, Id), State};

handle_call({find, Collection, Filter}, _From, #state{conn = Conn} = State) ->
  {reply, emongo_nif:find(Conn, Collection, Filter), State};
handle_call({find, Collection, Filter, Opts}, _From, #state{conn = Conn} = State) ->
  {reply, emongo_nif:find(Conn, Collection, Filter, Opts), State};

handle_call({exists, Collection, Filter}, _From,
    #state{conn = Conn} = State) ->
  {reply, emongo_nif:exists(Conn, Collection, Filter), State};

handle_call({count, DbName, Collection, Filter}, _From,
    #state{conn = Conn} = State) ->
  {reply, emongo_nif:count(Conn, DbName, Collection, Filter), State};
handle_call({count, DbName, Collection, Filter, Opts}, _From, #state{conn = Conn} = State) ->
  {reply, emongo_nif:count(Conn, DbName, Collection, Filter, Opts), State};

handle_call({insert, Collection, LenJsonData, JsonData}, _From,
    #state{conn = Conn} = State) ->
  {reply, emongo_nif:insert(Conn, Collection, LenJsonData, JsonData), State};
handle_call({insert, Collection, LenJsonData, JsonData, Opts}, _From,
    #state{conn = Conn} = State) ->
  {reply, emongo_nif:insert(Conn, Collection, LenJsonData, JsonData, Opts), State};

handle_call({update_by_id, Collection, Id, LenJsonData, JsonData}, _From,
    #state{conn = Conn} = State) ->
  {reply, emongo_nif:update_by_id(Conn, Collection, Id, LenJsonData, JsonData), State};
handle_call({update_by_id, Collection, Id, LenJsonData, JsonData, Opts}, _From,
    #state{conn = Conn} = State) ->
  {reply, emongo_nif:update_by_id(Conn, Collection, Id, LenJsonData, JsonData, Opts), State};

handle_call({update_by_query, Collection, LenFilter, Filter, LenJsonData, JsonData}, _From,
    #state{conn = Conn} = State) ->
  {reply, emongo_nif:update_by_query(Conn, Collection, LenFilter, Filter, LenJsonData, JsonData),
    State};
handle_call({update_by_query, Collection, LenFilter, Filter, LenJsonData, JsonData, Opts}, _From,
    #state{conn = Conn} = State) ->
  {reply, emongo_nif:update_by_query(Conn, Collection, LenFilter, Filter, LenJsonData, JsonData,
    Opts), State};

handle_call({delete_by_id, Collection, Id}, _From, #state{conn = Conn} = State) ->
  {reply, emongo_nif:delete_by_id(Conn, Collection, Id), State};
handle_call({delete_by_id, Collection, Id, Opts}, _From, #state{conn = Conn} = State) ->
  {reply, emongo_nif:delete_by_id(Conn, Collection, Id, Opts), State};

handle_call({delete_by_query, Collection, LenFilter, Filter}, _From, #state{conn = Conn} = State) ->
  {reply, emongo_nif:delete_by_query(Conn, Collection, LenFilter, Filter), State};
handle_call({delete_by_query, Collection, LenFilter, Filter, Opts}, _From, #state{conn = Conn} = State) ->
  {reply, emongo_nif:delete_by_query(Conn, Collection, LenFilter, Filter, Opts), State};

handle_call({set_read_concern, DbName, Collection, Opts}, _From, #state{conn = Conn} = State) ->
  {reply, emongo_nif:delete_by_query(Conn, DbName, Collection, Opts), State};
handle_call({set_write_concern, DbName, Collection, Opts}, _From, #state{conn = Conn} = State) ->
  {reply, emongo_nif:set_write_concern(Conn, DbName, Collection, Opts), State};


handle_call({create_collection, DbName, Collection, Opts}, _From, #state{conn = Conn} = State) ->
  {reply, emongo_nif:create_collection(Conn, DbName, Collection, Opts), State};
handle_call({list_databases}, _From, #state{conn = Conn} = State) ->
  {reply, emongo_nif:list_databases(Conn), State};
handle_call({list_collections, DbName}, _From, #state{conn = Conn} = State) ->
  {reply, emongo_nif:list_collections(Conn, DbName), State};
handle_call({collection_exists, DbName, Collection}, _From, #state{conn = Conn} = State) ->
  {reply, emongo_nif:collection_exists(Conn, DbName, Collection), State};

handle_call({drop_db, DbName}, _From, #state{conn = Conn} = State) ->
  {reply, emongo_nif:drop_db(Conn, DbName), State};

handle_call({drop_collection, DbName, Collection}, _From, #state{conn = Conn} = State) ->
  {reply, emongo_nif:drop_collection(Conn, DbName, Collection), State};

handle_call({rename_collection, DbName, Collection, NewName}, _From, #state{conn = Conn} = State) ->
  {reply, emongo_nif:rename_collection(Conn, DbName, Collection, NewName), State};

handle_call({run_command, DbName, Command}, _From, #state{conn = Conn} = State) ->
  {reply, emongo_nif:run_command(Conn, DbName, Command), State};

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