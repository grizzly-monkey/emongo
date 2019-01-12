%%%----------------------------------------------------------------------
%%% File    : emongo_sup.erl
%%% @author  : Jeet Parmar <jeet@glabbr.com>
%%% Purpose : Emongo Supervisor for emongo workers
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

-module(emongo_sup).
-author('jeet@glabbr.com').

-behaviour(supervisor).

%% API
-export([start_link/1, start_child/2]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).
-define(ETS_TAB, emongo).
-define(DEFAULT_TENANT, default).
%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Args) ->
  case ets:info(?ETS_TAB) of
    undefined -> ets:new(?ETS_TAB, [set, named_table, private]);
    _ -> ok
  end,
  case supervisor:start_link({local, ?SERVER}, ?MODULE, Args) of
    {ok, Pid} -> put_tenant_pid(?DEFAULT_TENANT, Pid),
      {ok, Pid};
    Err ->
      Err
  end.

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a supervisor is started using supervisor:start_link/[2,3],
%% this function is called by the new process to find out about
%% restart strategy, maximum restart frequency and child
%% specifications.
%%
%% @spec init(Args) -> {ok, {SupFlags, [ChildSpec]}} |
%%                     ignore |
%%                     {error, Reason}
%% @end
%%--------------------------------------------------------------------

init(Args) ->
  PoolArgs = [{name, {local, ?DEFAULT_TENANT}},
    {worker_module, emongo_worker}] ++ [{size, 1}, {max_overflow, 1}],
  PoolSpecs = poolboy:child_spec(?DEFAULT_TENANT, PoolArgs, Args),
  {ok, {{one_for_all, 10, 50}, [PoolSpecs]}}.


start_child(TenantId, Uri) ->

  PoolArgs = [{name, {local, TenantId}},
    {worker_module, emongo_worker}] ++ [{size, 1}, {max_overflow, 1}],
  PoolSpecs = poolboy:child_spec(TenantId, PoolArgs, [{uri, Uri}, {tenant_id, TenantId}]),

  case supervisor:start_child(get_tenant_pid(?DEFAULT_TENANT), PoolSpecs) of
    {ok, Pid} -> put_tenant_pid(TenantId, Pid),
      {ok, Pid};
    Err ->
      Err
  end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

get_tenant_pid(TenantId) ->
  [{TenantId, Pid}] = ets:lookup(?ETS_TAB, ?DEFAULT_TENANT),
  Pid.


put_tenant_pid(TenantId, Pid) ->
  ets:insert(?ETS_TAB, {TenantId, Pid}).