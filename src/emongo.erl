%%%----------------------------------------------------------------------
%%% File    : emongo_app.erl
%%% @author : Jeet Parmar <jeet@glabbr.com>
%%% Purpose : Emongo APIs
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


-module(emongo).

-author('jeet@glabbr.com').

%% API
-export([start_tenant/2, find_all/2]).


%%%===================================================================
%%% API functions
%%%===================================================================
start_tenant(TenantId, Uri) ->
  emongo_sup:start_child(TenantId,Uri).

find_all(PoolName, Collection) ->
  poolboy:transaction(PoolName, fun(Worker) ->
    gen_server:call(Worker, {find_all, Collection})
                                end, infinity).