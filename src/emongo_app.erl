%%%----------------------------------------------------------------------
%%% File    : emongo_app.erl
%%% Author  : Jeet Parmar <jeet@glabbr.com>
%%% Purpose : Emongo Application
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

-module(emongo_app).

-author('jeet@glabbr.com').

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called whenever an application is started using
%% application:start/[1,2], and should start the processes of the
%% application. If the application is structured according to the OTP
%% design principles as a supervision tree, this means starting the
%% top supervisor of the tree.
%%
%% @spec start(StartType, StartArgs) -> {ok, Pid} |
%%                                      {ok, Pid, State} |
%%                                      {error, Reason}
%%      StartType = normal | {takeover, Node} | {failover, Node}
%%      StartArgs = term()
%% @end
%%--------------------------------------------------------------------
start(_StartType, StartArgs) ->
  emongo_nif:init(),
  emongo_sup:start_link(StartArgs).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called whenever an application has stopped. It
%% is intended to be the opposite of Module:start/2 and should do
%% any necessary cleaning up. The return value is ignored.
%%
%% @spec stop(State) -> void()
%% @end
%%--------------------------------------------------------------------
stop(_State) ->
  ok.

