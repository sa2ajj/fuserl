-module (fuserlproc).
-behaviour (application).
-export ([ start/0,
           start/2,
           stop/0,
           stop/1 ]).

%-=====================================================================-
%-                        application callbacks                        -
%-=====================================================================-

%% @hidden

start () ->
  application:start (fuserl),
  application:start (fuserlproc).

%% @hidden

start (_Type, _Args) ->
  { ok, LinkedIn } = application:get_env (fuserlproc, linked_in),
  { ok, MountPoint } = 
    case application:get_env (fuserlproc, mount_point) of
      R when (R =:= { ok, from_environment }) or (R =:= undefined) ->
        case os:getenv ("DATADIR") of
          false ->
            undefined;
          Dir ->
            { ok, Dir ++ "/proc" }
        end;
      R ->
        R
    end,
  case application:get_env (fuserlproc, make_mount_point) of
    { ok, false } -> 
      ok;
    _ ->
      case file:make_dir (MountPoint) of
        ok ->
          ok;
        { error, eexist } ->
          ok
      end
  end,

  fuserlprocsup:start_link (LinkedIn, MountPoint).

%% @hidden

stop () ->
  application:stop (fuserlproc).

%% @hidden

stop (_State) ->
  ok.
