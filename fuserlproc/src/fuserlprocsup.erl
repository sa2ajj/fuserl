-module (fuserlprocsup).
-behaviour (supervisor).

-export ([ start_link/2, init/1 ]).

%-=====================================================================-
%-                                Public                               -
%-=====================================================================-

start_link (LinkedIn, MountPoint) ->
  supervisor:start_link (?MODULE, [ LinkedIn, MountPoint ]).

%-=====================================================================-
%-                         supervisor callbacks                        -
%-=====================================================================-

%% @hidden

init ([ LinkedIn, MountPoint ]) ->
  { ok,
    { { one_for_one, 3, 10 },
      [
        { fuserlprocsrv,
          { fuserlprocsrv, start_link, [ LinkedIn, MountPoint ] },
          permanent,
          10000,
          worker,
          [ fuserlprocsrv ]
        }
      ]
    }
  }.
