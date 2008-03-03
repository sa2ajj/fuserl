%% @doc Hello world filesystem in Erlang using low-level interface.
%% Compare with <a href="http://fuse.cvs.sourceforge.net/fuse/fuse/example/hello_ll.c?view=markup">hello_ll.c</a>.
%% @end

-module (fusehello).
-include_lib ("eunit/include/eunit.hrl").
-include_lib ("kernel/include/file.hrl").
-export ([ start_link/2 ]).
%-behaviour (fuserl).
-export ([ code_change/3,
           handle_info/2,
           init/1,
           terminate/2,
           getattr/4,
	   lookup/5,
           open/5,
           read/7,
           readdir/7 ]).

-include ("../src/fuserl.hrl").

-define (HELLO_STR, <<"Hello World Erlang!\n">>).
-define (HELLO_NAME, "hello").

%-=====================================================================-
%-                                Public                               -
%-=====================================================================-

start_link (LinkedIn, Dir) ->
  fuserlsrv:start_link (?MODULE, LinkedIn, "", Dir, [], []).

%-=====================================================================-
%-                           fuserl callbacks                          -
%-=====================================================================-

init ([]) ->
  { ok, void }.

code_change (_OldVsn, State, _Extra) -> { ok, State }.
handle_info (_Msg, State) -> { noreply, State }.
terminate (_Reason, _State) -> ok.

getattr (_, 1, _, void) ->
  { #fuse_reply_attr{ attr = #stat{ st_mode = ?S_IFDIR bor 8#0755,
                                    st_nlink = 2 }, 
                      attr_timeout_ms = 1000 },
    void };
getattr (_, 2, _, void) ->
  { #fuse_reply_attr{ attr = hello_stat (),
                      attr_timeout_ms = 1000 },
    void };
getattr (_, _, _, void) ->
  { #fuse_reply_err{ err = enoent }, void }.

lookup (_, 1, <<?HELLO_NAME>>, _, void) ->
  { #fuse_reply_entry{ 
      fuse_entry_param = #fuse_entry_param{ ino = 2,
					    generation = 1,  % (?)
					    attr_timeout_ms = 1000,
					    entry_timeout_ms = 1000,
					    attr = hello_stat () } },
    void };
lookup (_, _, _, _, void) ->
  { #fuse_reply_err{ err = enoent }, void }.

open (_, 1, _, _, void) ->
  { #fuse_reply_err{ err = eisdir }, void };
open (_, 2, Fi = #fuse_file_info{}, _, void) ->
  case (Fi#fuse_file_info.flags band ?O_ACCMODE) =/= ?O_RDONLY of
    true ->
      { #fuse_reply_err{ err = eacces }, void };
    false ->
      { #fuse_reply_open{ fuse_file_info = Fi }, void }
  end.

read (_, 2, Size, Offset, _Fi, _, void) ->
  Len = erlang:size (?HELLO_STR),

  if 
    Offset < Len ->
      if 
        Offset + Size > Len ->
          Take = Len - Offset,
          <<_:Offset/binary, Data:Take/binary, _/binary>> = ?HELLO_STR;
        true ->
          <<_:Offset/binary, Data:Size/binary, _/binary>> = ?HELLO_STR
      end;
    true ->
      Data = <<>>
  end,

  { #fuse_reply_buf{ buf = Data, size = erlang:size (Data) }, void }.

readdir (_, 1, Size, Offset, _Fi, _, void) ->
  DirEntryList = 
    take_while 
      (fun (E, { Total, Max }) -> 
         Cur = fuserlsrv:dirent_size (E),
         if 
           Total + Cur =< Max ->
             { continue, { Total + Cur, Max } };
           true ->
             stop
         end
       end,
       { 0, Size },
       lists:nthtail 
         (Offset,
          [ #direntry{ name = ".", offset = 1, stat = #stat{ st_ino = 1 } },
            #direntry{ name = "..", offset = 2, stat = #stat{ st_ino = 1 } },
            #direntry{ name = ?HELLO_NAME, offset = 3, stat = #stat{ st_ino = 2 } } ])),

  { #fuse_reply_direntrylist{ direntrylist = DirEntryList }, void };
readdir (_, _, _, _, _, _, void) ->
  { #fuse_reply_err{ err = enotdir }, void }.

%-=====================================================================-
%-                               Private                               -
%-=====================================================================-

hello_stat () ->
  #stat{ st_mode = ?S_IFREG bor 8#0444,
	 st_nlink = 1,
	 st_size = erlang:size (?HELLO_STR) }.

take_while (_, _, []) -> 
  [];
take_while (F, Acc, [ H | T ]) ->
  case F (H, Acc) of
    { continue, NewAcc } ->
      [ H | take_while (F, NewAcc, T) ];
    stop ->
      []
  end.

-ifdef (EUNIT).

%-=====================================================================-
%-                                Tests                                -
%-=====================================================================-

test_hello (LinkedIn) ->
  F = fun (Dir) ->
        fun () ->
          { ok, [ App ] } = file:consult ("../src/fuserl.app"),
          case application:load (App) of 
            ok -> ok; 
            { error, { already_loaded, _ } } -> ok 
          end,
          ?assert (erlang:system_info (thread_pool_size) > 0), % avoid deadlock

          { ok, Pid } = start_link (LinkedIn, Dir),

          MonitorRef = erlang:monitor (process, Pid),

          G = fun (F, T, N) when N > 0 -> 
                               case T () of true -> ok; 
                                            false -> 
                                              receive after 1000 -> ok end,
                                              F (F, T, N - 1)
                               end
              end,
          
          G (G, 
             fun () -> 
               { ok, Filenames } = file:list_dir (Dir),
               [ "hello" ] =:= lists:sort (Filenames) 
             end, 
             10),

          { ok, FileInfo } = file:read_file_info (Dir ++ "/hello"),
          FileInfo#file_info.size =:= size (?HELLO_STR),
          FileInfo#file_info.type =:= regular,
          (FileInfo#file_info.mode band 8#777) =:= 8#444,
          FileInfo#file_info.links =:= 1,

          { ok, ?HELLO_STR } = file:read_file (Dir ++ "/hello"),

          { error, enoent } = file:read_file_info (Dir ++ "/flass"),

          exit (Pid, normal),

          receive
            { 'DOWN', MonitorRef, _Type, _Object, _Info } -> ok
          end,

          true
        end
      end,

  { setup,
    fun () ->
      { MegaSec, Sec, MicroSec } = erlang:now (),
      Dir = "mount.tmp." ++
            integer_to_list (MegaSec) ++ "." ++
            integer_to_list (Sec) ++ "." ++
            integer_to_list (MicroSec),

      ok = file:make_dir (Dir),
      { ok, FileInfo } = file:read_link_info (Dir),
      case FileInfo#file_info.type of directory -> ok end,
      Dir
    end,
    fun (Dir) -> 
      H = fun (G) -> 
            case file:del_dir (Dir) of
              ok -> ok;
              { error, ebusy } -> receive after 1000 -> ok end, G (G)
            end
          end,

      ok = H (H)
    end,
    F }.

hello_pipe_test_ () ->
  test_hello (false).

hello_linkedin_test_ () ->
  test_hello (true).

-endif.
