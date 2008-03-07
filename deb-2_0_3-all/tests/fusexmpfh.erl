%% @doc Mount-point filesystem in Erlang.
%% Compare with <a href="http://fuse.cvs.sourceforge.net/fuse/fuse/example/fusexmp_fh.c?view=markup">fusexmp_fh.c</a>.
%% @end

-module (fusexmpfh).
-include_lib ("eunit/include/eunit.hrl").
-include_lib ("kernel/include/file.hrl").
-export ([ start_link/2 ]).
-behaviour (fuserl).
-include ("../src/fuserl.hrl").
-export ([ code_change/3,
           handle_info/2,
           init/1,
           terminate/2,
           access/5,
           create/7,
           flush/5,
           forget/5,
           fsync/6,
           fsyncdir/6,
           getattr/4,
           getlk/6,
           getxattr/6,
	   link/6,
	   listxattr/5,
	   lookup/5,
           mkdir/6,
           mknod/7,
           open/5,
           opendir/5,
           read/7,
           readdir/7,
           readlink/4,
           release/5,
           releasedir/5,
           removexattr/5,
           rename/7,
           rmdir/5,
           setattr/7,
           setlk/7,
           setxattr/7,
           statfs/4,
           symlink/6,
           unlink/5,
           write/7 ]).

%-=====================================================================-
%-                                Public                               -
%-=====================================================================-

start_link (LinkedIn, Dir) ->
  fuserlsrv:start_link (?MODULE, LinkedIn, "", Dir, [], []).

%-=====================================================================-
%-                           fuserl callbacks                          -
%-=====================================================================-

-record (state, { inodes, major_device, minor_device }).

init ([]) ->
  { ok, FileInfo } = file:read_link_info ("/"),

  % the major/minor device checking is because otherwise
  % inodes are reused, which causes errors

  { ok, 
    #state{ inodes = gb_trees:empty (),
            major_device = FileInfo#file_info.major_device,
            minor_device = FileInfo#file_info.minor_device } }.

code_change (_OldVsn, State, _Extra) -> { ok, State }.
handle_info (_Msg, State) -> { noreply, State }.
terminate (_Reason, _State) -> ok.

access (Ctx, Inode, Mode, _, State) ->
  io:format ("access~n", []),
  try
    Path = get_inode (Inode, State),
    case file:read_link_info (Path) of
      { ok, FileInfo } ->
        case 
          lists:all 
            (fun (X) -> X end,
             [ case Mode band Mask of 
                 0 -> true; 
                 _ -> lists:member (FileInfo#file_info.access, Perm)
               end ||
               { Mask, Perm } <- [ { ?R_OK, [ read, read_write ] },
                                   { ?W_OK, [ write, read_write ] } ] ]) of
            true ->
              case Mode band ?X_OK of
                0 ->
                  { #fuse_reply_err{ err = ok }, State };
                _ ->
                  case is_executable (Ctx, FileInfo) of
                    true ->
                      { #fuse_reply_err{ err = ok }, State };
                    false ->
                      { #fuse_reply_err{ err = eacces }, State }
                  end
              end;
            false ->
              { #fuse_reply_err{ err = eacces }, State }
        end;
      { error, Reason } ->
        { #fuse_reply_err{ err = Reason }, State }
    end
  catch
    _X : _Y ->
      { #fuse_reply_err{ err = einval }, State }
  end.

create (Ctx, Parent, Name, Mode, Fi, Cont, State) ->
  io:format ("create~n", []),
  case mknod (Ctx, Parent, Name, Mode, { 0, 0 }, Cont, State) of
    { #fuse_reply_entry{}, State2 } ->
      case lookup (Ctx, Parent, Name, Cont, State2) of
        { #fuse_reply_entry{ 
            fuse_entry_param = Param = #fuse_entry_param{ ino = Inode } },
          State3 } ->
          case open (Ctx, Inode, Fi, Cont, State3) of
            { #fuse_reply_open{ fuse_file_info = FileInfo }, State4 } ->
              { #fuse_reply_create{ fuse_entry_param = Param,
                                    fuse_file_info = FileInfo },
                State4 };
            R = { #fuse_reply_err{}, _ } ->
              R
          end;
        R = { #fuse_reply_err{}, _ } ->
          R
      end;
    R = { #fuse_reply_err{}, _ } ->
      R
  end.

flush (Ctx, Inode, Fi, Cont, State) ->
  io:format ("flush~n", []),
  fsync (Ctx, Inode, false, Fi, Cont, State).

forget (_, Inode, Nlookup, _, State) ->
  io:format ("forget~n", []),
  { #fuse_reply_none{}, forget_inode (Inode, Nlookup, State) }.

fsync (_, _Inode, _IsDataSync, Fi, _, State) ->
  io:format ("fsync~n", []),
  case gb_trees:lookup (Fi#fuse_file_info.fh, State#state.inodes) of
    { value, <<"directory">> } ->
      { #fuse_reply_err{ err = ok }, State };
    { value, IoDevice } ->
       case file:sync (IoDevice) of
         ok -> 
          { #fuse_reply_err{ err = ok }, State };
         { error, Reason } ->
          { #fuse_reply_err{ err = Reason }, State }
       end;
    none ->
      { #fuse_reply_err{ err = ebadf }, State }
  end.

fsyncdir (Ctx, Inode, IsDataSync, Fi, Cont, State) ->
  io:format ("fsyncdir~n", []),
  fsync (Ctx, Inode, IsDataSync, Fi, Cont, State).

getattr (_, Inode, _, State) ->
  io:format ("getattr~n", []),
  try
    case file:read_link_info (get_inode (Inode, State)) of
      { ok, FileInfo } ->
        { #fuse_reply_attr{ attr = file_info_to_stat (FileInfo), 
                            attr_timeout_ms = 1000 },
          State };
      { error, Reason } ->
        { #fuse_reply_err{ err = Reason }, State }
    end
  catch
    _X : _Y ->
      { #fuse_reply_err{ err = einval }, State }
  end.

% Cheesy implementation for testing purposes.
getlk (_, Inode, _Fi, Lock, _, State) ->
  io:format ("getlk~n", []),
  case gb_trees:lookup ({ lock, Inode }, State#state.inodes) of
    { value, Locks } ->
      case [ { LType, LPid, LFh } || { LType, LPid, LFh } <- Locks,
                                     LPid =/= Lock#flock.l_pid,
                                     LType =:= f_wrlck ] of
        [] ->
          { #fuse_reply_lock{ flock = #flock{ l_type = f_unlck } }, State };
        [ { LType, LPid, _ } ] ->
          { #fuse_reply_lock{ flock = #flock{ l_type = LType,
                                              l_whence = seek_set,
                                              l_start = 0,
                                              l_len = 0,
                                              l_pid = LPid } }, 
            State }
      end;
    none ->
      { #fuse_reply_lock{ flock = #flock{ l_type = f_unlck } }, State }
  end.

% Cheesy implementation for testing purposes.
getxattr (_, Inode, Name, Size, _, State) ->
  io:format ("getxattr~n", []),
  case gb_trees:lookup ({ attr, Inode, Name }, State#state.inodes) of
    { value, Value } ->
      case Size of
        0 ->
          { #fuse_reply_xattr{ count = erlang:size (Value) }, State };
        N when N < erlang:size (Value) ->
          { #fuse_reply_err{ err = erange }, State };
        _ ->
          { #fuse_reply_buf{ size = erlang:size (Value), buf = Value }, State }
      end;
    none ->
      % hmmm ... man page says ENOATTR ... 
      % can't find it in any C system header ... (?)
      { #fuse_reply_err{ err = enoent }, State }
  end.

link (Ctx, Inode, NewParent, NewName, Cont, State) ->
  io:format ("link~n", []),
  try
    From = get_inode (Inode, State),
    To = get_inode (NewParent, State) ++ "/" ++ binary_to_list (NewName),
    case file:make_link (From, To) of
      ok ->
        lookup (Ctx, NewParent, NewName, Cont, State);
      { error, Reason } ->
        { #fuse_reply_err{ err = Reason }, State }
    end
  catch
    _X : _Y ->
      { #fuse_reply_err{ err = einval }, State }
  end.

listxattr (_, Inode, Size, _, State) ->
  io:format ("listxattr~n", []),
  Names = [ N || 
            { attr, I, N } <- gb_trees:keys (State#state.inodes), I =:= Inode ],
  NameLen = lists:foldl (fun (X, Acc) -> Acc + erlang:size (X) end,
                         erlang:length (Names),
                         Names),

  case Size of 
    0 ->
      { #fuse_reply_xattr{ count = NameLen }, State };
    N when N < NameLen ->
      { #fuse_reply_err{ err = erange }, State };
    _ ->
      { #fuse_reply_buf{ size = NameLen, 
                         buf = [ [ N, <<0:8>> ] || N <- Names ] },
        State }
  end.

lookup (_, Parent, Name, _, State) ->
  io:format ("lookup~n", []),
  try
    Path = get_inode (Parent, State) ++ "/" ++ binary_to_list (Name),
    case file:read_link_info (Path) of
      { ok, FileInfo } 
        when (FileInfo#file_info.major_device =:= State#state.major_device) and
             (FileInfo#file_info.minor_device =:= State#state.minor_device) ->
        { #fuse_reply_entry{ 
            fuse_entry_param = file_info_to_entry_param (FileInfo) },
          lookup_inode (FileInfo#file_info.inode, Path, State) };
      { ok, _ } ->
        { #fuse_reply_err{ err = enoent }, State };
      { error, Reason } ->
        { #fuse_reply_err{ err = Reason }, State }
    end
  catch
    _X : _Y ->
      { #fuse_reply_err{ err = einval }, State }
  end.

mkdir (_, Parent, Name, Mode, Cont, State) ->
  io:format ("mkdir~n", []),
  try
    Path = get_inode (Parent, State) ++ "/" ++ binary_to_list (Name),
    case file:make_dir (Path) of
      ok ->
        chmod (Path, Mode, Cont, State);
      { error, Reason } ->
        { #fuse_reply_err{ err = Reason }, State }
    end
  catch
    _X : _Y ->
      { #fuse_reply_err{ err = einval }, State }
  end.

mknod (_, Parent, Name, Mode, Dev, Cont, State) ->
  io:format ("mknod~n", []),

  try
    Path = get_inode (Parent, State) ++ "/" ++ binary_to_list (Name),
    case file:read_link_info (Path) of
      { ok, _ } ->
        { #fuse_reply_err{ err = eexist }, State };
      { error, enoent } ->
        case { Mode band ?S_IFIFO, Mode band ?S_IFREG, Dev } of
          { N, 0, _ } when N =/= 0 ->
            os:cmd ("mkfifo '" ++ shell_escape (Path) ++ "'"),

            case file:read_link_info (Path) of
              { ok, FileInfo } ->
                if 
                  FileInfo#file_info.type =:= other ->
                    chmod (Path, Mode, Cont, State);
                  true ->
                    { #fuse_reply_err{ err = eio }, State }
                end;
              { error, Reason } ->
                { #fuse_reply_err{ err = Reason }, State }
            end;
          { 0, M, _ } when M =/= 0 ->
            case file:open (Path, [ raw, append ]) of
              { ok, IoDevice } ->
                ok = file:close (IoDevice),
                chmod (Path, Mode, Cont, State); 
              { error, Reason } ->
                { #fuse_reply_err{ err = Reason }, State }
            end;
          _ ->
            { #fuse_reply_err{ err = enotsup }, State }
        end;
      { error, Reason } ->
        { #fuse_reply_err{ err = Reason }, State }
    end
  catch
    _X : _Y ->
      { #fuse_reply_err{ err = einval }, State }
  end.

open (_, Inode, Fi, _, State) ->
  io:format ("open~n", []),
  try
    Modes = 
      case (Fi#fuse_file_info.flags band ?O_ACCMODE) of
        ?O_RDONLY -> [ read, read_ahead ];
        ?O_WRONLY -> [ write, delayed_write ];
        ?O_RDWR -> [ read, read_ahead, write, delayed_write ]
      end,
  
    case file:open (get_inode (Inode, State), [ raw, binary | Modes ]) of
      { ok, IoDevice } ->
        { #fuse_reply_open{ 
            fuse_file_info = Fi#fuse_file_info{ fh = nextkey (State) } },
          State#state{ inodes = gb_trees:insert (nextkey (State), 
                                                 IoDevice, 
                                                 State#state.inodes) } };
      { error, Reason } -> 
        { #fuse_reply_err{ err = Reason }, State }
    end
  catch
    _X : _Y ->
      { #fuse_reply_err{ err = einval }, State }
  end.

opendir (_, Inode, Fi, _, State) ->
  io:format ("opendir~n", []),
  try
    Path = get_inode (Inode, State),
    case file:read_link_info (Path) of
      { ok, FileInfo } ->
        case FileInfo#file_info.type of
          directory ->
            Fh = nextkey (State),
            NewInodes = 
              gb_trees:insert (Fh, <<"directory">>, State#state.inodes),
            { #fuse_reply_open{ fuse_file_info = Fi#fuse_file_info{ fh = Fh } },
              State#state{ inodes = NewInodes } };
          _ ->
            { #fuse_reply_err{ err = enotdir }, State }
        end;
      { error, Reason } ->
        { #fuse_reply_err{ err = Reason }, State }
    end
  catch
    _X : _Y ->
      { #fuse_reply_err{ err = einval }, State }
  end.

read (_, _Inode, Size, Offset, Fi, _, State) ->
  io:format ("read~n", []),
  case gb_trees:lookup (Fi#fuse_file_info.fh, State#state.inodes) of
    { value, <<"directory">> } ->
      { #fuse_reply_err{ err = eisdir }, State };
    { value, IoDevice } ->
      case file:pread (IoDevice, Offset, Size) of
        { ok, Data } ->
          { #fuse_reply_buf{ size = erlang:size (Data), buf = Data }, State };
        { error, Reason } ->
          { #fuse_reply_err{ err = Reason }, State }
      end;
    none ->
      { #fuse_reply_err{ err = ebadf }, State }
  end.

readdir (_, Inode, Size, Offset, _Fi, _, State) ->
  io:format ("readdir~n", []),
  try
    Path = get_inode (Inode, State),
    case file:list_dir (Path) of
      { ok, Filenames } ->
        { #fuse_reply_direntrylist{ 
            direntrylist = direntrylist_limited (Size,
                                                 Offset,
                                                 Path,
                                                 Filenames, 
                                                 State) },
          State };
      { error, Reason } ->
        { #fuse_reply_err{ err = Reason }, State }
    end
  catch
    _X : _Y ->
      { #fuse_reply_err{ err = einval }, State }
  end.

readlink (_, Inode, _, State) ->
  io:format ("readlink~n", []),
  try
    Path = get_inode (Inode, State),
    case file:read_link (Path) of
      { ok, Filename } ->
        { #fuse_reply_readlink{ link = Filename }, State };
      { error, Reason } ->
        { #fuse_reply_err{ err = Reason }, State }
    end
  catch
    _X : _Y ->
      { #fuse_reply_err{ err = einval }, State }
  end.

release (_, _Inode, Fi, _, State) ->
  io:format ("release~n", []),
  case gb_trees:lookup (Fi#fuse_file_info.fh, State#state.inodes) of
    { value, <<"directory">> } ->
      { error, ebadf, State };
    { value, IoDevice } ->
      ok = file:close (IoDevice),
      NewInodes = 
        lists:foldl (fun (Key, Acc) -> 
                       { value, Locks } = gb_trees:lookup (Key, Acc),
                        gb_trees:update (Key, 
                                         [ { LType, LPid, LFh } || 
                                           { LType, LPid, LFh } <- Locks,
                                           LFh =/= Fi#fuse_file_info.fh ],
                                         Acc)
                     end,
                     State#state.inodes,
                     [ { lock, Path } || 
                       { lock, Path } 
                         <- gb_trees:keys (State#state.inodes) ]),

      { #fuse_reply_err{ err = ok },
        State#state{ inodes = gb_trees:delete (Fi#fuse_file_info.fh, 
                                               NewInodes) } };
    none ->
      { #fuse_reply_err{ err = ebadf }, State }
  end.

releasedir (_, _Inode, Fi, _, State) ->
  io:format ("releasedir~n", []),
  case gb_trees:lookup (Fi#fuse_file_info.fh, State#state.inodes) of
    { value, <<"directory">> } ->
      NewInodes = gb_trees:delete (Fi#fuse_file_info.fh, State#state.inodes),

      { #fuse_reply_err{ err = ok }, State#state{ inodes = NewInodes } };
    { value, _ } ->
      { #fuse_reply_err{ err = ebadf }, State };
    none ->
      { #fuse_reply_err{ err = ebadf }, State }
  end.

removexattr (_, Inode, Name, _, State) ->
  io:format ("removexattr~n", []),
  case gb_trees:lookup ({ attr, Inode, Name }, State#state.inodes) of
    { value, _ } ->
      NewInodes = gb_trees:delete ({ attr, Inode, Name }, State#state.inodes),
      { #fuse_reply_err{ err = ok }, State#state{ inodes = NewInodes } };
    none ->
      { #fuse_reply_err{ err = enoent }, State }
  end.

rename (_, Parent, Name, NewParent, NewName, _, State) ->
  io:format ("rename~n", []),
  try
    Old = get_inode (Parent, State) ++ "/" ++ binary_to_list (Name),
    New = get_inode (NewParent, State) ++ "/" ++ binary_to_list (NewName),

    case file:rename (Old, New) of
      ok ->
        { #fuse_reply_err{ err = ok }, State };
      { error, Reason } ->
        { #fuse_reply_err{ err = Reason }, State }
    end
  catch
    _X : _Y ->
      { #fuse_reply_err{ err = einval }, State }
  end.

rmdir (_, Inode, Name, _, State) ->
  io:format ("rmdir~n", []),
  try
    Path = get_inode (Inode, State) ++ "/" ++ binary_to_list (Name),
    case file:del_dir (Path) of
      ok ->
        { #fuse_reply_err{ err = ok }, State };
      { error, Reason } ->
        { #fuse_reply_err{ err = Reason }, State }
    end
  catch
    _X : _Y ->
      { #fuse_reply_err{ err = einval }, State }
  end.

setattr (_, Inode, Attr, ToSet, _Fi, _, State) ->
  io:format ("setattr~n", []),
  try
    Path = get_inode (Inode, State),
    case file:read_link_info (Path) of
      { ok, FileInfo } ->
        NewFileInfo = mod_file_info (FileInfo, Attr, ToSet),
        case file:write_file_info (Path, NewFileInfo) of
          ok ->
            { #fuse_reply_attr{ attr = file_info_to_stat (FileInfo), 
                                attr_timeout_ms = 1000 },
              State };
          { error, Reason } ->
            { #fuse_reply_err{ err = Reason }, State }
        end;
      { error, Reason } ->
        { #fuse_reply_err{ err = Reason }, State }
    end
  catch
    _X : _Y ->
      { #fuse_reply_err{ err = einval }, State }
  end.

setlk (_, Inode, Fi, Lock, _Sleep, _, State) ->
  io:format ("setlk~n", []),
  case { Lock#flock.l_start, Lock#flock.l_len } of
    { 0, 0 } ->
      case gb_trees:lookup ({ lock, Inode }, State#state.inodes) of
        { value, Locks } ->
          case [ { LType, LPid, LFh } || { LType, LPid, LFh } <- Locks,
                                         LPid =/= Lock#flock.l_pid,
                                         LType =:= f_wrlck ] of
            [] ->
              NewInodes = gb_trees:update ({ lock, Inode },
                                            [ { Lock#flock.l_type,
                                                Lock#flock.l_pid,
                                                Fi#fuse_file_info.fh } | 
                                              Locks ],
                                            State#state.inodes),

              { #fuse_reply_err{ err = ok },
                State#state{ inodes = NewInodes } };
            _ ->
              { #fuse_reply_err{ err = eagain }, State }
          end;
        none ->
          NewInodes = gb_trees:insert ({ lock, Inode },
                                        [ { Lock#flock.l_type,
                                            Lock#flock.l_pid,
                                            Fi#fuse_file_info.fh } ],
                                        State#state.inodes),

          { #fuse_reply_err{ err = ok }, State#state{ inodes = NewInodes } }
      end;
    _ ->
      { #fuse_reply_err{ err = enotsup }, State }
  end.

setxattr (_, Inode, Name, Value, Flags, _, State) ->
  io:format ("setxattr~n", []),
  case { gb_trees:lookup ({ attr, Inode, Name }, State#state.inodes), 
         Flags band ?XATTR_CREATE,
         Flags band ?XATTR_REPLACE } of
    { _, N, M } when N > 0, M > 0 ->            % only one flag allowed
      { #fuse_reply_err{ err = einval }, State };
    { none, 0, N } when N > 0 ->
      { #fuse_reply_err{ err = enoent }, State };         
    { { value, _ }, N, 0 } when N > 0 ->
      { #fuse_reply_err{ err = eexist }, State };
    _ ->
      NewInodes = 
        gb_trees:enter ({ attr, Inode, Name }, Value, State#state.inodes),
      { #fuse_reply_err{ err = ok }, State#state{ inodes = NewInodes } }
  end.

statfs (_, _Inode, _, State) ->
  io:format ("statfs~n", []),
  { #fuse_reply_statfs{ statvfs = #statvfs{ f_bsize = 69,
                                            f_frsize = 2112,
                                            f_blocks = 31337,
                                            f_bfree = 42,
                                            f_bavail = 666,
                                            f_files = 36#brittany,
                                            f_ffree = 36#kfed,
                                            f_favail = 36#sean,
                                            f_fsid = 36#lohan,
                                            f_flag = 36#hilton,
                                            f_namemax = 36#winehouse } },
    State }.

symlink (Ctx, Link, Inode, Name, Cont, State) ->
  io:format ("symlink~n", []),
  try
    From = get_inode (Inode, State) ++ "/" ++ binary_to_list (Name),
    To = binary_to_list (Link),
    case file:make_symlink (To, From) of
      ok ->
        lookup (Ctx, Inode, Name, Cont, State);
      { error, Reason } ->
        { #fuse_reply_err{ err = Reason }, State }
    end
  catch
    _X : _Y ->
      { #fuse_reply_err{ err = einval }, State }
  end.

unlink (_, Inode, Name, _, State) ->
  io:format ("unlink~n", []),
  try
    Path = get_inode (Inode, State) ++ "/" ++ binary_to_list (Name),
    case file:delete (Path) of
      ok ->
        { #fuse_reply_err{ err = ok }, State };
      { error, Reason } ->
        { #fuse_reply_err{ err = Reason }, State }
    end
  catch
    _X : _Y ->
      { #fuse_reply_err{ err = einval }, State }
  end.

write (_, _Inode, Data, Offset, Fi, _, State) ->
  io:format ("write~n", []),
  case gb_trees:lookup (Fi#fuse_file_info.fh, State#state.inodes) of
    { value, <<"directory">> } ->
      { error, ebadf, State };
    { value, IoDevice } ->
      case file:pwrite (IoDevice, Offset, Data) of
        ok ->
          { #fuse_reply_write{ count = erlang:size (Data) }, State };
        { error, Reason } ->
          { #fuse_reply_err{ err = Reason }, State }
      end;
    none ->
      { #fuse_reply_err{ err = ebadf }, State }
  end.

%-=====================================================================-
%-                               Private                               -
%-=====================================================================-

% chmod mostly works, although file:write_file_info/2 does not 
% respond to ?S_ISVTX (directory sticky bit).

chmod (Path, Mode, _, State) ->
  io:format ("chmod~n", []),
  case Mode band ?S_ISVTX of
    0 ->
      case file:read_file_info (Path) of
        { ok, FileInfo } ->
          PermMask = 
            ?S_IRWXO bor ?S_IRWXG bor ?S_IRWXU bor ?S_ISGID bor ?S_ISUID,
          PermMode = Mode band PermMask,
          PermlessMode = FileInfo#file_info.mode band (bnot PermMask),
          NewMode = PermMode bor PermlessMode,
          case file:write_file_info (Path,
                                     FileInfo#file_info{ mode = NewMode }) of
            ok ->
              { #fuse_reply_entry{ 
                  fuse_entry_param = 
                    file_info_to_entry_param 
                      (FileInfo#file_info{ mode = NewMode }) },
                State };
            { error, Reason } ->
              { #fuse_reply_err{ err = Reason }, State }
          end;
        { error, Reason } ->
          { #fuse_reply_err{ err = Reason }, State }
      end;
    _ ->
      { #fuse_reply_err{ err = enotsup }, State }
  end.

direntrylist_limited (Size, Offset, Path, Filenames, State) ->
  { _, RevEntries } = 
    lists:foldl 
      (fun (P, { O, Acc }) ->
         case file:read_link_info (Path ++ "/" ++ P) of
           { ok, FileInfo } 
              when (FileInfo#file_info.major_device =:= 
                    State#state.major_device) and
                   (FileInfo#file_info.minor_device =:= 
                    State#state.minor_device) ->
             { O + 1,
               [ #direntry{ name = P, 
                            offset = O, 
                            stat = file_info_to_stat (FileInfo) } | 
                 Acc ] };
           { ok, _ } ->
             { O, Acc };
           { error, _ } ->
             { O, Acc }
         end
       end,
       { 1, [] },
       Filenames),

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
     lists:nthtail (Offset, lists:reverse (RevEntries))).

file_info_to_entry_param (F = #file_info{}) ->
  #fuse_entry_param{ ino = F#file_info.inode,
                     generation = 1,                    % (?)
                     attr = file_info_to_stat (F),
                     attr_timeout_ms = 1000,
                     entry_timeout_ms = 1000 }.

file_info_to_stat (#file_info{ size = Size,
                               atime = ATime,
                               mtime = MTime,
                               ctime = CTime,
                               mode = Mode,
                               links = Links,
                               inode = Inode,
                               uid = Uid,
                               gid = Gid }) ->
  #stat{ st_ino = Inode,
         st_mode = Mode,
         st_nlink = Links,
         st_uid = Uid,
         st_gid = Gid,
         st_size = Size,
         st_atime = localtime_to_unix (ATime),
         st_mtime = localtime_to_unix (MTime),
         st_ctime = localtime_to_unix (CTime)
       }.

forget_inode (Inode, Nlookup, State) ->
  { Path, Refcount } = gb_trees:get ({ inode, Inode }, State#state.inodes),
  case Refcount > Nlookup of
    true ->
      State#state{ inodes = gb_trees:update ({ inode, Inode },
                                             { Path, Refcount - Nlookup },
                                             State#state.inodes) };
    false ->
      State#state{ inodes = gb_trees:delete ({ inode, Inode }, 
                                             State#state.inodes) }
  end.

get_inode (1, _) -> 
  "/";
get_inode (Inode, State) ->
  { Path, _ } = gb_trees:get ({ inode, Inode }, State#state.inodes),
  Path.

is_executable (Ctx, FileInfo = #file_info{ mode = Mode }) ->
  is_world_executable (Mode) orelse
  is_group_executable (Mode, Ctx#fuse_ctx.gid, FileInfo#file_info.gid) orelse
  is_owner_executable (Mode, Ctx#fuse_ctx.uid, FileInfo#file_info.uid).

is_group_executable (Mode, ReqGid, Gid) ->
  case Mode band 8#00010 of
    0 ->
      false;
    _ ->
      ReqGid =:= Gid
  end.

is_owner_executable (Mode, ReqUid, Uid) ->
  case Mode band 8#00100 of
    0 ->
      false;
    _ ->
      ReqUid =:= Uid
  end.

is_world_executable (Mode) ->
  (Mode band 8#00001) =/= 0.

localtime_to_unix (DateTime) ->
  Seconds = 
    case calendar:local_time_to_universal_time_dst (DateTime) of
      [ UDT ] ->
        calendar:datetime_to_gregorian_seconds (UDT);
      [ UDTDst, _ ] ->
        calendar:datetime_to_gregorian_seconds (UDTDst)
    end,

  Start = calendar:datetime_to_gregorian_seconds ({ { 1970, 1, 1 },
                                                    { 0, 0, 0 } }),

  Seconds - Start.

lookup_inode (Inode, Path, State) ->
  case gb_trees:lookup ({ inode, Inode }, State#state.inodes) of
    { value, { _, Refcount } } ->
      State#state{ inodes = gb_trees:update ({ inode, Inode },
                                             { Path, Refcount + 1 },
                                             State#state.inodes) };
    none  ->
      State#state{ inodes = gb_trees:insert ({ inode, Inode },
                                             { Path, 1 },
                                             State#state.inodes) }
  end.

mod_file_info (FileInfo, Attr, ToSet) ->
  NewMode = 
    if ToSet band ?FUSE_SET_ATTR_MODE > 0 -> Attr#stat.st_mode;
       true -> FileInfo#file_info.mode
    end,

  NewUid = 
    if ToSet band ?FUSE_SET_ATTR_UID > 0 -> Attr#stat.st_uid;
       true -> FileInfo#file_info.uid
    end,

  NewGid = 
    if ToSet band ?FUSE_SET_ATTR_GID > 0 -> Attr#stat.st_gid;
       true -> FileInfo#file_info.gid
    end,

  NewSize = 
    if ToSet band ?FUSE_SET_ATTR_SIZE > 0 -> Attr#stat.st_size;
       true -> FileInfo#file_info.size
    end,

  NewATime = 
    if ToSet band ?FUSE_SET_ATTR_ATIME > 0 -> 
         unix_to_localtime (Attr#stat.st_atime);
       true -> 
         FileInfo#file_info.atime
    end,

  NewMTime = 
    if ToSet band ?FUSE_SET_ATTR_MTIME > 0 -> 
         unix_to_localtime (Attr#stat.st_mtime);
       true -> 
         FileInfo#file_info.mtime
    end,

  FileInfo#file_info{ mode = NewMode,
                      uid = NewUid,
                      gid = NewGid,
                      size = NewSize,
                      atime = NewATime,
                      mtime = NewMTime }.

nextkey (#state{ inodes = Inodes }) ->
  Keys = [ K || K <- gb_trees:keys (Inodes), erlang:is_integer (K) ],

  case erlang:length (Keys) of
    0 ->
      1;
    _ ->
      lists:max (Keys) + 1
  end.

shell_escape (String) -> shell_escape (String, []).

shell_escape ([], Acc) -> lists:reverse (Acc);
shell_escape ([ $' | T ], Acc) -> shell_escape (T, [ $', $\\ | Acc ]);
shell_escape ([ $\\ | T ], Acc) -> shell_escape (T, [ $\\, $\\ | Acc ]);
shell_escape ([ H | T ], Acc) -> shell_escape (T, [ H | Acc ]).

take_while (_, _, []) -> 
  [];
take_while (F, Acc, [ H | T ]) ->
  case F (H, Acc) of
    { continue, NewAcc } ->
      [ H | take_while (F, NewAcc, T) ];
    stop ->
      []
  end.

unix_to_localtime (UnixTime) ->
  Start = calendar:datetime_to_gregorian_seconds ({ { 1970, 1, 1 },
                                                    { 0, 0, 0 } }),

  calendar:universal_time_to_local_time 
    (calendar:gregorian_seconds_to_datetime (UnixTime + Start)).
