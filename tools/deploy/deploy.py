#!/usr/bin/env python2.6
'''
Usages:
  ./deploy.py path-to-config-file:path-to-method [extra-args]
     # You can omit `path-to-config-file', in which case the last `config[0-9]*.py' (in lexicographic order) will be used
Examples:
  ./deploy.py check_ssh # check ObCfg.all_hosts
  ./deploy.py tpl.gencfg config.py # generate a config template file, You can run all server on localhost without 'config.py'
  ## edit config.py.
  ./deploy.py ob1.update_local_bin
  ./deploy.py ob1[,.ct].reboot  # deploy and reboot obi and client, Will Cleanup Data
  ./deploy.py ob1[,.ct].restart # restart obi and client, Will Not Cleanup Data
  ./deploy.py ob1.random_test check 10 wait_time=1 # 'check' may be replaced by 'set_master/restart_server/disk_timeout/net_timeout/major_freeze/switch_schema/restart_client' or their combinations
  ./deploy.py ob1.random_test disk_timeout,set_master,major_freeze 10 wait_time=1 min_major_freeze_interval=10 # interval in seconds
  ./deploy.py ob1[,.ct].check_alive # check whether server and client are alive
  ./deploy.py ob1.ct.check_new_error
  ./deploy.py ob1[.ct,][stop,cleanup] # stop and cleanup obi and client
  ./deploy.py tr1.run case_pat=update_local_bin # special test case to update local bin
  ./deploy.py tr1.run fast_mode=True keep_going=True # run test case
Common options:
   ./deploy.py ... _dryrun_=True
   ./deploy.py ... _verbose_=True
   ./deploy.py ... _quiet_=True
   ./deploy.py ... _loop_=10
More Examples:
  ./deploy.py ob1.[id,pid,version]  # view ip, port, ver, data_dir, pid
  ./deploy.py ob1.rs0.[ip,port]
  ./deploy.py ob1.ups0.vi # view server config file
  ./deploy.py ob1.ups0.less # view log
  ./deploy.py ob1.ups0.grep
  ./deploy.py ob1.cs0.kill -41 # 41: debugon, 42: debugoff, 43: traceon, 44: traceoff
  ./deploy.py ob1.cs0.gdb
  ./deploy.py ob1.rs0.rs_admin stat -o ups
  ./deploy.py ob1.ups0.ups_admin get_clog_cursor
  ./deploy.py ob1.ms[0,1,2].restart
  ./deploy.py ob1.ms0.ssh
  ./deploy.py ob1.ms0.ssh ps -uxf
  ./deploy.py ob[1,2,3,4].all_server_do kill_by_port
  ./deploy.py ob1.set_obi_role obi_role=OBI_SLAVE
  ./deploy.py ob1.change_master_obi
  ...
More Configuration:
  ./deploy.py tpl.gencfg detail=True # dump detail configurable item
  ./deploy.py tpl.gensvrcfg # generate schema and server config template file in dir 'tpl'
'''
import sys, os
_base_dir_ = os.path.dirname(os.path.realpath(__file__))
sys.path.extend([os.path.join(_base_dir_, path) for path in '.'.split(':')])
import glob
from common import *
import random
import inspect
import signal

def usages():
    sys.stderr.write(__doc__)
    sys.exit(1)

def genconf(**attrs):
    role = attrs.get('role', 'unknown-role')
    tpl = find_attr(attrs, 'tpl.%s_template'%role)
    schema = find_attr(attrs, 'tpl.schema_template')
    if not tpl: raise Exception("Failed to Get '%s_template'"%(role))
    path, content = sub2('etc/$role.conf.$ip:$port', attrs), sub2(tpl, attrs)
    mkdir('etc')
    write(path, content)
    write(sub2('$schema', attrs), sub2(schema, attrs))
    return path

def get_match_child(d, pat):
    return dict_filter(lambda (k, v): type(v) == dict and re.match(pat, v.get('role', '')), d)

def update_bin(src, dest, workdir):
    server_list = '{rootserver,updateserver,chunkserver,mergeserver,lsyncserver}'
    server_list_in_src = '{rootserver/rootserver,updateserver/updateserver,chunkserver/chunkserver,mergeserver/mergeserver}'
    if re.match('^http:.*rpm$', src):
	cmd = '''mkdir -p $workdir $dest && ( [ -e $rpm ] || wget $src -O $rpm)
 && (cd $workdir; [ -e extract ] || rpm2cpio $rpm | cpio -idmv --no-absolute-filenames && touch extract)
 && rsync -a $workdir/home/admin/oceanbase/bin/$server_list $dest'''
        cmd = string.Template(cmd.replace('\n', '')).substitute(src=src, dest=dest, workdir=workdir, server_list=server_list, rpm='%s/ob.rpm'%(workdir))
    elif re.match('^http:.*svn.*branches', src):
	cmd = '''mkdir -p $workdir $dest && svn co $src $workdir
 && (cd $workdir; ([ -e .configured ] || (./build.sh init && CC=distcc CXX='distcc g++' ./configure --with-release) && touch .configured) && make -j -C src)
 && rsync -a $workdir/src/$server_list $dest'''
        cmd = string.Template(cmd.replace('\n', '')).substitute(src=src, dest=dest, workdir=workdir, server_list=server_list_in_src)
    else:
	cmd = '''mkdir -p $workdir $dest
 && (cd $workdir; ([ -e .configured ] || (./build.sh init && CC=distcc CXX='distcc g++' ./configure --with-release) && touch .configured) && make -j -C src)
 && rsync -a $workdir/src/$server_list $dest'''
        cmd = string.Template(cmd.replace('\n', '')).substitute(src=src, dest=dest, workdir=src, server_list=server_list_in_src)
    if gcfg.get('_dryrun_', False):
        return cmd
    elif 0 != sh(cmd):
        raise Fail('update_local_bin fail', cmd)

def Role():
    id = '$role$ver@$ip:$port:$dir'
    pid = '''popen: ssh $usr@$ip "pgrep -u $usr -f '^$exe'" || echo PopenException: NoProcessFound '''
    version = '''popen: ssh $usr@$ip "$exe -V" '''
    set_obi_role = '''sh: $localdir/tools/rs_admin -r $ip -p $port set_obi_role -o $obi_role'''
    start = '''sh: ssh $usr@$ip '(cd $dir; ulimit -c unlimited; pgrep -u $usr -f "^$exe" || $server_start_environ $exe -f etc/$role.conf.$ip:$port $_rest_)' '''
    stop = 'call: kill_by_name -9'
    kill = '''sh: ssh $usr@$ip "pkill $_rest_ -u $usr -f '^$exe'"'''
    kill_by_name = '''sh: ssh $usr@$ip "pkill $_rest_ -u $usr -f '^$exe'"'''
    kill_by_port = '''sh: ssh $usr@$ip "/sbin/fuser -n tcp $port -k $_rest_"'''
    conf = genconf
    rsync = '''sh: rsync -aR bin$ver/$role etc/$role.conf.$ip:$port $schema lib tools $usr@$ip:$dir'''
    rsync2 = '''sh: rsync -a $_rest_ $usr@$ip:$dir/'''
    rsync_clog = '''sh: rsync -av $clog_src $usr@$ip: && ssh $usr@$ip cp $clog_src/* $dir/data/ups_commitlog # ExceptionOnFail'''
    mkdir = '''sh: ssh $usr@$ip 'mkdir -p $dir/data/ups_data/raid{0,1} $dir/data/cs $dir/log
mkdir -p $data_dir/{1..10}/Recycle $data_dir/{1..10}/$sub_data_dir/{sstable,ups_store,{rs,ups}_commitlog}
chown $$USER $data_dir/{1..10}/$sub_data_dir -R
ln -sf $data_dir/1/$sub_data_dir/rs_commitlog $dir/data/
ln -sf $data_dir/1/$sub_data_dir/ups_commitlog $dir/data/
ln -sf $data_dir/{1..10} $dir/data/cs/
for i in {0,1}; do for j in {0,1}; do [ -e $dir/data/ups_data/raid$$i/store$$j ] || ln -sf $data_dir/$$((1 + i * 5 + j))/$sub_data_dir/ups_store $dir/data/ups_data/raid$$i/store$$j;done; done' '''.replace('\n', '; ')
    rmdir = '''sh: ssh $usr@$ip "rm -rf $data_dir/{1..10}/$sub_data_dir/* $dir/{etc,data,log}"'''
    debugon = '''sh: ssh $usr@$ip "pkill -41 -u $usr -f '^$exe'"'''
    debugoff = '''sh: ssh $usr@$ip "pkill -42 -u $usr -f '^$exe'"'''
    ssh = '''sh: ssh -t $ip $_rest_'''
    ssh_no_tty = '''sh: ssh $ip $_rest_'''
    gdb = '''sh: ssh -t $usr@$ip 'cd $dir; gdb bin$ver/$role --pid=`pgrep -f ^$exe`' '''
    bgdb = '''sh: ssh -t $usr@$ip "(cd $dir; gdb $exe --batch --eval-command=\\"$_rest_\\"  \`pgrep -u $usr -f '^$exe'\` | sed '/New Thread/d')"'''
    gdb_p = '''popen: ssh -t $usr@$ip "(cd $dir; gdb $exe --batch --eval-command=\\"$_rest_\\"  \`pgrep -u $usr -f '^$exe'\` | sed '/New Thread/d')"'''
    vi = '''sh: ssh -t $usr@$ip "vi $dir/etc/$role.conf.$ip\:$port"'''
    less = '''sh: ssh -t $usr@$ip "less $dir/log/$role.log.$ip\:$port"'''
    grep = '''sh: ssh $usr@$ip "grep '$_rest_' $dir/log/$role.log.$ip:$port"'''
    grep_p = '''popen: ssh $usr@$ip "grep '$_rest_' $dir/log/$role.log.$ip:$port"'''
    restart = '''seq: stop rsync start'''
    ups_admin = '''sh: $localdir/tools/ups_admin -a ${ip} -p ${port} -t $_rest_'''
    rs_admin = '''sh: $localdir/tools/rs_admin -r ${ip} -p ${port} $_rest_'''
    cs_admin = '''sh: $localdir/tools/cs_admin -s ${ip} -p ${port} -i "$_rest_"'''
    watch = 'call: cs_admin dump_server_stats ups 1 0 20 0 1~3'
    minor_freeze = '''sh: $localdir/tools/ups_admin -a ${ip} -p ${port} -t minor_freeze'''
    major_freeze = '''sh: $localdir/tools/ups_admin -a ${ip} -p ${port} -t major_freeze'''
    ups_admin_p = '''popen: $localdir/tools/ups_admin -a ${ip} -p ${port} -t $_rest_'''
    rs_admin_p = '''popen: $localdir/tools/rs_admin -r ${ip} -p ${port} $_rest_'''
    reload_conf = '''sh: $localdir/tools/ups_admin -a ${ip} -p ${port} -t reload_conf -f $dir/etc/$role.conf.$ip:$port'''
    collect_log = '''sh: mkdir -p $collected_log_dir/$run_id && scp $usr@$ip:$dir/log/$role.log.$ip:$port $collected_log_dir/$run_id/ '''
    switch_schema = '''seq: rsync rs_admin[switch_schema]'''
    return locals()

def role_op(ob, role, op, *args, **kw_args):
    def mycall(ob, path, *args, **kw_args):
        ret = call(ob, path, *args, **kw_args)
        return ret
    return [mycall(ob, '%s.%s'%(k, op), *args, **kw_args) for k, v in sorted(ob.items(), key=lambda x: x[0]) if type(v) == dict and re.match(role, v.get('role', ''))]

def obi_op(obi, op, *args, **attrs):
    return list_merge(role_op(obi, role, op, *args, **attrs) for role in 'rootserver updateserver mergeserver chunkserver lsyncserver proxyserver'.split())

def check_ssh_connection(ip, timeout):
    return timed_popen('ssh -T %s -o ConnectTimeout=%d true'%(ip, timeout), timeout)

def check_data_dir_permission(_dir, min_disk_space=10000000, timeout=1):
    ip, dir = _dir.split(':', 2)
    cmd = '''ssh %s "mkdir -p %s/{1..10} && test -w %s %s && test \`df /home|sed -n '/2/p' |awk '{print \$4}'\` -gt %d"'''%(
        ip, dir, dir,
        ' '.join(["&& test -w %s/%d"%(dir, i) for i in range(1,11)]),
        min_disk_space)
    return timed_popen(cmd, timeout)

def is_server_alive(server, timeout=3):
    return sh(sub2('''ssh $usr@$ip -o ConnectTimeout=%d "pgrep -u $usr -f '^$exe'" >/dev/null'''%(timeout), server)) == 0

def ObInstance():
    is_start_service_now = '''sh: ssh ${rs0.usr}@${rs0.ip} "grep '\[NOTICE\] start service now' ${rs0.dir}/log/${rs0.role}.log.${rs0.ip}:${rs0.port}"'''
    is_finish_reporting = '''sh: $localdir/tools/rs_admin -r ${rs0.ip} -p ${rs0.port} stat -o rs_status|grep INITED'''

    def check_alive(**ob):
        return all(map(is_server_alive, get_match_child(ob, '.*server$').values()))

    def check_basic(cli='ct', **ob):
        return call_(ob, 'check_alive') and call_(ob, '%s.check_alive'%(cli)) and call_(ob, '%s.check_new_error'%(cli))

    def require_start(**ob):
        if not check_until_timeout(lambda : call_(ob, 'is_start_service_now', _quiet_=True) == 0 and IterEnd() or 'wait ob start service', 180, 1):
            raise Fail('ob not start before timeout!')
        return 'OK'

    def check_local_bin(**ob):
        if 0 != sh(sub2('ls  bin$ver/{$local_servers} lib/{libsnappy_1.0.so,libobapi.so.1} tools/{$local_tools} >/dev/null', ob)):
            raise Fail('local bin file check failed. make sure "bin" "lib" "tools" dir and files exists!')
        return 'OK'
    def check_ssh(timeout=1, **ob):
        ip_list = set(v.get('ip') for v in get_match_child(ob, '.+server$').values())
        result = async_get(async_map(lambda ip: check_ssh_connection(ip, timeout), ip_list), timeout+1)
        if any(map(lambda x: x != 0, result)):
            raise Fail('check ssh fail, make sure you can ssh to these machine without passwd',
                    map(lambda ip, ret: '%s:%s'%(ip, ret == 0 and 'OK' or 'Fail'), ip_list, result))
        return 'OK'

    def check_dir(timeout=1, min_disk_space=10000000, **ob):
        dir_list = set(sub2('$ip:$data_dir', v) for v in get_match_child(ob, '.+server$').values())
        result = async_get(async_map(lambda dir: check_data_dir_permission(dir, min_disk_space, timeout), dir_list), timeout+1)
        if any(map(lambda x: x != 0, result)):
            raise Fail('check data dir fail, make sure you have write permission on configured data_dir and have enough space on home dir(>%dM):\n%s'%(min_disk_space,
                    pprint.pformat(map(lambda dir, ret: '%s:%s'%(dir, ret == 0 and 'OK' or 'Fail'), dir_list, result))))
        return 'OK'

    def set_obi_role_until_success(timeout=0.5, **ob):
        if not check_until_timeout(lambda : call_(ob, 'rs0.set_obi_role') == 0 and IterEnd() or 'try set_obi_role', 10, timeout):
            return 'Fail'
        return 'OK'

    def get_master_ups(**ob):
        m = re.search('[^0-9.]([0-9.]+):(\d+)\(\d+\s+master', call_(ob, 'rs0.rs_admin_p', 'stat -o ups'))
        if not m: return None
        return '%s:%s'% m.groups()

    def get_master_ups_name(**ob):
        ups = get_master_ups(**ob)
        master_ups = [k for k,v in get_match_child(ob, '^updateserver$').items() if type(v) == dict and sub2('$ip:$port', v) == ups]
        if not master_ups: return None
        else: return master_ups[0]

    def major_freeze(**ob):
        master_ups = get_master_ups_name(**ob)
        if not master_ups: raise Fail('no master ups present!')
        return call_(ob, '%s.major_freeze'%(master_ups))

    def all_server_do(*args, **ob):
        return par_do(ob, '.+server', *args)

    def status(full=False, **ob):
        call_(ob, 'rs0.rs_admin', 'get_obi_role')
        ups_list = get_match_child(ob, 'updateserver').keys()
        [call_(ob, '%s.ups_admin' %(ups), 'get_clog_cursor') for ups in ups_list]
        if full:
            call_(ob, 'rs0.rs_admin', 'stat -o ups')
            call_(ob, 'rs0.rs_admin', 'stat -o cs')
            call_(ob, 'rs0.rs_admin', 'stat -o ms')

    def update_local_bin(src=None, dest='bin$ver', workdir='$workdir/ob$ver', **ob):
        if src:
            return update_bin(sub2(src, ob), sub2(dest, ob), sub2(workdir, ob))
        else:
            return 'no src defined.'

    get_master_ups_using_client = 'popen: log_level=WARN $localdir/tools/client get_master_ups ${rs0.ip}:${rs0.port}'
    def get_master_ups_name2(**ob):
        ups = call_(ob, 'get_master_ups_using_client')
        master_ups = [k for k,v in get_match_child(ob, '^updateserver$').items() if type(v) == dict and sub2('$ip:$port', v) == ups]
        if not master_ups: return None
        else: return master_ups[0]

    def client(*req, **ob):
        ups = get_master_ups_name2(**ob)
        if not ups: return 'no master ups'
        cmd = 'log_level=INFO $localdir/tools/client %(req)s rs=${rs0.ip}:${rs0.port} ups=${%(ups)s.ip}:${%(ups)s.port} %(kw)s'%dict(req=' '.join(req), ups=ups, kw=kw_format(ob.get('__dynamic__', {})))
        return sh(sub2(cmd, ob))
        
    stress = '''sh: log_level=INFO n_transport=2 keep_goging_on_err=true $localdir/tools/client stress rs=${rs0.ip}:${rs0.port} server=ms write=10 scan=0 mget=0 write_size=10 cond_size=2'''
    cleanup_for_test = 'seq: stop cleanup ct.stop ct.clear'
    start_for_test = 'seq: reboot sleep[1] start_servers'
    start_ct_for_test = 'seq: ct.reboot sleep[1] ct.start'
    end_for_test = 'seq: stop collect_log'

    id = 'all: .+server id'
    pid = 'par: .+server pid'
    version = 'all: .+server version'
    set_obi_role = 'call: rs0.set_obi_role'
    start_servers = 'par: .+server start'
    start = 'seq: start_servers set_obi_role_until_success'
    stop = 'par: .+server kill_by_name'
    force_stop = 'par: .+server kill_by_name -9'
    rsync = 'all: .+server rsync'
    conf = 'all: .+server conf'
    mkdir = 'all: .+server mkdir'
    cleanup = 'all: .+server rmdir'
    collect_server_log = 'all: .+server collect_log'
    collect_log = 'seq: collect_server_log ct.collect_log'
    ssh = 'all: .+server ssh'
    ct_check_local_file = 'all: .*clientset check_local_file'
    ct_rsync = 'all: .*clientset rsync'
    ct_configure = 'all: .*clientset configure_obi'
    ct_prepare = 'all: .*clientset prepare'
    restart = 'seq: force_stop conf rsync mkdir ct_rsync start'
    reboot = 'seq: check_local_bin check_ssh check_dir ct_check_local_file force_stop cleanup conf rsync mkdir ct_rsync ct_prepare tc_prepare start'
    return locals()

def ClientAttr():
    id = '$client@$ip:$dir'
    role = 'client'
    ssh = '''sh: ssh -t $ip $_rest_'''
    rsync = '''sh: cp ${obi.rs0.schema} $client/${obi.app_name}.schema; rsync -az $client $usr@$ip:$dir'''
    start = '''sh: ssh $usr@$ip "${client_env_vars} $dir/$client/stress.sh start ${type} ${client_start_args}"'''
    clear = '''sh: ssh $usr@$ip "$dir/$client/stress.sh clear"'''
    stop = 'sh: ssh $usr@$ip $dir/$client/stress.sh stop ${type}'
    check = 'sh: ssh $usr@$ip $dir/$client/stress.sh check ${type}'
    collect_log = '''sh: mkdir -p $collected_log_dir/$run_id && ssh $usr@$ip 'scp $dir/$client/*.log $local_ip:$collected_log_dir/$run_id/' '''
    def check_correct(session=None, **self):
        if type(session) != dict: raise Fail('invalid session')
        if not session.has_key('last_error'): session['last_error'] = ''
        error = popen(sub2('ssh $usr@$ip $dir/$client/stress.sh check_correct', self))
        is_ok = error == session['last_error']
        session['last_error'] = error
        if is_ok:
            return 0
        else:
            print error
            return 1
    reboot = 'seq: stop rsync clear start'
    return locals()

def ClientSetAttr():
    id = 'all: client id'
    role = 'clientset'
    custom_attrs = {}
    nthreads = 5
    def client_op(self, role, op, *args, **kw_args):
        def mycall(self, path, *args, **kw_args):
            ret = call(self, path, *args, **kw_args)
            return ret
        return [mycall(self, '%s.%s'%(k, op), *args, **kw_args) for k, v in sorted(self.items(), key=lambda x: x[0]) if type(v) == dict and re.match(role, v.get('role', ''))]
    def check_alive(type='all', **self):
        return all(map(lambda cli: call_(cli, 'check', _quiet_=True, type=type) == 0, get_match_child(self, 'client$').values()))
    def check_new_error(**self):
        return all(map(lambda cli: call_(cli, 'check_correct', _quiet_=True, type='all') == 0, get_match_child(self, 'client$').values()))
    def configure_obi(**self):
        pass
    conf = 'all: client conf'
    prepare = 'all: client prepare'
    rsync = 'all: client rsync'
    start = 'all: client start type=all'
    stop = 'all: client stop type=all'
    clear = 'all: client clear type=all'
    check = 'all: client check type=all _quiet_=True'
    check_correct = 'all: client check_correct type=all _quiet_=True'
    collect_log = 'all: client collect_log'
    def require_start(type='all', **self):
        return check_until_timeout(lambda : check_alive(type=type, **self) and IterEnd() or (call_(self, 'start', **self.get('__dynamic__', {})) and 'try start client'), 30, 1)
    reboot = 'seq: stop conf rsync clear obi.require_start require_start'
    restart = 'seq: stop conf rsync obi.require_start require_start'
    return locals()

null_case_attr = dict(test_start='test_start', test_end='test_end')
def TestRunner():
    case_pat = '^.+$'
    tc_update_bin = 'seq: obi1.update_local_bin obi2.update_local_bin'
    tc_cleanup = 'seq: obi1.cleanup_for_test obi2.cleanup_for_test'
    tc_start_obi = 'seq: obi1.start_for_test obi2.start_for_test'
    tc_start_ct = 'seq: obi1.start_ct_for_test obi2.start_ct_for_test'
    tc_end = 'seq: obi1.end_for_test obi2.end_for_test'
    test_start = 'seq: tc_update_bin tc_cleanup tc_start_obi tc_start_ct'
    test_end='seq: tc_end'
    def load_case_dict(pat):
        return load_multi_file_vars(glob.glob(pat), __globals__=globals())
    def load_case_pairs(pat):
        return sorted([(name, func) for name, func in load_case_dict(pat).items() if getattr(func, 'TEST', None)], key=lambda (name,func): func.case_idx)
    def run_hook(case, inst1, inst2, hook, case_attr):
        return call(case.__dict__, hook, ob1=inst1, ob2=inst2, **case_attr)
    def _run(desc, case_pat, case_src_pat, obi=None, keep_going=True, no_hook=False, **attr):
        keep_going = keep_going != 'False'
        no_hook = no_hook == 'True'
        desc_kw = dict([i.split('=') for i in desc.split(':')[1:]])
        _obi = find_attr(attr, desc_kw.get('obi', obi))
        if not _obi: raise Fail('no obi specified!')
        inst1, inst2 = _obi.get('inst1', None), _obi.get('inst2', None)
        _inst1, _inst2 = inst1 and find_attr(attr, inst1) or _obi, inst2 and find_attr(attr, inst2)
        result = []
        print 'testrunner.run(file_pat="%s", case_pat="%s")'%(case_src_pat, case_pat)
        for name, case in load_case_pairs(case_src_pat):
            if not getattr(case, 'TEST', None) or not re.match(case_pat, name): continue
            ts = time.time()
            case_attr = dict(self_id=name, start_time=get_ts_str(ts), obi1=_inst1, obi2=_inst2, __parent__=[attr], **dict_updated(case.__dict__, **desc_kw))
            case_attr.update(**attr.get('__dynamic__', {}))
            print '%s runcase %s'%(timeformat(ts), name)
            succ = True
            if gcfg.get('_dryrun_', False):
                print 'runcase %s'%(name)
                ret = 'dryrun'
            else:
                try:
                    [inst.update(**case.__dict__) for inst in (_inst1, _inst2) if inst]
                    if not no_hook:
                        pprint.pprint(call(case_attr, 'test_start'))
                    ret = call(case_attr, 'self')
                    pprint.pprint(ret)
                except Exception as e:
                    succ = False
                    ret = e
                    print e, traceback.format_exc()
                finally:
                    if not no_hook:
                        pprint.pprint(call(case_attr, 'test_end'))
            result.append((succ, name, get_ts_str(ts), obi, ret))
            if not succ and not keep_going: break
        failed = filter(lambda x: not x[0], result)
        return result, failed
    def run(desc='desc', case_pat='^.+$', case_src_pat='test/Test*.py', obi=None, **attr):
        result, failed = _run(desc, case_pat, case_src_pat, obi, **attr)
        print '====================All===================='
        pprint.pprint(result)
        print '==================Failed==================='
        pprint.pprint(failed)
        print '==================Summary=================='
        print 'All Case: %d, Failed Case: %d: %s'%(len(result), len(failed), ','.join([c[1] for c in failed]))
        return len(failed)
    return locals()

def Attr(**attrs):
    def decorator(f):
        for k, v in attrs.items():
            setattr(f, k, v)
        f.self = f
        return f
    return decorator
def Test(TEST=True, **attr):
    return Attr(TEST=TEST, case_idx=ObCfg.case_counter.next(), **attr)
role_vars = dict_filter_out_special_attrs(Role())
obi_vars = dict_filter_out_special_attrs(ObInstance())
client_vars = dict_filter_out_special_attrs(ClientAttr())
ct_vars = dict_filter_out_special_attrs(ClientSetAttr())
tr_vars = dict_filter_out_special_attrs(TestRunner())

def make_role(role, **attr):
    return dict_updated(role_vars, idx=ObCfg.role_counter.next(), role=role, **attr)
    
def RootServer(ip='127.0.0.1', port='$rs_port', **attr):
    return make_role('rootserver', ip=ip, port=port, role_data_dir='data/rs_commitlog', **attr)

def UpdateServer(ip='127.0.0.1', port='$ups_port', inner_port='$ups_inner_port', lsync_ip='', lsync_port='$default_lsync_port', master_ip='', master_port='$ups_port', **attr):
    return make_role('updateserver', ip=ip, port=port, inner_port=inner_port,
                     lsync_ip=lsync_ip, lsync_port=lsync_port, master_ip=master_ip, master_port=master_port,
                     role_data_dir='data/ups_commitlog data/ups_data/raid*/store*', **attr)

def ChunkServer(ip='127.0.0.1', port='$cs_port', **attr):
    return make_role('chunkserver', ip=ip, port=port, role_data_dir='data/cs/*/$app_name/sstable', **attr)

def MergeServer(ip='127.0.0.1', port='$ms_port', **attr):
    return make_role('mergeserver', ip=ip, port=port, role_data_dir='', **attr)

def ObProxy(ip='127.0.0.1', port='$proxy_port', **attr):
    return make_role('proxyserver', ip=ip, port=port, role_data_dir='', **attr)

def LsyncServer(ip='127.0.0.1', port='$default_lsync_port', start_file_id=1, retry_wait_time_us=100000, **attr):
    return make_role('lsyncserver', ip=ip, port=port, start_file_id=start_file_id, retry_wait_time_us=100000, **attr)

def OBI(obi_role='OBI_MASTER', port_suffix=None, masters=[], slaves=[], proxys=[], hosts=[], need_lsync=False, **attr):
    session = dict()
    obi_idx = ObCfg.obi_counter.next()
    if not port_suffix: port_suffix = obi_idx
    deried_vars = dict(
        role = 'obi',
        session = session,
        obi_idx=obi_idx,
        rs_port = ObCfg.rs_port + port_suffix,
        ups_port = ObCfg.ups_port + port_suffix,
        ups_inner_port = ObCfg.ups_inner_port + port_suffix,
        cs_port = ObCfg.cs_port + port_suffix,
        ms_port = ObCfg.ms_port + port_suffix,
        proxy_port = ObCfg.proxy_port + port_suffix)
    if not get_match_child(attr, '^.*server$'):
        if not hosts: hosts = ObCfg.default_hosts
        if not masters: masters = hosts
        if not slaves: slaves = hosts
        if not proxys: proxys = hosts
        if not masters or not slaves:
            raise Fail('not enough master or slave support!')
        for idx, master in enumerate(masters):
            deried_vars['rs%d'%(idx)] = RootServer(master)
            deried_vars['ups%d'%(idx)] = UpdateServer(master)
            if need_lsync:
                deried_vars['lsync%d'%(idx)] = LsyncServer(master)
        for idx, slave in enumerate(slaves):
            deried_vars['cs%d'%(idx)] = ChunkServer(slave)
            deried_vars['ms%d'%(idx)] = MergeServer(slave)
        if any((v.get('client') == 'bigquery' or v.get('client') == 'sqltest' ) for v in get_match_child(attr, 'client').values()):
            for idx, proxy in enumerate(proxys):
                deried_vars['proxyserver%d'%(idx)] = ObProxy(proxy)
    obi = dict_updated(obi_vars, obi_role=obi_role, **dict_merge(deried_vars, attr))
    if not get_match_child(obi, '^.*server$'):
        raise Fail('ob%d: not even one server defined: default_hosts=%s!'%(obi_idx, ObCfg.default_hosts))
    if (any((v.get('client') == 'bigquery' or v.get('client') == 'sqltest' ) for v in get_match_child(attr, 'client').values()) and not get_match_child(obi, '^proxyserver$')):
        raise Fail('ob%d: no proxy server defined, but you request use "bigquery" or "seqltest" as client'%(obi_idx))
    obi.update(local_servers = ObCfg.local_servers)
    if get_match_child(obi, '^lsyncserver$'):
        obi['local_servers'] += ',lsyncserver'
    if get_match_child(obi, '^proxyserver$'):
        obi['local_servers'] += ',proxyserver'
    for ct in get_match_child(obi, '^.*clientset$').values():
        ct.update(obi=obi)
    ObCfg.after_load_hook.append(lambda obcfg: call(obi, "ct_configure"))
    return obi

def Client(ip='127.0.0.1', **attr):
    session = dict()
    return dict_updated(client_vars, ip=ip, session=session, **attr)

def CT(client='mixed_test', hosts=[], **attr):
    if not os.path.exists(client): raise Fail('no dir exists', client)
    idx = ObCfg.ct_counter.next()
    deried_vars = load_file_vars(search_file('%s/%s.py'%(client, client), ObCfg.module_search_path), globals())
    if not get_match_child(attr, '^.*client$'):
        if not hosts: hosts = ObCfg.default_hosts[:3]
        for idx, ip in enumerate(hosts):
            deried_vars['c%d'%(idx)] = Client(ip, **deried_vars.get('client_custom_attr', {}))
    ct = dict_updated(ct_vars, client=client, idx=idx, **dict_updated(deried_vars, **attr))
    if not get_match_child(ct, '^.*client$'):
        raise Fail('not even one client defined: default_hosts=%s!'%(ObCfg.default_hosts))
    return ct

def TR(obi=None, **attr):
    return dict_updated(tr_vars, obi=obi, **attr)

class ObCfg:
    def sleep(sleep_time=1, **ob):
        time.sleep(float(sleep_time))
        return sleep_time
    def check_ssh(timeout=1, **attr):
        result = async_get(async_map(lambda ip: check_ssh_connection(ip, timeout), ObCfg.all_hosts), timeout+1)
        return map(lambda ip,ret: (ip, ret == 0 and 'OK' or 'Fail'), ObCfg.all_hosts, result)
    all_hosts = []
    obi_counter = itertools.count(1)
    role_counter = itertools.count(1)
    ct_counter = itertools.count(1)
    case_counter = itertools.count(1)
    localdir = os.path.realpath('.')
    dev = popen(r'/sbin/ifconfig | sed -n "1s/^\(\S*\).*/\1/p"').strip()
    ip = popen(r'/sbin/ifconfig |sed -n "/inet addr/s/ *inet addr:\([.0-9]*\).*/\1/p" |head -1').strip()
    local_ip = ip
    usr = os.getenv('USER')
    uid = int(popen('id -u'))
    tpl = load_file_vars('%s/tpl.py'%(_base_dir_), globals())
    app_name='${name}.$usr'
    home = os.path.expanduser('~')
    dir = '$home/${name}'
    ver=''
    schema = 'etc/${app_name}.schema'
    workdir = '$home/ob.workdir'
    data_dir='/data'
    sub_data_dir = '$app_name'
    exe = '$dir/bin$ver/$role'
    cfg = '$dir/etc/$role.conf.$ip:$port'
    collected_log_dir = '$localdir/collected_log'
    self_id = '$name'
    start_time = get_ts_str()
    run_id = '$self_id.$start_time'
    trace_log_level='debug'
    log_sync_type = 1
    log_size = 64 # MB
    log_level = 'info'
    _port_base = 50 * (uid % 1000)
    rs_port = _port_base + 0
    ups_port = _port_base + 10
    ups_inner_port = _port_base + 20
    cs_port = _port_base + 30
    ms_port = _port_base + 40
    proxy_port = _port_base + 45
    default_lsync_port = _port_base + 50
    module_search_path = ['.', _base_dir_]
    default_hosts = [ip]
    after_load_hook = []
    server_ld_preload = ''
    server_start_environ = 'LD_LIBRARY_PATH=$dir/lib:/usr/local/lib:/usr/lib:/usr/lib64:$LD_LIBRARY_PATH LD_PRELOAD="$server_ld_preload"'
    local_servers = 'rootserver,updateserver,mergeserver,chunkserver'
    local_tools = 'rs_admin,ups_admin'
    convert_switch_log = 0
    replay_checksum_flag = 1
    lsync_retry_wait_time_us = 100000

    @staticmethod
    def get_cfg(path):
        cfg = ObCfg.get_cfg_(path)
        [v.get('name') or v.update(name=k) for k,v in get_match_child(cfg, 'obi').items()]
        return cfg
    @staticmethod
    def get_cfg_(path):
        if not os.path.exists(path):
            return dict_updated(load_str_vars(ObCfg.tpl.get("simple_config_template"), globals()), __parent__=[get_class_vars(ObCfg)])
        else:
            return dict_updated(load_file_vars(path, globals()), __parent__=[get_class_vars(ObCfg)])


def load_file(*file_list):
    for file in file_list:
        execfile('%s/%s'%(_base_dir_, file), globals())

def stop(sig, frame):
    print "catch sig: %d"%(sig)
    gcfg['stop'] = True

if __name__ == '__main__':
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    list_args, kw_args = parse_cmd_args(sys.argv[1:])
    list_args or usages()
    gcfg['_dryrun_'] = kw_args.get('_dryrun_') == 'True'
    _loop_ = int(kw_args.get('_loop_', '1'))
    method, rest_args = list_args[0], list_args[1:]
    method = method.split(':', 1)
    if len(method) == 2:
        config, method = method
    else:
        config, method = get_last_matching_file('.', '^config[0-9]*.py$'), method[0]
    print 'Use "%s" as Config File'%(config)
    try:
        ob_cfg = ObCfg.get_cfg(config)
    except Fail as e:
        print e
        sys.exit(1)
    ob_cfg = add_parent_link(ob_cfg)
    ob_cfg.update(help=lambda *ls,**kw: usages())
    for hook in ObCfg.after_load_hook:
        hook(ob_cfg)
    for i in range(_loop_):
        if _loop_ > 1:
            print 'iter %d'%(i)
        try:
            for m in multiple_expand(method):
                pprint.pprint(call(ob_cfg, m, *rest_args, **kw_args))
                sys.stdout.flush()
        except Fail as e:
            print e
    
