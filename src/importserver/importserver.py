#!/usr/bin/python2.6

import time
import SocketServer
import optparse
import threading
import logging
import pickle
import struct
import copy
import ConfigParser
import os
import sys
import datetime
import re
from subprocess import *
import Queue

def R(cmd, local_vars):
  G = copy.copy(globals())
  G.update(local_vars)
  return cmd.format(**G)

def checkpid(pid_file):
  try:
    with open(pid_file, 'r') as fd:
      pid = int(fd.read().strip())
  except IOError:
    pid = None
  if pid:
    try:
      os.kill(pid, 0)
    except OSError:
      pid = None
  if pid:
    message = "program has been exist: pid={0}\n".format(pid)
    sys.stderr.write(message)
    sys.exit(1)

def writepid():
  try:
    pid = str(os.getpid())
    with open(pid_file, 'w') as fd:
      fd.write("{0}\n".format(pid))
  except Exception as err:
    logging.exception('ERROR: {0}'.format(err))

def Daemonize():
  try:
    pid = os.fork()
    if pid > 0:
      # exit first parent
      sys.exit(0)
  except OSError, e:
    sys.stderr.write("fork #1 failed: %d (%s)\n" % (e.errno, e.strerror))
    sys.exit(1)

  # decouple from parent environment
  #os.chdir("/")
  os.setsid()
  os.umask(0)

  # do second fork
  try:
    pid = os.fork()
    if pid > 0:
      # exit from second parent
      sys.exit(0)
  except OSError, e:
    sys.stderr.write("fork #2 failed: %d (%s)\n" % (e.errno, e.strerror))
    sys.exit(1)

  # redirect standard file descriptors
  sys.stdout.flush()
  sys.stderr.flush()
  si = file('/dev/zero', 'r')
  so = file('/dev/null', 'a+')
  se = file('/dev/null', 'a+', 0)
  os.dup2(si.fileno(), sys.stdin.fileno())
  os.dup2(so.fileno(), sys.stdout.fileno())
  os.dup2(se.fileno(), sys.stderr.fileno())

  writepid()

class ExecutionError(Exception): pass

class Shell:
  @classmethod
  def popen(cls, cmd, host=None, username=None):
    '''Execute a command locally, and return
    >>> Shell.popen('ls > /dev/null')
    ''
    '''
    if host is not None:
      if username is not None:
        cmd = "ssh {username}@{host} '{cmd}'".format(**locals())
      else:
        cmd = "ssh {host} '{cmd}'".format(**locals())
    p = Popen(cmd, shell=True, stdout=PIPE, stderr=STDOUT)
    output = p.communicate()[0]
    err = p.wait()
    if err:
        output = 'Shell.popen({0})=>{1} Output=>"{2}"'.format(cmd, err, output)
        raise ExecutionError(output)
    return output

  @classmethod
  def sh(cls, cmd, host=None, username=None):
    '''Execute a command locally or remotely
    >>> Shell.sh('ls > /dev/null')
    0
    >>> Shell.sh('ls > /dev/null', host='10.232.36.29')
    0
    '''
    if host is not None:
      if username is not None:
        cmd = "ssh {username}@{host} '{cmd}'".format(**locals())
      else:
        cmd = "ssh {host} '{cmd}'".format(**locals())
    ret = os.system(cmd)
    if ret != 0:
      err_msg = 'Shell.sh({0}, host={1})=>{2}\n'.format(
          cmd, host, ret);
      sys.stderr.write(err_msg)
      raise ExecutionError(err_msg)
    else:
      logging.debug('"{0}" Execute SUCCESS'.format(cmd))
    return ret 

  @classmethod
  def scp(cls, src, host, dst, username=None):
    '''remote copy
    >>> Shell.scp('build1.py', '10.232.36.29', '')
    0
    '''
    if username is not None:
      cmd = 'scp {0} {1}@{2}:{3}'.format(src, username, host, dst)
    else:
      cmd = 'scp {0} {1}:{2}'.format(src, host, dst)
    return Shell.sh(cmd)

  @classmethod
  def mkdir(cls, path, host=None):
    '''make directory locally or remotely
    >>> Shell.mkdir('test', host='10.232.36.29')
    0
    >>> Shell.mkdir('test')
    0
    '''
    if host is None:
      os.path.exists(path) or os.mkdir(path)
      return 0
    else:
      return Shell.sh('mkdir -p {0}'.format(path), host)

class WorkerPool:
  class Worker(threading.Thread):
    def __init__(self, task_queue, status):
      threading.Thread.__init__(self)
      self.task_queue = task_queue
      self.status = status
      self.__stop = 0
      self.err = None

    def run(self):
      #cwd = 'thread' + str(self.ident)
      #Shell.mkdir(cwd)
      while not self.__stop:
        try:
          task = self.task_queue.get(timeout=1)
          task()
          self.task_queue.task_done()
        except Queue.Empty:
          pass
        except BaseException as e:
          self.task_queue.task_done()
          if self.err is None:
            self.err = e
          logging.exception('thread' + str(self.ident) + ' ' + str(e))
      if self.err is not None:
        raise self.err

    def stop(self):
      self.__stop = True

  class Status:
    def __init__(self):
      self.status = 0
    def set_active(self):
      self.status = 1
    def set_idle(self):
      self.status = 2
    def is_idle(self):
      self.status == 2

  def __init__(self, num):
    self.task_queue = Queue.Queue()
    self.n = num
    self.status = [WorkerPool.Status()] * num
    self.workers = [None] * num
    for i in range(num):
      self.workers[i] = WorkerPool.Worker(self.task_queue, self.status[i]);
      self.workers[i].start()

  def add_task(self, task):
    self.task_queue.put(task)

  def all_idle(self):
    for i in range(num):
      if not self.status[i].is_idle():
        return False
    return True

  def wait(self):
    self.task_queue.join()
    for w in self.workers:
      w.stop()

SendLock = threading.Lock()
def SendPacket(sock, pkg_type, data):
  '''
  packet format
      ---------------------------------------------------
      |    Packet Type (8B)    |    Data Length (8B)    |
      ---------------------------------------------------
      |                      Data                       |
      ---------------------------------------------------
  '''
  buf = pickle.dumps(data)
  packetheader = struct.pack('!ll', pkg_type, len(buf))
  try:
    SendLock.acquire()
    sock.sendall(packetheader)
    sock.sendall(buf)
  except Exception as e:
    pass
  finally:
    if SendLock is not None:
      SendLock.release()

def Receive(sock, length):
  result = ''
  while len(result) < length:
    data = sock.recv(length - len(result))
    if not data:
      break
    result += data
  if len(result) == 0:
    return None
  if len(result) < length:
    raise Exception('Do not receive enough data: '
        'length={0} result_len={1}'.format(length, len(result)))
  return result

def ReceivePacket(sock):
  '''
  receive a packet from socket
  return type is a tuple (PacketType, Data)
  '''
  packetformat = '!ll'
  packetheaderlen = struct.calcsize(packetformat)
  packetheader = Receive(sock, packetheaderlen)
  if packetheader is not None:
    (packettype, datalen) = struct.unpack(packetformat, packetheader)
    data = pickle.loads(Receive(sock, datalen))
    return (packettype, data)
  else:
    return (None, None)


class ImportServer(SocketServer.ThreadingMixIn,
    SocketServer.TCPServer):
  pass

class RequestHandler(SocketServer.StreamRequestHandler):
  GlobalLock = threading.Lock()

  def invoke_dispatch(self, inputs, options):
    try:
      import_list = []
      for i in inputs:
        input_dir = i[0]
        table_name = i[1]
        ic = R('{conf_dir}/{app_name}/{table_name}/sample.conf,{input_dir}',
            locals())
        import_list.append(ic)
      dpch_arg = ' '.join(import_list)
      logging.debug('dpch_arg is {0}'.format(dpch_arg))
      self.ts = datetime.datetime.now().strftime('%Y%m%d%H%M%S%f')
      logging.debug('timestamp for hadoop output path is {0}'.format(self.ts))
      dispatch_cmd = '{0}/dispatch.sh {1} -b -c "{2}" -t {3}'.format(bin_dir,
          options, dpch_arg, self.ts)
      logging.debug('dispatch_cmd is {0}'.format(dispatch_cmd))
      p = Popen(dispatch_cmd, shell=True, stdout=PIPE, stderr=STDOUT)
      l = p.stdout.readline()
      while l != '':
        SendPacket(self.connection, SUBMIT_IMPORT_TASK_RES_PKG, l)
        logging.debug('dispatch_cmd output is {0}'.format(l.rstrip()))
        l = p.stdout.readline()
      err = p.wait()
      if err:
        output = 'Popen({0})=>{1}'.format(dispatch_cmd, err)
        raise ExecutionError(output)
      ret = 0
    except Exception as err:
      output = 'ERROR: ' + str(err)
      logging.exception(output)
      ret = 1
    return ret

  def invoke_copy_sstable(self, inputs):
    file_num = 0
    wp = WorkerPool(copy_sstable_concurrency)
    try:
      ups_slaves = GetUpsList(dict(ip = rs_ip, port = rs_port))['slaves']
      l = 'ups slave list is ' + str(ups_slaves)
      logging.info(l)
      SendPacket(self.connection, SUBMIT_IMPORT_TASK_RES_PKG, l + '\n')
      obi_list = []
      for rs in obi_rs:
        ups = GetUpsList(rs)
        if ups['master'] != '':
          obi_list.append(ups)
      for obi in obi_list:
        l = 'remote cluster master is ' + obi['master']
        logging.info(l)
        SendPacket(self.connection, SUBMIT_IMPORT_TASK_RES_PKG, l + '\n')
        l = 'remote cluster slave list is ' + str(obi['slaves'])
        logging.info(l)
        SendPacket(self.connection, SUBMIT_IMPORT_TASK_RES_PKG, l + '\n')
      for path in GenBypassList(bypass_dir):
        ups_list = copy.copy(ups_slaves)
        for obi in obi_list:
          ups_list.append(obi['master'])
          ups_list += obi['slaves']
        ClearBypassDir(path, ups_list)
      globals()['ups_slaves'] = ups_slaves
      globals()['obi_list'] = obi_list
      for i in inputs:
        table_name = i[1]
        sample_conf = R('{conf_dir}/{app_name}/{table_name}/sample.conf',
            locals())
        def load_single_item(filename, key):
          with open(filename) as f:
            for l in f:
              i = l.split('=')
              if i[0].strip() == key:
                return i[1].strip()
          return None
        data_dir = load_single_item(sample_conf, 'HADOOP_DATA_DIR')
        if data_dir is None:
          logging.error('Can\'t get HADOOP_DATA_DIR')
          ret = 1
        else:
          current_date = self.ts
          ups_list = ' '.join(ups_slaves)
          hadoop_input_dir = R('{data_dir}/{app_name}/{current_date}/'
              '{table_name}/', locals())
          CopySSTable(self.connection, wp, hadoop_input_dir,
              GenBypassList(bypass_dir))
          file_num += GetFileNumber(hadoop_input_dir)
          ret = 0
    except Exception as err:
      output = 'ERROR: ' + str(err)
      logging.exception(output)
      ret = 1
    finally:
      wp.wait()
      for i in inputs:
        table_name = i[1]
        sample_conf = R('{conf_dir}/{app_name}/{table_name}/sample.conf',
            locals())
        data_dir = load_single_item(sample_conf, 'HADOOP_DATA_DIR')
        current_date = self.ts
        hadoop_input_dir = R('{data_dir}/{app_name}/{current_date}/'
            '{table_name}/', locals())
        err = DeletePath(hadoop_input_dir)
        if err:
          logging.error('Delete output data error: {0}'.format(
            hadoop_input_dir))
          ret = 1
    return (ret, file_num)

  def write_ctl_data(self):
    try:
      import_cmd = R('{bin_dir}/ob_import -h {rs_ip} -p {rs_port} '
          '-c {conf_dir}/ctl_data.ini -t ctl_table -f {conf_dir}/ctl_data.txt',
          locals())
      logging.debug(R('import_cmd is {import_cmd}', locals()))
      output = Shell.popen(import_cmd)
      logging.debug(R('import_cmd output is {output}', locals()))
      ret = 0
    except Exception as err:
      output = 'ERROR: ' + str(err)
      logging.exception(output)
      ret = 1
    return ret

  def invoke_load_bypass(self):
    loaded_num = 0
    try:
      ret = self.write_ctl_data()
      logging.debug('write_ctl_data returned {0}'.format(ret))
      if ret == 0:
        ups_admin_load_cmd = R('{bin_dir}/ups_admin -a {rs_ip} -p {rs_port} '
            '-o master_ups -t minor_load_bypass', locals())
        logging.debug(R('ups_admin_load_cmd is {ups_admin_load_cmd}', locals()))
        output = Shell.popen(ups_admin_load_cmd)
        logging.debug(R('ups_admin_load_cmd output is {output}', locals()))
        loaded_num = ParseLoadedNum(output)
        ret = self.write_ctl_data()
        logging.debug('write_ctl_data returned {0}'.format(ret))
        if ret == 0:
          ups_admin_freeze_cmd = R('{bin_dir}/ups_admin -a {rs_ip} -p {rs_port} '
              '-o master_ups -t major_freeze', locals())
          logging.debug(R('ups_admin_freeze_cmd is {ups_admin_freeze_cmd}', locals()))
          output = Shell.popen(ups_admin_freeze_cmd)
          logging.debug(R('ups_admin_freeze_cmd output is {output}', locals()))
    except Exception as err:
      output = 'ERROR: ' + str(err)
      logging.exception(output)
      ret = 1
    return (ret, loaded_num)

  def overwrite_op(self, inputs):
    options = '-f -m -g gateway_host_file'
    ret = self.invoke_dispatch(inputs, options)
    logging.debug('dispatch_cmd returned {0}'.format(ret))
    #SendPacket(self.connection, SUBMIT_OVERWRITE_TASK_RES_PKG, output)
    if ret == 0:
      SendPacket(self.connection, SUBMIT_OVERWRITE_TASK_RES_PKG, 'SUCCESSFUL')
    else:
      SendPacket(self.connection, SUBMIT_OVERWRITE_TASK_RES_PKG, 'FAILED')

  def import_op(self, inputs):
    options = ''
    import_num = 0
    loaded_num = 0
    ret = self.invoke_dispatch(inputs, options)
    logging.debug('dispatch_cmd returned {0}'.format(ret))
    if ret == 0:
      try:
        timer = 0
        while not self.GlobalLock.acquire(False):
          SendPacket(self.connection, SUBMIT_IMPORT_TASK_RES_PKG,
              'Waiting for another import task, {0} seconds\n'.format(timer))
          time.sleep(10)
          timer += 10
        ret, import_num = self.invoke_copy_sstable(inputs)
        logging.debug('copy_sstable returned {0}, import_num = {1}'.format(
          ret, import_num))
        if import_num > 0:
          if ret == 0:
            ret, loaded_num = self.invoke_load_bypass()
            logging.debug('ups_admin returned {0}'.format(ret))
          if import_num != loaded_num:
            err_msg = ('loaded error, the number of sstable is incorrect, '
                'loaded {0} and correct number should be {1}').format(
                loaded_num, import_num)
            logging.error(err_msg)
            SendPacket(self.connection, SUBMIT_IMPORT_TASK_RES_PKG, err_msg + '\n')
            ret = 1
      finally:
        try:
          if self.GlobalLock is not None:
            self.GlobalLock.release()
        except Exception:
          pass
    if ret != 0:
      SendPacket(self.connection, SUBMIT_IMPORT_TASK_RES_PKG, 'FAILED')
    else:
      SendPacket(self.connection, SUBMIT_IMPORT_TASK_RES_PKG, 'SUCCESSFUL')

  def handle(self):
    try:
      (packettype, data) = ReceivePacket(self.connection)
      if packettype == SUBMIT_OVERWRITE_TASK_PKG:
        try:
          timer = 0
          while not self.GlobalLock.acquire(False):
            SendPacket(self.connection, SUBMIT_IMPORT_TASK_RES_PKG,
                'Waiting for another import task, {0} seconds\n'.format(timer))
            time.sleep(10)
            timer += 10
          self.overwrite_op(data)
        finally:
          try:
            if self.GlobalLock is not None:
              self.GlobalLock.release()
          except Exception:
            pass
      elif packettype == SUBMIT_IMPORT_TASK_PKG:
        self.import_op(data)
      else:
        SendPacket(self.connection, ERROR_PKG, 'FAILED')
    except Exception as err:
      SendPacket(self.connection, SUBMIT_IMPORT_TASK_RES_PKG, 'FAILED')
      logging.exception("ERROR " + str(err))

def GenBypassList(pattern):
  match = re.search(r'\[(.+)\]', pattern)
  if match is None:
    return [pattern]
  specifier = match.group(1)
  match = re.match('([0-9]+)-([0-9]+)', specifier)
  if not match:
      raise Exception('illformaled range specifier: %s'%(specifier))
  _start, _end = match.groups()
  start,end = int(_start), int(_end)
  formatter = re.sub('\[.+\]', '%0'+str(len(_start))+'d', pattern)
  return [formatter%(x) for x in range(start, end+1)]

def ClearBypassDir(path, ups_slaves):
  trash_dir = R('{path}/trash', locals())
  mk_trash_dir = R('mkdir -p {trash_dir}', locals())
  clear_cmd = R("""for f in `ls -l {path} | grep -v "^total" | grep -v "^d" | awk "{{print \\\$NF}}"`; """
      """do mv {path}/$f {trash_dir}; done""", locals())
  tmp_dir = R('{path}/tmp', locals())
  rm_tmp_dir = R('rm -rf {tmp_dir}', locals())
  mkdir_tmp_dir = R('mkdir -p {tmp_dir}', locals())
  r = Shell.popen(mk_trash_dir)
  logging.debug('execute `{0}`, returned {1}'.format(mk_trash_dir, r))
  r = Shell.popen(clear_cmd)
  logging.debug('execute `{0}`, returned {1}'.format(clear_cmd, r))
  r = Shell.sh(rm_tmp_dir)
  logging.debug('execute `{0}`, returned {1}'.format(rm_tmp_dir, r))
  r = Shell.sh(mkdir_tmp_dir)
  logging.debug('execute `{0}`, returned {1}'.format(mkdir_tmp_dir, r))
  for ups in ups_slaves:
    r = Shell.popen(mk_trash_dir, host = ups)
    logging.debug('execute `{0}` on {1}, returned {2}'.format(
        mk_trash_dir, ups, r))
    r = Shell.popen(clear_cmd, host = ups)
    logging.debug('execute `{0}` on {1}, returned {2}'.format(
        clear_cmd, ups, r))
    r = Shell.popen(rm_tmp_dir, host = ups)
    logging.debug('execute `{0}` on {1}, returned {2}'.format(
        rm_tmp_dir, ups, r))
    r = Shell.popen(mkdir_tmp_dir, host = ups)
    logging.debug('execute `{0}` on {1}, returned {2}'.format(
        mkdir_tmp_dir, ups, r))


def GetUpsList(rs):
  '''
  Parsing updateserver addresses from output of 'rs_admin stat -o ups' command
  '''
  try:
    rs_ip = rs['ip']
    rs_port = rs['port']
    rs_admin_cmd = R('{bin_dir}/rs_admin -r {rs_ip} -p {rs_port} '
        'stat -o ups', locals())
    ups_list = dict(master = '', slaves = [])
    try:
      output = Shell.popen(rs_admin_cmd)
      ups_str_list = output.splitlines()[-1].split('|')[-1].split(',')
      for ups_str in ups_str_list:
        m = re.match(r'(.*)\(.* (.*) .* .* .* .*\)', ups_str)
        if m is not None:
          addr = m.group(1).split(':')[0]
          if m.group(2) == 'master':
            ups_list['master'] = addr
          else:
            ups_list['slaves'].append(addr)
        elif ups_str != '':
          logging.error("error rs output: `{0}'".format(ups_str))
      if ups_list['master'] is None:
        raise Exception('oceanbase has no ups master')
    except ExecutionError as err:
      logging.exception('ERROR: ' + str(err))
    finally:
      return ups_list
  except Exception as err:
    output = 'ERROR: ' + str(err)
    logging.exception(output)
    return None

def GetFileNumber(path):
  try:
    hadoop_ls_cmd = R('{hadoop_bin_dir}hadoop fs -ls {path}', locals())
    output = Shell.popen(hadoop_ls_cmd)
    num = 0
    for l in output.split('\n'):
      m = re.match(r'^[-d].* ([^ ]+)$', l)
      if m is not None:
        num += 1
    return num
  except Exception as err:
    output = 'ERROR: ' + str(err)
    logging.exception(output)
    return 0

def DeletePath(path):
  try:
    hadoop_ls_cmd = R('{hadoop_bin_dir}hadoop fs -rmr {path}', locals())
    output = Shell.popen(hadoop_ls_cmd)
    return 0
  except Exception as err:
    output = 'ERROR: ' + str(err)
    logging.exception(output)
    return 1

def ParseLoadedNum(output):
  try:
    loaded_num = int(output.strip().splitlines()[-1].split('=')[-1])
    return loaded_num
  except Exception as err:
    output = 'ERROR: ' + str(err)
    return 0

class GetFromHadoop:
  def __init__(self, connection, task):
    '''task should be a dict with these fields:
         dir
         files
    '''
    self.connection = connection
    self.task = task

  def __call__(self):
    dir = self.task['dir']
    files = self.task['files']
    dest_dir = dir
    tmp_dir = R('{dest_dir}/tmp', locals())
    for f in files:
      m = re.match(r'.*/([^/]+)', f)
      if m is not None:
        ts = int(time.time() * 1000000)
        filename = '{0}_{1}'.format(m.group(1), ts)
        hadoop_get_cmd = R('{hadoop_bin_dir}hadoop fs -get {f} {tmp_dir}/{filename}', locals())
        commit_mv_cmd = R('mv {tmp_dir}/{filename} {dest_dir}', locals())
        #rsync -e \"ssh -oStrictHostKeyChecking=no\" -avz --inplace --bwlimit=%ld src dst
        logging.debug(hadoop_get_cmd)
        logging.debug(commit_mv_cmd)
        Shell.sh(hadoop_get_cmd)
        Shell.sh(commit_mv_cmd)
        msg = R('Successfully get "{filename}" to "{dest_dir}"', locals())
        logging.info(msg)
        copy_ups_list = copy.copy(ups_slaves)
        for obi in obi_list:
          copy_ups_list.append(obi['master'])
        for h in copy_ups_list:
          rsync_cmd = R('rsync -e \"ssh -oStrictHostKeyChecking=no\" '
              '-avz --inplace --bwlimit=200000 '
              '{dest_dir}/{filename} {h}:{dest_dir}', locals())
          logging.debug(rsync_cmd)
          Shell.sh(rsync_cmd)
          l = R('Successfully push "{filename}" to ups slave '
              '{h}:{dest_dir}', locals())
          logging.info(l)
          SendPacket(self.connection, SUBMIT_IMPORT_TASK_RES_PKG, l + '\n')
        for obi in obi_list:
          for slave in obi['slaves']:
            rsync_cmd = R('rsync -e \"ssh -oStrictHostKeyChecking=no\" '
                '-avz --inplace --bwlimit=200000 '
                '{dest_dir}/{filename} {slave}:{dest_dir}', locals())
            logging.debug(rsync_cmd)
            master_addr = obi['master']
            Shell.sh(rsync_cmd, host = master_addr)
            l = R('Successfully push "{filename}" from {master_addr} '
                'to ups slave {slave}:{dest_dir}', locals())
            logging.info(l)
            SendPacket(self.connection, SUBMIT_IMPORT_TASK_RES_PKG, l + '\n')

def round_inc(n, ceiling):
  n += 1
  if n >= ceiling:
    n = 0
  return n

def CopySSTable(connection, wp, input_dir, data_dir_list):
  dir_num = len(data_dir_list)
  tasks = []
  for i in range(dir_num):
    tasks.append(dict(disk_id=None, files=[]))
  file_list = []

  hadoop_ls_cmd = R("{hadoop_bin_dir}hadoop fs -ls {input_dir}", locals())
  logging.debug(hadoop_ls_cmd)

  ls_output = Shell.popen(hadoop_ls_cmd).split('\n')
  for l in ls_output:
    m = re.match(r'^[-d].* ([^ ]+)$', l)
    if m is not None:
      file_list.append(m.group(1))
  logging.debug(file_list)

  disk_index = 0
  for f in file_list:
    if f != '':
      tasks[disk_index]['files'].append(f)
      disk_index = round_inc(disk_index, dir_num)

  for i in range(dir_num):
    tasks[i]['dir'] = data_dir_list[i]
    logging.debug(str(tasks[i]))
    wp.add_task(GetFromHadoop(connection, tasks[i]))

def ParseArgs():
  try:
    parser = optparse.OptionParser()
    parser.add_option('-f', '--config_file',
        help='Configuration file',
        dest='config_file', default='importserver.conf')
    (options, args) = parser.parse_args()
    config = ConfigParser.RawConfigParser()
    config.read(options.config_file)

    log_file    = config.get('public', 'log_file')
    log_level   = config.get('public', 'log_level')
    pid_file    = config.get('public', 'pid_file')
    if config.has_option('public', 'java_home'):
      java_home = config.get('public', 'java_home')
    else:
      java_home = '/usr/java/jdk1.6.0_13'
    if config.has_option('public', 'hadoop_home'):
      hadoop_home = config.get('public', 'hadoop_home')
    else:
      hadoop_home = '/home/hadoop/hadoop-current'
    if config.has_option('public', 'hadoop_conf_dir'):
      hadoop_conf_dir = config.get('public', 'hadoop_conf_dir')
    else:
      hadoop_conf_dir = '/home/admin/config'
    listen_port = config.getint('importserver', 'port')
    app_name    = config.get('importserver', 'app_name')
    conf_dir    = config.get('importserver', 'conf_dir')
    bin_dir     = config.get('importserver', 'bin_dir')
    hadoop_bin_dir = config.get('importserver', 'hadoop_bin_dir')
    bypass_dir  = config.get('importserver', 'bypass_dir')
    hdfs_name   = config.get('importserver', 'hdfs_name')
    if config.has_option('importserver', 'copy_sstable_concurrency'):
      copy_sstable_concurrency = config.getint('importserver', 'copy_sstable_concurrency')
    else:
      copy_sstable_concurrency = 8
    rs_ip       = config.get('rootserver', 'vip')
    rs_port     = config.getint('rootserver', 'port')
    if config.has_option('ob_instances', 'obi_count'):
      obi_count = config.getint('ob_instances', 'obi_count')
    else:
      obi_count = 0
    obi_rs      = []
    for i in range(obi_count):
      remote_rs_ip = config.get('ob_instances', 'obi{0}_rs_vip'.format(i))
      remote_rs_port = config.getint('ob_instances', 'obi{0}_rs_port'.format(i))
      if rs_ip != remote_rs_ip:
        obi_rs.append(dict(ip = remote_rs_ip, port = remote_rs_port))

    checkpid(pid_file)

    LEVELS = {'debug': logging.DEBUG,
        'info': logging.INFO,
        'warning': logging.WARNING,
        'error': logging.ERROR,
        'critical': logging.CRITICAL}
    level = LEVELS.get(log_level.lower(), logging.NOTSET)
    logging.basicConfig(filename=log_file, level=level,
        format='[%(asctime)s] %(levelname)s  %(funcName)s (%(filename)s:%(lineno)d) [%(thread)d] %(message)s')

    logging.info('pid_file    => ' + str(pid_file))
    logging.info('java_home   => ' + str(java_home))
    logging.info('hadoop_home => ' + str(hadoop_home))
    logging.info('hadoop_conf_dir => ' + str(hadoop_conf_dir))
    logging.info('listen_port => ' + str(listen_port))
    logging.info('app_name    => ' + app_name)
    logging.info('conf_dir    => ' + conf_dir)
    logging.info('bin_dir     => ' + bin_dir)
    logging.info('hadoop_bin_dir => ' + hadoop_bin_dir)
    logging.info('bypass_dir  => ' + bypass_dir)
    logging.info('hdfs_name   => ' + hdfs_name)
    logging.info('copy_sstable_concurrency => ' + str(copy_sstable_concurrency))
    logging.info('rs_ip       => ' + rs_ip)
    logging.info('rs_port     => ' + str(rs_port))
    logging.info('obi_count   => ' + str(obi_count))
    logging.info('obi_rs      => ' + str(obi_rs))
    globals()['pid_file']     = pid_file
    globals()['java_home']    = java_home
    globals()['hadoop_home']  = hadoop_home
    globals()['hadoop_conf_dir']  = hadoop_conf_dir
    globals()['listen_port']  = listen_port
    globals()['app_name']     = app_name
    globals()['conf_dir']     = conf_dir
    globals()['bin_dir']      = bin_dir
    globals()['hadoop_bin_dir'] = hadoop_bin_dir
    globals()['bypass_dir']   = bypass_dir
    globals()['hdfs_name']    = hdfs_name
    globals()['copy_sstable_concurrency'] = copy_sstable_concurrency
    globals()['rs_ip']        = rs_ip
    globals()['rs_port']      = rs_port
    globals()['obi_count']    = obi_count
    globals()['obi_rs']       = obi_rs

    os.putenv('JAVA_HOME', java_home)
    os.putenv('HADOOP_HOME', hadoop_home)
    os.putenv('HADOOP_CONF_DIR', hadoop_conf_dir)

  except Exception as err:
    logging.exception('ERROR: ' + str(err))

def Main():
  try:
    server = ImportServer(("", listen_port), RequestHandler)
    server.serve_forever()
  except Exception as err:
    logging.exception("ERROR: {0}".format(err))
  finally:
    if server is not None:
      server.shutdown()

SUBMIT_IMPORT_TASK_PKG     = 100
SUBMIT_IMPORT_TASK_RES_PKG = 101
SUBMIT_OVERWRITE_TASK_PKG     = 102
SUBMIT_OVERWRITE_TASK_RES_PKG = 103
ERROR_PKG = 200

if __name__ == '__main__':
  ParseArgs()
  Daemonize()
  Main()

