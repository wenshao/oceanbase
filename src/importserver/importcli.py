#!/usr/bin/python

import optparse
import logging
import struct
import pickle
import socket
import sys

def ParseArgs():
  usage = ('usage: %prog [options] '
      '[input_path1 table_name1 input_path2 table_name2 ...]')
  parser = optparse.OptionParser(usage)
  parser.add_option('-s', '--server',
      help='Import server address, default is localhost',
      dest='server', default='localhost')
  parser.add_option('-p', '--port',
      help='Import server port, default is 2900',
      dest='port', default='2900')
  parser.add_option('-t', '--import_type',
      help='Type of import operation, OVERWRITE or IMPORT, default is IMPORT',
      dest='import_type', default='IMPORT')
  parser.add_option('-i', '--import_conf',
      help=('Import configuration file including input paths and table names, '
        'this file is optional. If this argument is set, '
        'the arguments in command line is forbidden'),
      dest='import_conf')
  (options, args) = parser.parse_args()
  import_type = options.import_type
  if options.import_conf is not None:
    context = dict()
    execfile(options.import_conf, context)
    if context.has_key('inputs') and context['inputs'] is not None:
      inputs = context['inputs']
    else:
      parser.error('import configuration file format is error, '
          '"inputs" can not be parsed successfully')
    if context.has_key('import_type') and context['import_type'] is not None:
      import_type = context['import_type']
    if len(args) != 0:
      parser.error('import configuration and arguments are mutually exclusive')
  else:
    inputs = []
    if len(args) % 2 != 0 or len(args) == 0:
      parser.error("the args is wrong")
    else:
      for i in range(0, len(args), 2):
        inputs.append((args[i], args[i+1]))

  logging.debug('server      => ' + str(options.server))
  logging.debug('port        => ' + str(options.port))
  logging.debug('inputs      => ' + str(inputs))
  logging.debug('import_type => ' + str(import_type))
  globals()['server']      = options.server
  globals()['port']        = options.port
  globals()['inputs']      = inputs
  globals()['import_type'] = import_type

class SocketManager:
  def __init__(self, address):
    self.address = address
  def __enter__(self):
    self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    self.sock.connect(self.address)
    return self.sock
  def __exit__(self, *ignore):
    self.sock.close()

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
  sock.sendall(packetheader)
  sock.sendall(buf)

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

def SendRequest():
  try:
    with SocketManager((server, int(port))) as sock:
      if import_type.upper() == 'OVERWRITE':
        SendPacket(sock, SUBMIT_OVERWRITE_TASK_PKG, inputs)
      elif import_type.upper() == 'IMPORT':
        SendPacket(sock, SUBMIT_IMPORT_TASK_PKG, inputs)
      else:
        raise Exception('Wrong import type: {0}'.format(import_type))
      while True:
        (packettype, data) = ReceivePacket(sock)
        if packettype is None:
          break;
        if len(data) == 0:
          print('The server returned nothing before closing the connection...')
          sys.exit(1)
        else:
          if data == 'FAILED':
            print('Failed')
            sys.exit(1)
          elif data == 'SUCCESSFUL':
            break
          else:
            sys.stdout.write(data)
  except socket.error as err:
    print('Network error: {0}'.format(err))
    sys.exit(1)

SUBMIT_IMPORT_TASK_PKG        = 100
SUBMIT_IMPORT_TASK_RES_PKG    = 101
SUBMIT_OVERWRITE_TASK_PKG     = 102
SUBMIT_OVERWRITE_TASK_RES_PKG = 103

if __name__ == '__main__':
  ParseArgs()
  SendRequest()

