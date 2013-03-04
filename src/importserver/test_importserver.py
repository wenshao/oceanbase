#!/usr/bin/python2.6

import unittest

def R(cmd, local_vars):
  return ""

class Shell:
  @classmethod
  def popen(cls, cmd, host=None, username=None):
    return cls.output

class TestFuncs(unittest.TestCase):
  def test_GetSlaveList1(self):
    Shell.output = '''do_rs_stat, key=19...
lease_left=8648571|172.24.131.51:2700(2701 master 0 0 422944353 MASTER),'''
    self.assertEqual(GetUpsList(dict(ip='dummy', port='dummy'))['slaves'], [])

  def test_GetSlaveList2(self):
    Shell.output = '''do_rs_stat, key=19...
lease_left=8663221|172.24.131.195:2700(2701 sync 0 0 72153762 MASTER),172.24.131.196:2700(2701 master 0 0 72153762 MASTER),'''
    self.assertEqual(GetUpsList(dict(ip='dummy', port='dummy')),
        dict(master = '172.24.131.196', slaves = ['172.24.131.195']))

  def test_GetFileNumber(self):
    Shell.output = '''Found 15 items
-rw-r-----   3 yanran.hfs cug-tbptd-dev 5557257992 2012-10-10 13:01 /group/tbptd-dev/yanran.hfs/mrrunner/tmp_bidword/p4p/2012-10-10/rpt_bpp4p_bidword_ob/part-00000-1002-000000
-rw-r-----   3 yanran.hfs cug-tbptd-dev 4994658682 2012-10-10 13:27 /group/tbptd-dev/yanran.hfs/mrrunner/tmp_bidword/p4p/2012-10-10/rpt_bpp4p_bidword_ob/part-00001-1002-000001
-rw-r-----   3 yanran.hfs cug-tbptd-dev 3948613128 2012-10-10 12:51 /group/tbptd-dev/yanran.hfs/mrrunner/tmp_bidword/p4p/2012-10-10/rpt_bpp4p_bidword_ob/part-00002-1002-000002
-rw-r-----   3 yanran.hfs cug-tbptd-dev 5345736481 2012-10-10 13:12 /group/tbptd-dev/yanran.hfs/mrrunner/tmp_bidword/p4p/2012-10-10/rpt_bpp4p_bidword_ob/part-00003-1002-000003
-rw-r-----   3 yanran.hfs cug-tbptd-dev 4723579927 2012-10-10 13:51 /group/tbptd-dev/yanran.hfs/mrrunner/tmp_bidword/p4p/2012-10-10/rpt_bpp4p_bidword_ob/part-00004-1002-000004
-rw-r-----   3 yanran.hfs cug-tbptd-dev 4388441680 2012-10-10 12:51 /group/tbptd-dev/yanran.hfs/mrrunner/tmp_bidword/p4p/2012-10-10/rpt_bpp4p_bidword_ob/part-00005-1002-000005
-rw-r-----   3 yanran.hfs cug-tbptd-dev 5212664638 2012-10-10 13:10 /group/tbptd-dev/yanran.hfs/mrrunner/tmp_bidword/p4p/2012-10-10/rpt_bpp4p_bidword_ob/part-00006-1002-000006
-rw-r-----   3 yanran.hfs cug-tbptd-dev 4322449038 2012-10-10 13:02 /group/tbptd-dev/yanran.hfs/mrrunner/tmp_bidword/p4p/2012-10-10/rpt_bpp4p_bidword_ob/part-00007-1002-000007
-rw-r-----   3 yanran.hfs cug-tbptd-dev 4675327973 2012-10-10 12:53 /group/tbptd-dev/yanran.hfs/mrrunner/tmp_bidword/p4p/2012-10-10/rpt_bpp4p_bidword_ob/part-00008-1002-000008
-rw-r-----   3 yanran.hfs cug-tbptd-dev 4196412014 2012-10-10 13:10 /group/tbptd-dev/yanran.hfs/mrrunner/tmp_bidword/p4p/2012-10-10/rpt_bpp4p_bidword_ob/part-00009-1002-000009
-rw-r-----   3 yanran.hfs cug-tbptd-dev 4604049336 2012-10-10 12:30 /group/tbptd-dev/yanran.hfs/mrrunner/tmp_bidword/p4p/2012-10-10/rpt_bpp4p_bidword_ob/part-00010-1002-000010
-rw-r-----   3 yanran.hfs cug-tbptd-dev 4958604451 2012-10-10 12:56 /group/tbptd-dev/yanran.hfs/mrrunner/tmp_bidword/p4p/2012-10-10/rpt_bpp4p_bidword_ob/part-00011-1002-000011
-rw-r-----   3 yanran.hfs cug-tbptd-dev 4413709246 2012-10-10 12:52 /group/tbptd-dev/yanran.hfs/mrrunner/tmp_bidword/p4p/2012-10-10/rpt_bpp4p_bidword_ob/part-00012-1002-000012
-rw-r-----   3 yanran.hfs cug-tbptd-dev 4969421522 2012-10-10 13:19 /group/tbptd-dev/yanran.hfs/mrrunner/tmp_bidword/p4p/2012-10-10/rpt_bpp4p_bidword_ob/part-00013-1002-000013
-rw-r-----   3 yanran.hfs cug-tbptd-dev 4725608933 2012-10-10 12:56 /group/tbptd-dev/yanran.hfs/mrrunner/tmp_bidword/p4p/2012-10-10/rpt_bpp4p_bidword_ob/part-00014-1002-000014
'''
    self.assertEqual(GetFileNumber(''), 15)

  def test_invoke_load_bypass(self):
    Shell.output = '''[load_bypass] err=0
loaded_num=1
'''
    self.assertEqual(ParseLoadedNum(Shell.output), 1)

  def test_ClearBypassDir(self):
    Shell.output = ''
    ClearBypassDir('.', [])

if __name__ == '__main__':
  import importserver
  oldR = importserver.R
  oldShell = importserver.Shell
  importserver.R = R
  importserver.Shell = Shell
  from importserver import *
  unittest.main()
