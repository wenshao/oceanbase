## Config File for 'deploy.py', this file is just a valid Python file.
## Note: the servers' port will be determined by the 'OBI's definition order in this config file,
##       unless your explict configure them. (that means Don't change 'OBI's definition order if not necessary)

## comment line below if you need to run './deploy.py ob1.random_test ...'
load_file('monitor.py', 'fault_test.py')
data_dir = '/data/'         # $data_dir/{1..10} should exist
## comment line below if you want to provide custom schema and server conf template(see ob5's definition for another way)
## run: './deploy.py tpl.gensvrcfg' to generate a 'tpl' dir, edit tpl/rootserver.template... as you wish 
# tpl = load_dir_vars('tpl')  # template of config file such as rootserver.conf/updateserver.conf...

ObCfg.all_hosts = '''10.232.36.29 10.232.36.30 10.232.36.31 10.232.36.32 10.232.36.33
 10.232.36.42 10.232.36.171 10.232.36.175 10.232.36.176 10.232.36.177'''.split()
# list ip of test machines
ObCfg.default_hosts = '10.232.36.175 10.232.36.176 10.232.36.177'.split()
ob1 = OBI(ct=CT('mixed_test'))
ob2 = OBI('OBI_MASTER', max_sstable_size=4194304, masters = ObCfg.default_hosts[:2], ct=CT('bigquery', hosts = ObCfg.default_hosts[:1]))
ob3 = OBI('OBI_MASTER', masters = ObCfg.default_hosts[:1], ct=CT('syschecker', hosts = ObCfg.default_hosts[:2]))
mysql = {
"ip" : "10.232.36.187",
"port" : "3306",
"user" : "sqltest",
"pass" : "sqltest",
"db" : ObCfg.usr.replace('.', '_'),
}
ob4 = OBI('OBI_MASTER', masters = ObCfg.default_hosts[:2], ct=CT('sqltest', mysql=mysql, hosts = ObCfg.default_hosts[:2]))
ob5 = OBI('OBI_MASTER', masters = ObCfg.default_hosts[:2], ct=CT('sqltest', mysql=mysql, hosts = ObCfg.default_hosts[:2]))
# rpm21_url = 'http://upload.yum.corp.taobao.com/taobao/5/x86_64/test/oceanbase/oceanbase-0.2.1-655.el5.x86_64.rpm'
# rpm30_url = 'http://upload.yum.corp.taobao.com/taobao/5/x86_64/test/oceanbase/oceanbase-0.3.0-674.el5.x86_64.rpm'
# svn30_url = 'http://svn.app.taobao.net/repos/oceanbase/branches/rev_0_3_0_ii_dev/oceanbase'
# local30_dir = '~/ob.30'
# ob1 = OBI('OBI_MASTER', src=rpm30_url, ver='.30', ct=CT('mixed_test'))
# tr0 = TR('ob1', case_src_pat='test/A0*.py')
# tr1 = TR('ob1', case_src_pat='test/A1*.py', case_pat='one_cluster')

# ob2 = OBI('OBI_MASTER', src=rpm30_url, ver='.30', ct=CT('mixed_test'))
# obis = dict(inst1='ob3', inst2='ob4')

## replace ob3/ob4 with following definition to use custom schema and lsync
# h0, h1 =ObCfg.default_hosts[:2]
## ob1/ob2 common attributes, to union ob1/ob2 as MASTER/SLAVE cluster, need define inst1='ob1', inst2='ob2'
# obis = dict(inst1='ob3', inst2='ob4', masters = [h0], tpl = dict(schema_template=read('clientTest.schema')), need_lsync=True)
# ob3 = OBI('OBI_SLAVE', src=rpm21_url, ver='.21',
# 		lsync0=dict(convert_switch_log=0, port=3045),
# 		ups0=dict(lsync_ip=h0, lsync_port=3046),
# 		**obis)
# ob4 = OBI('OBI_MASTER', src=local30_dir, ver='.30',
# 		lsync0=dict(convert_switch_log=1, port=3046),
# 		ups0=dict(lsync_ip=h0, lsync_port=3045), ct = CT('javaclient', hosts=[h0]),
#           **obis)
# tr2 = TR('ob4', case_src_pat='test/A1*.py', case_pat='two_cluster')
# ob5 = OBI('OBI_MASTER', src=svn30_url, ver='.30svn')

# ## explict config each server and their attributes
# rs0_ip = '10.232.36.31'
# rs1_ip = '10.232.36.33'
# ups0_ip = '10.232.36.29'
# cs0_ip = '10.232.36.31'
# ms0_ip = '10.232.36.31'
# cli0_ip = '10.232.36.33'

# # config 'port', 'version' and 'schema' etc, no 'client'
# ## Note: if you set 'ver', updateserver/rootserver/... should be in dir 'bin.$ver'
# ob5 = OBI('OBI_MASTER', ver='.30',
#           ## comment lines below to explit configure default servers' ports using by this 'OBI'
#           # rs_port = 2544,
#           # ups_port = 2644,
#           # ups_inner_port = 2744,
#           # cs_port = 2844,
#           # ms_port = 2944,
# 	  # tpl = dict(schema_template=read('yubai.schema')), # Note: client will reset schema
#                     rs0 = RootServer(rs0_ip),
#                     rs1 = RootServer(rs1_ip),
#                     ups0 = UpdateServer(ups0_ip, replay_checksum_flag=1),
#                     cs0 = ChunkServer(cs0_ip),
#                     ms0 = MergeServer(ms0_ip),
# 		    ct = CT('syschecker', hosts=[cli0_ip]),
#                     ## comment line below to explict configure 'ms1' port
#                     # ms1 = ChunkServer(ms0_ip),
# )

# ## config two OBI, UPS sync using lsync
# ob6 = OBI('OBI_MASTER', ver='.21',
#                     rs0 = RootServer(rs0_ip),
#                     ups0 = UpdateServer(ups0_ip, lsync_ip='', lsync_port=3046),
#                     cs0 = ChunkServer(cs0_ip),
#                     ms0 = MergeServer(ms0_ip),
# 		    lsync0 = LsyncServer(ups0_ip, port=3045, lsync_retry_wait_time_us=100000, log_level='debug'),
# 		    ct = CT('client_simulator', hosts = [cli0_ip]))
# ob7 = OBI('OBI_MASTER', ver='.30',
# 		    lsyc_port = 3046,
#                     rs0 = RootServer(rs0_ip),
#                     ups0 = UpdateServer(ups0_ip, lsync_ip= ups0_ip, lsync_port = 3045),
# 		    lsync0 = LsyncServer(ups0_ip, port=3046, convert_switch_log=1, lsync_retry_wait_time_us=100050, log_level='debug'),
#                     cs0 = ChunkServer(cs0_ip),
#                     ms0 = MergeServer(ms0_ip),
# 		    ct = CT('client_simulator', hosts = [cli0_ip]))

# ob8 = OBI('OBI_MASTER', ver='.dev-0.3', ct=CT('mixed_test'))
