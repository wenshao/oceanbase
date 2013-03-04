check_local_file = 'sh: ls simple_client/{stress.sh,client} # ExceptionOnFail'
n_transport = 50
duration = -1
write_type = 'write'
client_env_vars = 'log_level=INFO n_transport=${n_transport} write_type=$write_type'
write=500
scan=0
mget=0
write_size=8
cond_size=2
scan_size=100
mget_size=100
client_start_args = '${obi.rs0.ip}:${obi.rs0.port} server=ms duration=$duration write=${write} scan=${scan} mget=${mget} write_size=$write_size cond_size=$cond_size scan_size=$scan_size mget_size=$mget_size'

def configure_obi(obi=None, **self):
    if not obi: raise Exception('no obi defined for simple_client')
    return 'configure_obi by simple_client'

def prepare(obi=None, **self):
    pass

def conf(**self):
    pass
