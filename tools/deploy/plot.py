#!/usr/bin/env python2
'''
Usages:
 ./plot.py type output.img src.input1 src.input2...
'''

import sys
import os, os.path
import re
import pprint
import traceback
import random
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

class Fail(Exception):
    def __init__(self, msg, obj=None):
        self.msg, self.obj = msg, obj

    def __repr__(self):
        return 'Fail:%s %s'%(self.msg, self.obj != None and pprint.pformat(self.obj) or '')

    def __str__(self):
        return repr(self)

def read(path):
    with file(path) as f:
        return f.read()

def get_load(path, labels):
    data = [(int(tps), int(scan_qps), int(get_qps)) for tps, scan_qps, get_qps in re.findall('\[tps=(\d+):.*\[qps=(\d+):.*\[qps=(\d+):', read(path))]
    if not data: raise Fail('no data extract', path)
    return np.transpose(data), ['%s:%s'%(labels.get(path), type) for type in 'write scan get'.split()]

def get_fail_count(path, labels):
    data = re.findall('write=\[total=(\d+):(\d+).*scan=\[total=(\d+):(\d+).*mget=\[total=(\d+):(\d+)', read(path))
    if not data: raise Fail('no data extract', path)
    return map(np.diff, np.transpose([map(int, req) for req in data])), ['%s:%s'%(labels.get(path), type) for type in 'write_total write_fail scan_total scan_fail get_total get_fail'.split()]
    
def plot_multi_seq(img, *seq, **attr):
    plt.clf()
    for label, data, linestyle in seq:
        plt.plot(data, linestyle, label='%s:%.2g'%(label, np.mean(data)))
    plt.xlabel(attr.get('xlabel'))
    plt.ylabel(attr.get('ylabel'))
    #plt.legend(loc="upper center",bbox_to_anchor=(0., 1.02, 1., .102), ncol=len(seq), mode="expand", borderaxespad=0.)
    lgd = plt.legend(loc="center left", bbox_to_anchor=(1.0, 0.5), borderaxespad=0.)
    plt.savefig(img, bbox_extra_artists=(lgd,), bbox_inches='tight')
    return plt

def plot_load(path, **attr):
    qtys, labels = get_load(path, attr.get('labels'))
    return zip(labels, qtys, '+ + +'.split()), dict(xlabel='time', ylabel='tps:qps')

def plot_fail(path, **attr):
    qtys, labels = get_fail_count(path, attr.get('labels'))
    return zip(labels, qtys, 'r+ rx g+ gx b+ bx'.split()), dict(xlabel='time', ylabel='total:fail')

def plot_tps(*path_list, **attr):
    def get_tps(path):
        qtys, labels = get_load(path, attr.get('labels'))
        return labels[0], qtys[0], '+'
    return map(get_tps, path_list), dict(xlabel='time', ylabel='tps')

def usages():
    sys.stderr.write(__doc__)
    sys.exit(1)

def do_plot(type, img, *path_list):
    def get_label(path):
        m = re.match('[^.]+?\.([^:]+)', os.path.basename(path))
        if not m: raise Fail('ill formaled path!', path)
        return m.group(1)
    print 'do_plot(%s, %s, %s)'%(type, img, path_list)
    func = globals().get('plot_%s'%(type), None)
    if not callable(func): raise Fail('func not exist', type)
    labels = dict([(p, get_label(p)) for p in path_list])
    seq, attr = func(*path_list, labels=labels)
    plot_multi_seq(img, *seq, **attr)
    return img

if __name__ == '__main__':
    len(sys.argv) >= 4 or usages()
    try:
        print do_plot(*sys.argv[1:])
    except Fail as e:
        print e
    except Exception as e:
        print e
        print traceback.format_exc()
