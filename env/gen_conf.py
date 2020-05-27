#!/bin/env python

import json
import os
from copy import deepcopy as copy

quorum = int(os.getenv('QUORUM'))


def port(i):
    return 9000 + i


nodes = range(quorum)

confs = {
    '%d.json' % (i,): {
        'max_batch': 1,
        'max_delay': 5,
        'id': i,
        'port': port(i),
        'pool_size': 3,
        'max_message': 8192,
        'members': [
            {'host': 'localhost', 'port': port(i)}
            for i in nodes
        ],
        'heartbeat_interval': 0.3,
        'heartbeat_timeout': 0.01,
        'election_timeout': 4,
        'rotate_interval': 200,
        'applied_backlog': 0,
        'flush_interval': 0.005,
        'timeout': 2,
        'rpc_max_batch': 10,
        'log': 'storage/%d.dir' % (i,)
    }
    for i in nodes
}

for fname, conf in confs.items():
    with open(fname, 'w') as fout:
        json.dump(conf, fout, indent=4)


client_conf = copy(next(iter(confs.values())))
client_conf['port'] = port(quorum)
del client_conf['id']
del client_conf['log']

with open('client.json', 'w') as fout:
    json.dump(client_conf, fout, indent=4)
