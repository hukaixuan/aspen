# -*- coding: utf-8 -*-
import sys
from threading import Thread
from collections import namedtuple
from urllib import request

from flask import Flask

from server import Server
from command import CommandType

app = Flask(__name__)

cluster_addrs = ['127.0.0.1:9000', '127.0.0.1:9001', '127.0.0.1:9002', '127.0.0.1:9003', '127.0.0.1:9004']
addr = sys.argv[1]
server = Server(addr, cluster_addrs)

Entry = namedtuple('Entry', ['term', 'command'])

def execute(command):
    server.log.append(Entry(server.currentTerm, command))
    current_log_index = len(server.log)
    server.state._refresh_nextIndex()
    print(server.log)
    while True:
        # print(server.commitIndex, current_log_index)
        if server.commitIndex == current_log_index:
            entry = server.log[current_log_index-1]
            res = server.state_machine.execute(entry.command)
            server.lastApplied = server.commitIndex
            return res

def redirect_to_leader(server, path):
    leader_ip = server.leader.split(':')[0]
    leader_port = str(int(server.leader.split(':')[1]) - 1000)
    url = 'http://' + leader_ip + ':' + leader_port + path
    print('redirect to url:', url)
    u = request.urlopen(url)
    resp = u.read()
    return str(resp)

@app.route('/get/<key>')
def get_value(key):
    if server.leader == server.addr:
        print('get value from self')
        command = {
            'type': CommandType.GET,
            'argv': (key,)
        }
        return execute(command)
    else:
        print('get value from leader')
        return redirect_to_leader(server, '/get/'+key)

@app.route('/set/<key>/<value>')
def set_value(key, value):
    if server.leader == server.addr:
        command = {
            'type': CommandType.SET,
            'argv': (key, value)
        }
        return str(execute(command))
    else:
        return redirect_to_leader(server, '/set/'+key+'/'+value)

@app.route('/items')
def get_all_items():
    if server.leader == server.addr:
        command = {
            'type': CommandType.ITEMS,
            'argv': ()
        }
        return str(execute(command))
    else:
        return redirect_to_leader(server, '/items')

@app.route('/del/<key>')
def del_value(key):
    if server.leader == server.addr:
        command = {
            'type': CommandType.DELETE,
            'argv': (key,)
        }
        return str(execute(command))
    else:
        return redirect_to_leader(server, '/del/'+key)
    

if __name__ == '__main__':
    client_thread = Thread(target=app.run, args=(server.ip, server.port-1000))
    client_thread.start()
    server.serve()
    