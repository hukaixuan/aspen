# -*- coding: utf-8 -*-
import sys
import time
from threading import Thread
from collections import namedtuple
from urllib import request

from flask import Flask

from aspen.server import Server
from aspen.command import CommandType
from aspen.utils.log import logger

app = Flask(__name__)

cluster_addrs = ['127.0.0.1:9000', '127.0.0.1:9001', '127.0.0.1:9002', '127.0.0.1:9003', '127.0.0.1:9004']
addr = sys.argv[1]
server = Server(addr, cluster_addrs)

Entry = namedtuple('Entry', ['term', 'command'])

def execute(command):
    """
    记录日志并等待大多数client响应，commit到state machine
    """
    server.log.append(Entry(server.currentTerm, command))
    current_log_index = len(server.log)
    server.state._refresh_nextIndex()
    logger.debug(server.log)
    while True:
        # log entry 被大多数节点复制
        is_commited = (server.commitIndex == current_log_index)
        # 该entry还未执行
        has_not_executed = (server.lastApplied < server.commitIndex)
        if is_commited and has_not_executed:
            entry = server.log[current_log_index-1]
            logger.debug(entry.command)
            res = server.state_machine.execute(entry.command)
            server.lastApplied = server.commitIndex
            return res
    return ''

def redirect_to_leader(server, path):
    leader_ip = server.leader.split(':')[0]
    leader_port = str(int(server.leader.split(':')[1]) - 1000)
    url = 'http://' + leader_ip + ':' + leader_port + path
    logger.debug('redirect to url:{}'.format(url))
    u = request.urlopen(url)
    resp = u.read().decode(u.headers.get_content_charset())
    return str(resp)

@app.route('/get/<key>')
def get_value(key):
    if server.leader == server.addr:
        logger.debug('get value from self')
        command = {
            'type': CommandType.GET,
            'argv': [key,]
        }
        res = server.state_machine.execute(command)
        return res if res else ''
    else:
        logger.debug('get value from leader')
        return redirect_to_leader(server, '/get/'+key)

@app.route('/set/<key>/<value>')
def set_value(key, value):
    if server.leader == server.addr:
        command = {
            'type': CommandType.SET,
            'argv': [key, value]
        }
        res = str(execute(command))
        return res if res else ''
    else:
        return redirect_to_leader(server, '/set/'+key+'/'+value)

@app.route('/items')
def get_all_items():
    if server.leader == server.addr:
        command = {
            'type': CommandType.ITEMS,
            'argv': []
        }
        res = server.state_machine.execute(command)
        return res if res else ''
    else:
        return redirect_to_leader(server, '/items')

@app.route('/del/<key>')
def del_value(key):
    if server.leader == server.addr:
        command = {
            'type': CommandType.DELETE,
            'argv': [key,]
        }
        return str(execute(command))
    else:
        return redirect_to_leader(server, '/del/'+key)


def commit_logEntry_2_statemachine():
    """
    commit logEntry to statemachine(for follower)
    """
    logger.debug('running commit logEntry to statemachine thread....')
    while True:
        if (
            server.addr != server.leader and
            server.commitIndex > server.lastApplied
        ):
            for entry in server.log[server.lastApplied:server.commitIndex]:
                logger.debug(server.log)
                logger.debug(entry.command)
                res = server.state_machine.execute(entry.command)
                logger.debug(res)
            server.lastApplied = server.commitIndex
        time.sleep(1)

    
if __name__ == '__main__':
    # client_thread = Thread(target=app.run, args=(server.ip, server.port-1000))
    client_thread = Thread(target=app.run, args=('0.0.0.0', server.port-1000))
    commit_entry_thread = Thread(target=commit_logEntry_2_statemachine)
    client_thread.start()
    commit_entry_thread.start()
    server.serve()
    