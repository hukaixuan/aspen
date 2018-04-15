# -*- coding: utf-8 -*-
import socket
import json
import time
from threading import Thread, Event

from state import Follower

from state_machine import StateMachine

class Server(object):
    """
    Server类定义服务节点，存储相关状态
    """
    def __init__(self, addr, cluster_addrs, state=None):
        self.addr = addr
        self.ip, self.port = self._get_ip_port_from_addr(self.addr)
        self.state = state if not (state is None) else Follower()
        self.currentTerm = 0
        self.votedFor = None
        self.voteCount = 0
        self.log = []
        self.state_machine = StateMachine()
        self.commitIndex = 0
        self.lasApplied = 0
        self.leader = None
        self.cluster_addrs = cluster_addrs
        self.otherServer_Addrs = list(filter(lambda x: x != self.addr, self.cluster_addrs))

        self.state.server = self

        # use udp for communication
        self._udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._udp_socket.bind((self.ip, self.port))
        self._udp_socket.settimeout(1)

        self.follower_timeout_event = Event()
        self.candidate_timeout_event = Event()

    def serve(self):
        run_thread = Thread(target=self.run)
        watcher_thread = Thread(target=self.receiving_msg)
        run_thread.start()
        watcher_thread.start()

    def run(self):
        while True:
            self.state.run()

    def send_msg_to(self, msg, to_addr):
        data = json.dumps(msg).encode()
        self._udp_socket.sendto(data, self._get_ip_port_from_addr(to_addr))
        # print('type:{}, from:{}, to:{}, time:{}, msg:{}'.format(msg.get('type'), self.addr, to_addr, time.time(), data))

    def receiving_msg(self):
        while True:
            try:
                data, from_addr = self._udp_socket.recvfrom(65535)
                msg = json.loads(data.decode())
                self.state.on_message(msg)
            except socket.timeout:
                pass

    def broadcast(self, msg, addrs=None):
        if addrs is None:
            addrs = self.otherServer_Addrs
        for addr in addrs:
            self.send_msg_to(msg, addr)

    def _get_ip_port_from_addr(self, addr):
        return addr.split(':')[0], int(addr.split(':')[1])
    