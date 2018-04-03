# -*- coding: utf-8 -*-
import random
import time
import math
from threading import Event
from collections import namedtuple

Entry = namedtuple('Entry', ['term', 'command'])

class MessageType(object):
    REQUEST_VOTE = 0
    RESPONSE_TO_VOTEREQUEST = 1
    APPENDENTRIES = 2
    RESPONSE_TO_APPENDENTRIES = 3
    CLIENT_COMMAND = 4  


class State(object):
    """
    节点状态基类，只定义逻辑，不存储状态
    各State分别定义不同的方法处理不同逻辑，
    通过 self._server 获取当前节点存储的属性状态
    """

    def set_server(self, server):
        """
        设置状态所属节点，**随着节点状态的改变，所属的server一直传下去了，没有改变，节点的状态也一直保持下去**
        """
        if server:
            self.server = server
            self.server.state = self

    def on_message(self, msg):
        """
        收到消息时的处理逻辑
        """
        # print(msg)
        if msg.get('term', 0) > self.server.currentTerm:
            self.server.currentTerm = msg.get('term')
            self.change_to_follower()
        if msg.get('type') == MessageType.REQUEST_VOTE:
            self.on_requestVote_message(msg)
        elif msg.get('type') == MessageType.APPENDENTRIES:
            self.on_appendentries_message(msg)
        elif msg.get('type') == MessageType.RESPONSE_TO_VOTEREQUEST:
            self.on_voteRequest_response_message(msg)
        elif msg.get('type') == MessageType.RESPONSE_TO_APPENDENTRIES:
            self.on_appendentries_response_message(msg)
        elif msg.get('type') == MessageType.CLIENT_COMMAND:
            self.on_client_command_message(msg)

    def change_to_state(self, state):
        state.set_server(self.server)
        state.server.voteCount = 0
        state.server.votedFor = None
        print(time.time())

    def change_to_candidate(self):
        print('STATE CHANGED --- become candidate')
        candidate = Candidate()
        self.change_to_state(candidate)

    def change_to_follower(self):
        print('STATE CHANGED --- become follower')
        follower = Follower()
        self.change_to_state(follower)

    def change_to_leader(self):
        self.server.candidate_timeout_event.clear()
        print('STATE CHANGED --- become leader')
        leader = Leader()
        self.change_to_state(leader)
        leader.init_run()

    def on_requestVote_message(self, msg):
        pass

    def on_appendentries_message(self, msg):
        pass

    def on_voteRequest_response_message(self, msg):
        pass

    def on_appendentries_response_message(self, msg):
        pass

    def on_client_command_message(self, msg):
        pass


class Follower(State):
    """
    Follower 状态
    """
    def __init__(self):
        super().__init__()

    def run(self):
        # 当其他地方执行self._timeout_event.set()方法时会终止wait
        self.server.follower_timeout_event.wait(self._gen_timeout())
        
        # 如果有其他地方触发set(), 重置并进行下一轮timeout
        if self.server.follower_timeout_event.is_set():
            # print('Term[{}] reset timeout {}'.format(self.server.currentTerm, time.time()))
            self.server.follower_timeout_event.clear()
        # 否则说明在此次timeout过程中，没有触发set(触发没有收到其他节点的消息)，timeout完成，成为candidate
        else:
            self.change_to_candidate()

    def on_appendentries_message(self, msg):
        self.server.follower_timeout_event.set()
        term = msg.get('term')
        from_addr = msg.get('from_addr')
        prevLogIndex = msg.get('prevLogIndex')
        prevLogTerm = msg.get('prevLogTerm')
        entries = [Entry(entry[0], entry) for entry in msg.get('entries')]
        leaderCommit = msg.get('leaderCommit')

        resp_msg = {
            'type': MessageType.RESPONSE_TO_APPENDENTRIES,
            'addr': self.server.addr,
            'term': self.server.currentTerm,
            'success': False
        }

        # leader 任期落后
        if term < self.server.currentTerm:
            self.server.send_msg_to(resp_msg, from_addr)
        
        # 未能匹配到一致的prev log
        elif(
            len(self.server.log) < prevLogIndex 
            or (prevLogIndex>0 and self.server.log[prevLogIndex-1].term != prevLogTerm)
        ):
            self.server.send_msg_to(resp_msg, from_addr)

        # 匹配到了一致的 prev log
        else:
            del self.server.log[prevLogIndex:]
            self.server.log.extend(entries)
            self.server.commitIndex = min(leaderCommit, len(self.server.log))
            resp_msg['matchIndex'] = len(self.server.log)
            resp_msg['success'] = True
            self.server.send_msg_to(resp_msg, from_addr)
            if entries:
                print('log:', self.server.log)
            

    def on_requestVote_message(self, msg):
        term = msg.get('term')
        from_addr = msg.get('from_addr')
        lastLogIndex = msg.get('lastLogIndex')
        lastLogTerm = msg.get('lastLogTerm')

        self_lastLogIndex = len(self.server.log)
        self_lastLogTerm = self.server.log[-1].term if self.server.log else 0
        # 如果Candidate的Term不小于当前的currentTerm，并且当前任期内没有为其他节点投票，
        # 并且Candidate的日志至少和当前节点的日志一样新，则投赞同票
        if (
            term >= self.server.currentTerm and self.server.votedFor is None
            and lastLogTerm >= self_lastLogTerm and lastLogIndex >= self_lastLogIndex
        ):
            self.server.votedFor = from_addr
            self.server.send_msg_to({
                'type': MessageType.RESPONSE_TO_VOTEREQUEST,
                'term': self.server.currentTerm,
                'from_addr': self.server.addr,
                'voteGranted': True,
            }, from_addr)
        # 否则投反对票
        else:
            self.server.send_msg_to({
                'type': MessageType.RESPONSE_TO_VOTEREQUEST,
                'term': self.server.currentTerm,
                'from_addr': self.server.addr,
                'voteGranted': False,
            }, from_addr)
        

    def _gen_timeout(self, start=0.15, end=0.3):
        """
        生成start到end范围之间的timeout
        """
        return random.uniform(start, end)


class Candidate(State):
    """
    Candidate 状态
    """
    def __init__(self):
        super().__init__()

    def run(self):
        self.do_election()
        self.server.candidate_timeout_event.wait(self._gen_timeout())

    def do_election(self):
        self.server.currentTerm += 1
        self.server.voteCount = 0
        print('Term[{}] do election...{}'.format(self.server.currentTerm, time.time()))
        self.server.votedFor = self.server.addr
        self.server.voteCount += 1
        self.server.broadcast({
            'type': MessageType.REQUEST_VOTE, 
            'term': self.server.currentTerm,
            'from_addr': self.server.addr,
            'lastLogIndex': len(self.server.log),
            'lastLogTerm': self.server.log[-1].term if self.server.log else 0,
        })

    def on_voteRequest_response_message(self, msg):
        term = msg.get('term')
        voteGranted = msg.get('voteGranted')
        # 如果收到同意投票的消息
        if voteGranted:
            self.server.voteCount += 1
        # 若得到大多数节点的选票，成为leader
        if self.server.voteCount*2 > len(self.server.cluster_addrs):
            self.server.candidate_timeout_event.set()
            self.change_to_leader()

    def on_appendentries_message(self, msg):
        term = msg.get('term')
        if term >= self.server.currentTerm:
            self.change_to_follower()

    def _gen_timeout(self, start=0.15, end=0.3):
        """
        生成start到end范围之间的timeout
        """
        return random.uniform(start, end)


class Leader(State):
    """
    Leader 状态
    """
    def __init__(self):
        super().__init__()
        self.heartbeat_interval = 0.1
        self.nextIndex = {}
        self.matchIndex = {}

    def init_run(self):
        for addr in self.server.otherServer_Addrs:
            self.matchIndex[addr] = 0
        self._refresh_nextIndex()

    def run(self):
        self.append_entries()

    def append_entries(self):
        for addr in self.server.otherServer_Addrs:
            if addr in self.nextIndex.keys() and self.nextIndex.get(addr)>1:
                prevLogIndex = self.nextIndex.get(addr) - 1  
                prevLogTerm = self.server.log[prevLogIndex-1].term
            else:
                prevLogIndex = 0
                prevLogTerm = 0
            msg = {
                'type': MessageType.APPENDENTRIES,
                'term': self.server.currentTerm,
                'from_addr': self.server.addr,   # leader_id
                'prevLogIndex': prevLogIndex,
                'prevLogTerm': prevLogTerm,
                'entries': self.server.log[prevLogIndex:],
                'leaderCommit': self.server.commitIndex,
            }
            self.server.send_msg_to(msg, addr)
        time.sleep(self.heartbeat_interval)
        # print('Term[{}]leader is doing heartbeat...'.format(self.server.currentTerm))

    def on_client_command_message(self, msg):
        """
        Client发来命令
        """
        # Append entry to local log, response after entry applied to state machine
        command = msg.get('command')
        self.server.log.append(Entry(self.server.currentTerm, command))
        self._refresh_nextIndex()
        print(self.server.log)

    def on_appendentries_response_message(self, msg):
        # print('appendentries respone msg: {}'.format(msg))
        prev_log_match = msg.get('success')
        addr = msg.get('addr')
        # print(self.nextIndex.keys(), self.nextIndex.get(addr))
        if prev_log_match:
            matchIndex = msg.get('matchIndex')
            self.matchIndex[addr] = matchIndex
            self.nextIndex[addr] = len(self.server.log) + 1
            self.server.commitIndex = self._get_majority_minNum(self.matchIndex.values())
        else:
            if(addr in self.nextIndex.keys() and self.nextIndex.get(addr)>0):
                self.nextIndex[addr] -= 1
        # print('='*50)
        # print('AppppResp....')
        # print(msg)
        # print(self.nextIndex)
        # print(self.matchIndex)
        # print('='*50)

    def _refresh_nextIndex(self):
        for addr in self.server.otherServer_Addrs:
            self.nextIndex[addr] = len(self.server.log)+1
            
    def _get_majority_minNum(self, l):
        """
        获取一个list中大多数item都大于的最小item
        """
        # 大多数的最少数量
        majority = math.ceil(float(len(l)+1)/2)
        return sorted(l, reverse=True)[majority-1: majority][0]

    