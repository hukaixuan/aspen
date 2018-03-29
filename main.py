# -*- coding: utf-8 -*-
import sys
from server import Server

if __name__ == '__main__':
    cluster_addrs = ['127.0.0.1:9000', '127.0.0.1:9001', '127.0.0.1:9002', '127.0.0.1:9003', '127.0.0.1:9004']
    addr = sys.argv[1]
    server = Server(addr, cluster_addrs)
    server.serve()