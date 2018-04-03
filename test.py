# -*- coding: utf-8 -*-
import socket
import json
import sys

msg = {
    'type': 4,
    'command': 'goooooood'
}

ip = '127.0.0.1'
port = 8000
_udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
_udp_socket.bind((ip, port))
_udp_socket.settimeout(1)

data = json.dumps(msg).encode()

port = int(sys.argv[1])
print(port)
_udp_socket.sendto(data, ('127.0.0.1', port))