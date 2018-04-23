# -*- coding: utf-8 -*-
import shelve
from .command import CommandType

class StateMachine(object):
    def __init__(self, storage='db'):
        self.storage = storage

    def execute(self, command):
        cmd_type = command['type'] 
        argv = command['argv']
        if cmd_type == CommandType.GET:
            key = argv[0]
            res = self._get(key)
        elif cmd_type == CommandType.SET:
            key = argv[0]
            value = argv[1]
            res = self._set(key, value)
        elif cmd_type == CommandType.ITEMS:
            res = self._items()
        elif cmd_type == CommandType.DELETE:
            key = argv[0]
            res = self._delete(key)
        return res

    def _get(self, key):
        with shelve.open(self.storage) as db:
            res = db.get(key)
        return res

    def _set(self, key, value):
        with shelve.open(self.storage) as db:
            db[key] = value
        return True

    def _items(self):
        with shelve.open(self.storage) as db:
            res = list(db.items())
        return res

    def _delete(self, key):
        with shelve.open(self.storage) as db:
            del db[key]
        return True