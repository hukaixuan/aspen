## ASPEN
基于raft算法的分布式kv数据库

### Improve
- improve project structure  √
- write test 
- use logging instead of print
- chose a license √
- imporve readme
- optimize Code，improve doc string

### TODO:
- 日志压缩
- log 持久化
- 节点的动态增减
- client  (待改进)
- 存储部分选成熟的单机kv数据库
- 监控：最好能有一个动态显示节点状态及数据流向的前端页面
- test
- 开一个新线程commit log entry 到状态机  √
- 读操作不走log