import rpyc
import os
import time
import argparse
import json
import redis
import threading
import logging
from rpyc.utils.server import ThreadedServer

REDIS_ADDR = '47.113.123.159'
REDIS_PORT = 6379

RPYC_IP = '192.168.43.52'
RPYC_PORT = 50000

ERR_CODE = {
    'CAN_NOT_CONNECT': 101,
}

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
DATE_FORMAT = "%m/%d/%Y %H:%M:%S %p"

logging.basicConfig(filename='my.log', level=logging.DEBUG,
                    format=LOG_FORMAT, datefmt=DATE_FORMAT)


class NameNode(rpyc.Service):
    def __init__(self, count=3):
        self.isRunning = True
        self.replicationCount = count
        # redis的hash name
        self.nodeHashName = 'node'
        self.allNodeSetName = 'DNode'
        self.allNodeHashTime = 'DNodeTime'
        # 初始化本地缓存库
        self.r = redis.StrictRedis(
            host=REDIS_ADDR, port=REDIS_PORT, db=0, decode_responses=True)

        self.startUpdateNode()

    def shortConnect(self, IP, port):
        return rpyc.connect(IP, port)

    def shortDisconnect(self, conn):
        conn.close()

    def isAlive(self, nodeInfo):
        '''
        判断一个node是否活着，返回延迟，未响应返回101
        '''
        try:
            conn = self.shortConnect(nodeInfo[0], nodeInfo[1])
            res = conn.root.stillAlive()
            self.shortDisconnect(conn)
            return res
        except:
            return ERR_CODE['CAN_NOT_CONNECT']

    def updateNode(self):
        '''
        轮循更新节点
        '''
        while(self.isRunning):
            time.sleep(10)
            allNode = self.r.smembers(self.allNodeSetName)
            currTime = time.time()
            for name in allNode:
                nodeTime = self.r.hget(self.allNodeHashTime, name)
                if nodeTime - currTime > 10:
                    logging.log(logging.DEBUG, name +
                                ' last avtive time '+nodeTime+', pop!')
                    self.r.srem(self.allNodeSetName, name)
                    self.r.hdel(self.allNodeHashTime, name)

    def startUpdateNode(self):
        t1 = threading.Thread(target=self.updateNode)
        t1.start()
        t1.join()

    def sortDataNode(self, nodeNameList):
        '''
        给传入的datanode排序
        nodeList:node 名 uuid
        '''
        nodeList = [eval(self.r.hget(self.nodeHashName, nodeName))
                    for nodeName in nodeNameList]
        nodeList.sort(key=lambda x: self.isAlive(x))
        return nodeList

    def getBestNode(self, count):
        '''
        获取最优节点，返回[ip,port]
        '''
        allNode = self.r.smembers(self.allNodeSetName)
        res = set()
        for i in range(count):
            res.add(self.sortDataNode(allNode-res)[0])
        return list(res)

    def exposed_setNode(self, nodeInfo):
        '''
        注册节点，传入节点名，节点IP和端口
        list,list[0]-nodename list[1]-ip list[2]-port
        '''
        nodeName = nodeInfo[0]
        nodeIP = nodeInfo[1]
        nodePort = nodeInfo[2]
        self.r.sadd(self.allNodeSetName, nodeName)
        self.r.hset(self.nodeHashName, nodeName, [nodeIP, nodePort])

    def exposed_activateNode(self, nodeName):
        '''
        节点报送心跳信息
        '''
        self.r.sadd(self.allNodeSetName, nodeName)
        self.r.hset(self.allNodeHashTime, nodeName, time.time())

    def exposed_getFileInfo(self, fileName):
        '''
        获取文件的块和分布，并且按照延时排序
        [block1,block2,block3...blockm],
        [
            [[ip,port],[ip,port],[ip,port],...,[ip,port]],
            [[ip,port],[ip,port],[ip,port],...,[ip,port]],
            ...
        ]
        '''
        res = []
        blockList = []
        if (self.r.exists(fileName)):
            blockList = self.r.zrevrange(fileName, 0, -1)
            for block in blockList:
                nodeList = list(self.r.smembers(block))
                res.append(self.sortDataNode(nodeList))
        return blockList, res

    def exposed_saveFile(self, fileName, count):
        '''
        fileName：str
        count：int
        返回列表[[ip,port],[ip,port],[ip,port],...,[ip,port]],
        '''
        blockName = fileName+'-block-'+str(count)
        nodeList = self.getBestNode(self.replicationCount)
        self.r.zadd(fileName, count, blockName)
        return blockName, nodeList

    def exposed_writeCheck(self, nodeName, blockName):
        '''
        nodeName:uuid
        blockName:str
        '''
        self.r.sadd(blockName, nodeName)


if __name__ == '__main__':

    server = ThreadedServer(NameNode, hostname=RPYC_IP, port=RPYC_PORT)
    try:
        server.start()
    except KeyboardInterrupt:
        server.close()