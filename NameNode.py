import rpyc
import os
import sys
import time
import argparse
import json
import redis
import threading
import logging
from rpyc.utils.server import ThreadPoolServer, ThreadedServer
from rpyc import BgServingThread

# -----------------------------CONFIG------------------------------
REPLICATION_COUNT = 3

REDIS_ADDR = '47.113.123.159'
REDIS_PORT = 6379

RPYC_IP = '127.0.0.1'
RPYC_PORT = 50001

ERR_CODE = {
    'CAN_NOT_CONNECT': 101,
}

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
DATE_FORMAT = "%m/%d/%Y %H:%M:%S %p"

logging.basicConfig(filename='my.log', level=logging.DEBUG,
                    format=LOG_FORMAT, datefmt=DATE_FORMAT)
# -----------------------------CONFIG------------------------------

server = None


def stopServer():
    server.close()


class NameNode(rpyc.Service):
    def __init__(self):
        self.isRunning = True
        self.replicationCount = REPLICATION_COUNT
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
            logging.log(logging.DEBUG, 'Connection failed！')

    def updateNode(self):
        '''
        轮循更新节点
        '''
        while(self.isRunning):
            time.sleep(10)
            try:
                allNode = self.r.smembers(self.allNodeSetName)
            except Exception as e:
                print(e)
                continue
            currTime = time.time()
            for name in allNode:
                nodeTime = self.r.hget(self.allNodeHashTime, name)
                if nodeTime == None or currTime - float(nodeTime) > 20:
                    logging.log(logging.DEBUG, name + 'pop!')
                    self.copyToOtherNodes(name)
                    self.r.srem(self.allNodeSetName, name)
                    self.r.hdel(self.allNodeHashTime, name)

    def startUpdateNode(self):
        t1 = threading.Thread(target=self.updateNode)
        t1.start()

    def sortDataNode(self, nodeNameList):
        '''
        给传入的datanode排序
        nodeList:node 名
        返回按照最优排序的节点名
        '''
        # nodeList = [eval(self.r.hget(self.nodeHashName, nodeName))
        #             for nodeName in nodeNameList]
        if (len(nodeNameList) == 0):
            return []
        nodeNameList.sort(key=lambda x: self.isAlive(self.getNodeInfo(x)))
        return nodeNameList

    def getBestNode(self, count):
        '''
        获取最优节点，返回[ip,port]
        '''
        allNode = set(self.getAllNodeName())
        res = []
        try:
            for i in range(count):
                res.append(self.sortDataNode(list(allNode-set(res)))[0])
            return res
        except Exception as e:
            print(e)
            return res

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
        self.r.hset(self.allNodeHashTime, nodeName, time.time())

    def exposed_activateNode(self, nodeName):
        '''
        节点报送心跳信息
        '''
        self.r.sadd(self.allNodeSetName, nodeName)
        self.r.hset(self.allNodeHashTime, nodeName, time.time())

    def getAllNodeName(self):
        '''
         返回所有的有效结点
        '''
        return list(self.r.smembers(self.allNodeSetName))

    def getNodeInfo(self, nodeName):
        '''
        获取一个node的IP和port
        '''
        return eval(self.r.hget(self.nodeHashName, nodeName))

    def getNodesInfo(self, nodeNameList):
        '''
        获取一组nodeName的info
        '''
        return [self.getNodeInfo(node) for node in nodeNameList]

    def getChunkNodeName(self, chunkName):
        '''
        输入chunk列表，返回存储该chunk的所有nodeName
        '''
        return list(self.r.smembers(chunkName))

    def getChunkNode(self, chunkName):
        '''
        从chunkName得到节点信息[[ip,port],[xx,xx],,,,[xx,xx]]
        '''
        return [self.getNodeInfo(node) for node in self.getChunkNodeName(chunkName)]

    def getNodeBlocks(self, nodeName):
        '''
        获取一个节点存储的所有block
        '''
        return list(self.r.smembers(nodeName))

    def getBlockNodes(self, block):
        '''
        获取一个block的所有结点分布
        '''
        return list(self.r.smembers(block))

    def getRestNode(self, block):
        '''
        获取不含有block的结点列表，按照优先级排序
        '''
        nodeNameList = list(set(self.getAllNodeName()) -
                            set(self.getBlockNodes(block)))
        sortedNodeNameList = self.sortDataNode(nodeNameList)
        sortedNodeNameInfo = self.getNodesInfo(sortedNodeNameList)
        return sortedNodeNameInfo

    def copyToOtherNodes(self, nodeName):
        '''
        将一个结点的文件拷贝到其它结点
        最大程度拷贝，当结点不足时不拷贝
        '''
        blockList = self.getNodeBlocks(nodeName)
        for block in blockList:
            if(len(self.getBlockNodes(block)) < self.replicationCount):
                restNode = self.getRestNode(block)
                restNodeInfo = self.getNodesInfo(restNode)
                originNode = self.getNodeInfo(self.getBlockNodes(block)[0])
                conn = rpyc.connect(originNode[0], originNode[1])
                conn.root.relicate(restNodeInfo, block)

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
        sortedNodeList = []
        blockList = []
        if (self.r.exists(fileName)):
            blockList = self.r.zrevrange(fileName, 0, -1)
            for block in blockList:
                nodeList = list(self.r.smembers(block))
                sortedNodeList.append(self.sortDataNode(nodeList))
        return blockList, [[eval(self.r.hget(self.nodeHashName, node)) for node in blockNode] for blockNode in sortedNodeList]

    def exposed_saveFile(self, fileName, count):
        '''
        fileName：str
        count：int
        返回列表[[ip,port],[ip,port],[ip,port],...,[ip,port]],
        '''
        if (self.r.exists(fileName)):
            blockList = self.r.zrevrange(fileName, 0, -1)
            return blockList[count], self.getChunkNode(blockList[count])
        self.r.sadd('savedFile', fileName)
        blockName = fileName + '-block-' + str(count)
        nodeList = self.getBestNode(self.replicationCount)
        self.r.zadd(fileName, count, blockName)
        nodeInfoList = [eval(self.r.hget(self.nodeHashName, name))
                        for name in nodeList]
        return blockName, nodeInfoList

    def exposed_writeCheck(self, nodeName, blockName):
        '''
        存储每个file的block被存储到哪些节点
        每个节点存储了哪些block
        nodeName:uuid
        blockName:str
        '''
        self.r.sadd(blockName, nodeName)
        self.r.sadd(nodeName, blockName)

    def on_disconnect(self, conn):
        conn.close()


if __name__ == '__main__':

    server = ThreadedServer(NameNode, hostname=RPYC_IP, port=RPYC_PORT)
    try:
        server.start()
    except Exception:
        server.close()
