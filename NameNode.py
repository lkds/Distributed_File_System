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


class NameNode(rpyc.Service):
    def __init__(self):
        self.isRunning = True
        self.replicationCount = REPLICATION_COUNT
        # redis的hash name
        self.nodeHashName = 'node'
        self.allNodeSetName = 'DNode'
        self.allNodeHashTime = 'DNodeTime'
        self.savedSetFile = 'savedFile'
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
            return 101

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
                    self.r.srem(self.allNodeSetName, name)
                    self.r.hdel(self.allNodeHashTime, name)
                    self.copyToOtherNodes(name)

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

    def getAllFileName(self):
        '''
        获取所有存储的文件
        '''
        return list(self.r.smembers(self.savedSetFile))

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

    def getFileChunks(self, fileName):
        '''
        获取一个文件的所有chunk
        '''
        return list(self.r.zrange(fileName, 0, -1))

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

    def getBlockLiveNodes(self, block):
        '''
        获取一个block的有效结点个数
        '''
        allNodes = self.getBlockNodes(block)
        return list(set(self.getAllNodeName()) & set(allNodes))

    def getRestNode(self, block):
        '''
        获取不含有block的活的结点列表，按照优先级排序
        '''
        nodeNameList = list(set(self.getAllNodeName()) -
                            set(self.getBlockNodes(block)))
        sortedNodeNameList = self.sortDataNode(nodeNameList)
        # sortedLiveNodeNameList = list(
        #     set(sortedNodeNameList) & set(self.getAllNodeName()))
        sortedNodeNameInfo = self.getNodesInfo(sortedNodeNameList)
        return sortedNodeNameInfo

    def getBestLiveNodes(self, blockName):
        '''
        返回一组blockName最优的活的存储结点
        '''
        allBlockNode = self.getBlockNodes(blockName)
        allLiveBlockNode = list(set(allBlockNode) & set(self.getAllNodeName()))
        sortedAllLiveBlockNode = self.sortDataNode(allLiveBlockNode)
        return sortedAllLiveBlockNode

    def copyToOtherNodes(self, nodeName):
        '''
        将一个结点的文件拷贝到其它结点
        最大程度拷贝，当结点不足时不拷贝
        '''
        blockList = self.getNodeBlocks(nodeName)
        for block in blockList:
            if(len(list(set(self.getAllNodeName()) & set(self.getBlockNodes(block)))) < self.replicationCount):
                restNode = self.getRestNode(block)
                originNodes = self.getBestLiveNodes(block)
                if (len(originNodes) == 0):
                    logging.log(
                        logging.DEBUG, 'Living Node not enough, stop replicate for {}'.format(block))
                    break
                originNode = originNodes[0]
                originNodeInfo = self.getNodeInfo(originNode)
                conn = rpyc.connect(originNodeInfo[0], originNodeInfo[1])
                conn.root.replicate(restNode, block)
                conn.close()

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
        if (self.r.sismember(self.savedSetFile, fileName)):
            blockList = self.r.zrange(fileName, 0, -1)
            for block in blockList:
                nodeList = self.getBlockLiveNodes(block)
                # nodeList = list(self.r.smembers(block))
                sortedNodeList.append(self.sortDataNode(nodeList))
            return blockList, [[eval(self.r.hget(self.nodeHashName, node)) for node in blockNode] for blockNode in sortedNodeList]
        return [], []

    def exposed_saveFile(self, fileName, count):
        '''
        fileName：str
        count：int
        返回列表[[ip,port],[ip,port],[ip,port],...,[ip,port]],
        '''
        blockName = fileName + '-block-' + str(count)
        # if(self.r.sismember(self.savedSetFile, fileName))
        if (self.r.exists(blockName)):
            # blockList = self.r.zrevrange(fileName, 0, -1)
            # return blockList[count], self.getChunkNode(blockList[count])
            return blockName, self.getChunkNode(blockName)

        nodeList = self.getBestNode(self.replicationCount)
        self.r.zadd(fileName, count, blockName)
        nodeInfoList = [eval(self.r.hget(self.nodeHashName, name))
                        for name in nodeList]
        self.r.sadd(self.savedSetFile, fileName)
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

    def exposed_deleteCheck(self, nodename, blockName):
        '''
        删除完成后确认删除node存储的block和block
        '''
        self.r.srem(nodename, blockName)
        self.r.delete(blockName)
        fileName = blockName.split('-block-')[0]
        self.r.delete(fileName)
        self.r.srem(self.savedSetFile, fileName)

    def exposed_listFile(self, node='all'):
        '''
        列出集群/结点上的所有文件 块数 总副本数量 有效副本数量
        '''
        if (node == 'all'):
            allFile = self.getAllFileName()
            allFileChunk = [self.getFileChunks(file) for file in allFile]
            allFileChunkCount = [len(chunk) for chunk in allFileChunk]
            allFileNodeCount = [len(self.getBlockNodes(
                fileChunk[0])) for fileChunk in allFileChunk]
            allFileLiveNodeCount = [len(self.getBlockLiveNodes(
                fileChunk[0])) for fileChunk in allFileChunk]
            return [allFile, allFileChunkCount, allFileNodeCount, allFileLiveNodeCount]
        elif (node == 'node'):
            allNodeName = self.getAllNodeName()
            return [allNodeName, [self.getNodeInfo(name) for name in allNodeName]]

    def on_disconnect(self, conn):
        conn.close()


if __name__ == '__main__':

    server = ThreadedServer(NameNode, hostname=RPYC_IP, port=RPYC_PORT)
    try:
        server.start()
    except Exception:
        server.close()
