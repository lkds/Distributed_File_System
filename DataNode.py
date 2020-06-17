from rpyc import Service
import rpyc
from rpyc.utils.server import ThreadedServer
from rpyc import BgServingThread
from rpyc.utils.helpers import classpartial
import os
import random
import configparser
import uuid
import pandas as pd
from threading import Thread

NAMENODE_HOST = '127.0.0.1'
NAMENODE_PORT = 50001



BASE_DIR = str(os.getcwd()).replace('\\', '/')
DATA_DATAPATH = BASE_DIR+"/DataNode"

# 节点列表
# node1 node2 node3
DEVICE_ID_FILE = 'DEVICE_LIST'

class DataNode(Service):

    def __init__(self, nodeID):
        self.nodeID = nodeID
        self.chunkSize = 1024*1024*4
        # 初次上线，向NameNode注册
        if not os.path.exists(DATA_DATAPATH):
            os.makedirs(DATA_DATAPATH)

    # 获取自己的网络状况

    def getstatus(self):

        return random.randint(0, 100)+len(os.listdir(DATA_DATAPATH))

    # 还活着函数
    def exposed_stillAlive(self):
        return self.getstatus()

    # 读数据

    def exposed_read(self, chunkname):
        f = open(DATA_DATAPATH+'/'+self.nodeID+chunkname, 'rb')
        chunk = f.read()
        f.close()
        return chunk

    # 写数据
    def write(self, chunk, chunkname):
        e = open(DATA_DATAPATH+'/'+self.nodeID+chunkname, 'wb+')
        e.write(chunk)
        e.close()
    # 将信息拷贝到下一个DataNode

    def exposed_copy(self, DataAList, chunk, chunkname):
        # 将文件复制到本地，并报告
        self.write(chunk, chunkname)
        connn = rpyc.connect(NAMENODE_HOST, NAMENODE_PORT)
        connn.root.writeCheck(DEVICE_ID, chunkname)
        connn.close()
        # 若有下一个节点
        if(len(DataAList) != 0):
            conn = rpyc.connect(DataAList[0][0], DataAList[0][1])
            conn.root.copy(DataAList[1:], chunk, chunkname)
            conn.close()

def startANode(nodeID,nodeIp,nodeport): 
    t = Thread(target=startNodeThread,args=(nodeID,nodeIp,nodeport))
    t.start()


def startNodeThread(nodeID,nodeIp,nodeport):
    dataNode = classpartial(DataNode, nodeID)
    t = ThreadedServer(dataNode, hostname=nodeIp, port=nodeport)
    t.start()


def registerNode():
    """
    注册节点，向namenode发送所有ID，IP PORT
    """
    # 1.读取名字
    namelist = []
    with open(DEVICE_ID_FILE,'r') as f:
        for line in f.readlines():
            linestr = line.strip()
            namelist.append(linestr)
    print(namelist)
    #读完名字
    # 2.向datanode注册并创建线程
    i = 50002
    for DEVICE_ID in namelist:
        DATANODE_HOST = '127.0.0.1'
        DATANODE_PORT = i
        nodeinfo = [DEVICE_ID, DATANODE_HOST, DATANODE_PORT]
        i = i+1

        #创建数据节点储存空间
        if os.path.exists(DATA_DATAPATH+'./'+DEVICE_ID):
            print('已存在')
        else:
            os.mkdir(DATA_DATAPATH+'./'+DEVICE_ID)
        #启动并注册
        startANode(DEVICE_ID,DATANODE_HOST, DATANODE_PORT)
        conn = rpyc.connect(NAMENODE_HOST, NAMENODE_PORT)
        conn.root.setNode(nodeinfo)
        conn.close()

if __name__ == '__main__':
    print('DataNode starting.....')
    registerNode()