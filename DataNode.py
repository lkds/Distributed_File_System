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
import time
import logging

NAMENODE_HOST = '127.0.0.1'
NAMENODE_PORT = 50001

DATANODE_HOST = '127.0.0.1'
DATANODE_START_PORT = 50002



BASE_DIR = str(os.getcwd()).replace('\\', '/')
DATANODE_PATH = BASE_DIR+"/DataNode"

# 节点列表
# node1 node2 node3
DEVICE_ID_FILE = 'DEVICE_LIST'

CHUNK_SIZE = 4

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
DATE_FORMAT = "%m/%d/%Y %H:%M:%S %p"
logging.basicConfig(filename='datanode.log', level=logging.DEBUG,
                    format=LOG_FORMAT, datefmt=DATE_FORMAT)
# -------------------------------------------------

NodeStatus = dict()  # 名称--线程


port = DATANODE_START_PORT
class DataNode(Service):

    def __init__(self, nodeID, nodeIP, nodePort):
        self.nodeID = nodeID
        self.nodeIP = nodeIP
        self.nodePort = nodePort
        self.chunkSize = 1024 * 1024 * CHUNK_SIZE
        self.path = DATANODE_PATH+'/'+nodeID+'/'

        # self.startHeartBeat()

    # 获取自己的网络状况
    def getstatus(self):

        return random.randint(0, 100)+len(os.listdir(DATANODE_PATH))

    # def heartBeat(self):
    #     '''
    #     心跳
    #     '''
    #     while (True):
    #         time.sleep(10)
    #         connn = rpyc.connect(NAMENODE_HOST, NAMENODE_PORT)
    #         connn.root.activateNode(self.nodeID)
    #         connn.close()

    # def startHeartBeat(self):
    #     t1 = Thread(target=self.heartBeat)
    #     t1.start()
    # 还活着函数

    def exposed_stillAlive(self):
        return self.getstatus()

    # 读数据

    def exposed_read(self, chunkname):
        f = open(self.path+chunkname, 'rb')
        chunk = f.read()
        f.close()
        return chunk

    def exposed_delete(self, chunkname):
        try:
            os.remove(self.path + chunkname)
            conn = rpyc.connect(NAMENODE_HOST, NAMENODE_PORT)
            conn.root.deleteCheck(self.nodeID, chunkname)
            conn.close()
            return {'status': 1, 'description': 'Successfully delete {}'.format(chunkname)}
        except:
            logging.log(logging.DEBUG, 'try to remove file not exits!')
            return {'status': 1, 'description': 'Chunk {} not exits!'.format(chunkname)}

    # 写数据

    def write(self, chunk, chunkname):
        e = open(self.path+chunkname, 'wb+')
        e.write(chunk)
        e.close()
    # 将信息拷贝到下一个DataNode

    def exposed_copy(self, DataAList, chunk, chunkname):
        # 将文件复制到本地，并报告
        self.write(chunk, chunkname)
        connn = rpyc.connect(NAMENODE_HOST, NAMENODE_PORT)
        connn.root.writeCheck(self.nodeID, chunkname)
        connn.close()
        # 若有下一个节点
        if(len(DataAList) != 0):
            conn = rpyc.connect(DataAList[0][0], DataAList[0][1])
            conn.root.copy(DataAList[1:], chunk, chunkname)
            conn.close()

    def exposed_replicate(self, DataAList, chunkname):
        f = open(self.path+chunkname, 'rb')
        chunk = f.read()
        conn = rpyc.connect(DataAList[0][0], DataAList[0][1])
        conn.root.copy(DataAList, chunk, chunkname)
        conn.close()


def startANode(nodeID, nodeIp, nodeport):
    '''
    启动线程
    '''
    t = Thread(target=startNodeThread, args=(nodeID, nodeIp, nodeport))
    t.start()
    t2 = Thread(target=heatBeatThred, args=([nodeID, nodeIp, nodeport],))
    t2.start()
    logging.log(
        logging.INFO, '{}-{}-{} register!'.format(nodeID, nodeIp, nodeport))


def startNodeThread(nodeID, nodeIp, nodeport):
    '''
    启动主服务线程
    '''
    dataNode = classpartial(DataNode, nodeID, nodeIp, nodeport)
    t = ThreadedServer(dataNode, hostname=nodeIp, port=nodeport)
    NodeStatus[nodeID].append(t)
    t.start()

def addNodeThread(nodeID):
    global port
    ip = DATANODE_HOST
    # 创建数据节点储存空间
    nodePath = DATANODE_PATH+'/'+node
    if os.path.exists(nodePath):
        logging.log(logging.INFO, '{} already exits'.format(nodePath))
    else:
        os.makedirs(nodePath)
    # 启动并注册
    NodeStatus[node] = []
    NodeStatus[node].append(True)
    startANode(node, ip, port)

    port += 1


def stopNodeThread(nodeID):
    NodeStatus[nodeID][0] = False
    NodeStatus[nodeID][1].close()


def heatBeatThred(nodeinfo):
    '''
    发送心跳包
    '''
    try:
        conn = rpyc.connect(NAMENODE_HOST, NAMENODE_PORT)
        while(True):
            if NodeStatus[nodeinfo[0]][0] == True:
                conn.root.setNode(nodeinfo)
                time.sleep(10)
            else:
                break
    except:
        conn.close()
        logging.log(logging.DEBUG, 'namenode is dead！')


def registerNode():
    """
    注册节点，向namenode发送所有ID，IP PORT
    """
    global port
    # 1.读取名字
    namelist = []
    with open(DEVICE_ID_FILE, 'r') as f:
        for line in f.readlines():
            linestr = line.strip()
            namelist.append(linestr)
    print(namelist)
    # 读完名字
    # 2.向datanode注册并创建线程
    ip = DATANODE_HOST
    for node in namelist:
        # 创建数据节点储存空间
        nodePath = DATANODE_PATH+'/'+node
        if os.path.exists(nodePath):
            logging.log(logging.INFO, '{} already exits'.format(nodePath))
        else:
            os.makedirs(nodePath)
        # 启动并注册
        NodeStatus[node] = []
        NodeStatus[node].append(True)
        startANode(node, ip, port)

        port += 1


if __name__ == '__main__':
    print('DataNode starting.....')
    logging.log(logging.INFO, 'DataNode starting......')
    registerNode()
    while(True):
        print("添加和删除节点：1.添加节点   2.删除节点")
        n = input()
        if(n=='1'):
            print("输入节点名：格式如（node1）")
            node = input()
            addNodeThread(node)
        elif(n=='2'):
            print("输入节点名：格式如（node1）")
            node = input()
            stopNodeThread(node)
        else:
            print("输入有误")
