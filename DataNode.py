from rpyc import Service
import rpyc
from rpyc.utils.server import ThreadedServer
from rpyc.utils.helpers import classpartial
import os
import random
import configparser
import uuid

NAMENODE_HOST = '127.0.0.1'
NAMENODE_PORT = 50001

DATANODE_HOST = '127.0.0.1'
DATANODE_PORT = 50002

BASE_DIR = str(os.getcwd()).replace('\\', '/')
DATA_DATAPATH = BASE_DIR+"/DataNode"

# 节点列表
# node1 node2 node3
DEVICE_ID_FILE = 'DEVICE_LIST'


def registerNode():
    """
    注册节点，向namenode发送所有ID，IP PORT
    """
    # 1.读取名字
    # 2.向datanode注册并创建线程
    nodeinfo = [DEVICE_ID, DATANODE_HOST, DATANODE_PORT]
    conn = rpyc.connect(NAMENODE_HOST, NAMENODE_PORT)
    conn.root.setNode(nodeinfo)
    conn.close()


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
        f = open(DATA_DATAPATH+'/'+chunkname, 'rb')
        chunk = f.read()
        f.close()
        return chunk

    # 写数据
    def write(self, chunk, chunkname):
        e = open(DATA_DATAPATH+'/'+chunkname, 'wb+')
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


def startANode(nodeID):
    dataNode = classpartial(DataNode, nodeID)
    t = ThreadedServer(dataNode, hostname=DATANODE_HOST, port=DATANODE_PORT)
    t.start()


if __name__ == '__main__':
    print('DataNode starting.....')
