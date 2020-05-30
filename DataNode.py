from rpyc import Service
import rpyc
from rpyc.utils.server import ThreadedServer
import os
import random

import uuid

NameNode_Host = '192.168.43.52'
NameNode_Port = 50000

DataNode_datapath = os.getcwd()+"/DataNode"
UUID_path =  os.getcwd()+"/uuid"


class DataNode(Service):


    def __init__(self):
        node = uuid.getnode()
        self.id = uuid.UUID(int = node).hex[-12:]
        self.ip = '192.168.43.156'
        self.port = 50000
        #初次上线，向NameNode注册
        nodeinfo = []
        nodeinfo.append(self.id) #根据mac地址生成uuid
        nodeinfo.append(self.ip) 
        nodeinfo.append(self.port) 
        conn=rpyc.connect(NameNode_Host,NameNode_Port)
        conn.root.setNode(nodeinfo)
        conn.close()

    #获取id
    def exposed_getid(self):
        return self.id

    #获取自己的网络状况
    def getstatus(self):
        path =DataNode_datapath
        count = 0
        for file in os.listdir(path): #file 表示的是文件名
                count = count+1
        
        return random.randint(0,100)+count

    #还活着函数
    def exposed_stillAlive(self):
        return self.getstatus()


    #读数据
    def exposed_read(self, chunkname):
        f = open(DataNode_datapath+'/'+chunkname,'rb')
        chunk = f.read(1024*1024*4)
        f.close()
        return chunk

    #写数据
    def write(self,chunk, chunkname):
        e = open(DataNode_datapath+'/'+chunkname, 'wb+')
        e.write(chunk)
        e.flush()
        e.close()


    #将信息拷贝到下一个DataNode
    def exposed_copy(self,DataAList,chunk, chunkname):
        #将文件复制到本地，并报告
        self.write(chunk, chunkname)
        connn = rpyc.connect(NameNode_Host,NameNode_Host)
        connn.root.writeCheck(conn.root.getid,chunkname)
        connn.close
        #若有下一个节点
        del DataAList[0]
        if(len(DataAList)!=0):        
            conn=rpyc.connect(DataAList[0][0],DataAList[0][1])
            conn.root.copy(DataAList,count,chunk, chunkname)
            conn.close()

        

if __name__ == '__main__':
    print('DataNode starting.....')
    t = ThreadedServer(DataNode, hostname = '192.168.43.156',port=50000)
    t.start()
    
    pass