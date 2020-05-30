from rpyc import Service
from rpyc.utils.server import ThreadedServer

NameNode_Host = '127.0.0.1'
NameNode_Port = 50000
blocksize = 1024*1024*4

Client_datapath = os.getcwd()+"/ClientSpace"


class client(Service):
    def __init__(self):
        pass
    #初次写文件建立文件副本
    def clientSaveFile(self,filename):  
        inputfile = open(Client_datapath+'/'+chunkname,'rb')#open the fromfile
        count = 0
        while True:
            conn=rpyc.connect(NameNode_Host,NameNode_Port)
            chunk = inputfile.read(blocksize)
            if not chunk:             #check the chunk is empty
                break
            count += 1
            chunkname,DataNodeAlist=conn.root.savefile(filename,count)
            conn.close()
            ######################################################
            conn=rpyc.connect(DataAList[0][0],DataAList[0][1])
            conn.root.copy(DataAList,count,chunk, chunkname)
            conn.close()
            #####################################################
    #读取文件
    def clientReadFile(self,filename):
        conn=rpyc.connect(NameNode_Host,NameNode_Port)
        blocks,DataNodes=conn.root.getFileInfo(filename)
        conn.close()
        f= open(Client_datapath+'/'+filename, 'wb+')
        for i in range(0,len(blocks)):
            conn=rpyc.connect(DataNodes[i][0][0],(DataNodes[i][0][1])
            f.write(conn.root.read(blocks[i]))
            conn.close()
        f.close()

