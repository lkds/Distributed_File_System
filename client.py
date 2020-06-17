import rpyc
from rpyc import Service
from rpyc.utils.server import ThreadedServer
import os
import argparse

NAMENODE_HOST = '127.0.0.1'
NAMENODE_PORT = 50001

BASE_DIR = str(os.getcwd()).replace('\\', '/')
CLIENT_DATAPATH = BASE_DIR + '/ClientSpace/'


class Client(Service):
    def __init__(self):
        self.blocksize = 1024*1024*4
        if not os.path.exists(CLIENT_DATAPATH):
            os.makedirs(CLIENT_DATAPATH)

    def setBlockSize(self, size):
        '''
        设置单个文件块大小
        '''
        self.blockSize = size*1024*1024

    def put(self, filename):
        '''
        保存文件
        '''
        try:
            inputfile = open(CLIENT_DATAPATH+'/'+filename,
                             'rb')  # open the fromfile
        except Exception as e:
            print(e)
            return
        count = 0
        while True:
            conn = rpyc.connect(NAMENODE_HOST, NAMENODE_PORT)
            chunk = inputfile.read(self.blocksize)
            if not chunk:  # check the chunk is empty
                break
            count += 1
            chunkname, DataNodeAlist = conn.root.saveFile(filename, count)

            ##########################向DataNode写入########################
            con = rpyc.connect(DataNodeAlist[0][0], DataNodeAlist[0][1])
            con.root.copy(DataNodeAlist, chunk, chunkname)
            con.close()
            conn.close()

    def get(self, filename):
        '''
        获取文件
        '''
        conn = rpyc.connect(NAMENODE_HOST, NAMENODE_PORT)
        blocks, DataNodes = conn.root.getFileInfo(filename)
        conn.close()
        f = open(CLIENT_DATAPATH+'/'+filename, 'wb+')
        for i in range(0, len(blocks)):
            conn = rpyc.connect(DataNodes[i][0][0], (DataNodes[i][0][1]))
            f.write(conn.root.read(blocks[i]))
            conn.close()
        f.close()

    def delete(self, filename):
        pass


# if __name__ == "__main__":
#     parser = argparse.ArgumentParser()
#     parser.add_argument('--put', type=str, default=None,
#                         help='put')
#     parser.add_argument('--get', type=str, default=None,
#                         help='get')
#     parser.add_argument('--delete', type=str, default=None,
#                         help='get')
#     args = parser.parse_args()
#     client = Client()
#     if (args.put):
#         client.put(args.put)
#     elif (args.get):
#         client.get(args.put)
#     elif (args.delete):
#         client.delete(args.delete)

client = Client()
client.put('a')
