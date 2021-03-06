import rpyc
from rpyc import Service
from rpyc.utils.server import ThreadedServer
import os
import argparse

# NAMENODE_HOST = '127.0.0.1'
NAMENODE_HOST = '192.168.43.52'
NAMENODE_PORT = 50001

CHUNK_SIZE = 4

BASE_DIR = str(os.getcwd()).replace('\\', '/')
CLIENT_DATAPATH = BASE_DIR + '/ClientSpace/'


class Client(Service):
    def __init__(self):
        self.blocksize = 1024*1024*CHUNK_SIZE
        if not os.path.exists(CLIENT_DATAPATH):
            os.makedirs(CLIENT_DATAPATH)
        if not os.path.exists(CLIENT_DATAPATH+'/get'):
            os.makedirs(CLIENT_DATAPATH+'/get')

    def setBlockSize(self, size):
        '''
        设置单个文件块大小
        '''
        self.blockSize = size*1024*1024

    def printInfo(self, name, data):
        '''
        打印信息
        '''
        for item in name:
            print('{}\t'.format(item), end='')
        print('')
        for item in data:
            for it in item:
                print('{}\t'.format(it), end='')
            print('')

    def put(self, filename):
        '''
        保存文件
        '''
        try:
            inputfile = open(CLIENT_DATAPATH+filename,
                             'rb')  # open the fromfile
        except Exception as e:
            print(e)
            return
        count = 0
        while True:
            conn = rpyc.connect(NAMENODE_HOST, NAMENODE_PORT)
            chunk = inputfile.read(self.blocksize)
            if not chunk:  # check the chunk is empty
                inputfile.close()
                break
            chunkname, DataNodeAlist = conn.root.saveFile(filename, count)
            if (len(DataNodeAlist) == 0):
                print('结点故障，请检查！')
                return
            count += 1
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
        if (len(blocks) == 0):
            print('文件不存在！')
        f = open(CLIENT_DATAPATH+'/get/'+filename, 'wb+')
        for i in range(0, len(blocks)):
            currNode = 0
            while (True):
                try:
                    conn2 = rpyc.connect(
                        DataNodes[i][currNode][0], DataNodes[i][currNode][1])
                    break
                except:
                    currNode += 1
                    if (currNode >= len(blocks)):
                        print('节点连接失败！')
                        conn2.close()
                        f.close()
                        conn.close()
                        return
                    print('节点{}连接失败，重试...'.format(currNode))
                    continue
            f.write(conn2.root.read(blocks[i]))
            conn2.close()
        f.close()
        conn.close()

    def delete(self, filename):
        '''
        删除文件
        '''
        conn = rpyc.connect(NAMENODE_HOST, NAMENODE_PORT)
        blocks, DataNodes = conn.root.getFileInfo(filename)
        if (len(blocks) == 0):
            print('文件不存在！')
            return
        status = True
        for i in range(0, len(blocks)):
            for node in DataNodes[i]:
                conn2 = rpyc.connect(node[0], node[1])
                res = conn2.root.delete(blocks[i])
                if (res['status'] == 0):
                    status = False
                conn2.close()
        conn.close()
        if (not status):
            print('删除失败！详情查看DataNode日志')
        else:
            print('删除成功！')

    def listFiles(self, node):
        '''
        显示文件
        '''
        con = rpyc.connect(NAMENODE_HOST, NAMENODE_PORT)
        fileInfo = con.root.listFile(node)
        if(node == 'all'):
            self.printInfo(['file', 'chunk count', 'node count',
                            'active node count'], list(zip(*fileInfo)))
        elif (node == 'node'):

            self.printInfo(['nodeID', 'IP\t', 'port'],
                           [*zip(*[fileInfo[0], *zip(*fileInfo[1])])])
        # print(fileInfo)

    def reset(self):
        con = rpyc.connect(NAMENODE_HOST, NAMENODE_PORT)
        con.root.reset()
        con.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--put', type=str, default=None,
                        help='put the file int cluster')
    parser.add_argument('--ls', type=str, default=None,
                        help='list file in the cluster')
    parser.add_argument('--get', type=str, default=None,
                        help='get file from cluster')
    parser.add_argument('--dele', type=str, default=None,
                        help='delete file in the cluster')
    parser.add_argument('--reset', type=str, default=None,
                        help='reset the cluster')
    args = parser.parse_args()
    client = Client()
    if (args.put):
        client.put(args.put)
    elif (args.get):
        client.get(args.get)
    elif (args.dele):
        client.delete(args.dele)
    elif (args.ls):
        client.listFiles(args.ls)
    elif (args.reset):
        client.reset()

# client = Client()
# client.put('v.zip')
