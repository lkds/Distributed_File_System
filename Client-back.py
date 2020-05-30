import rpyc
import os
import time
import argparse
import json
from rpyc.utils.server import ThreadedServer

BASE_DIR = str(os.getcwd()).replace('\\', ' / ')
BASE_CLIENT_DIR = BASE_DIR + '/Client/'

ERR_MSG = {

}


class Client(rpyc.Service):
    def __init__(self):
        self.bufferSize = 1024*1024*4  # 缓冲区大小
    # def blockToFile(self,)

    def connectToSever(self, serverIP, port):
        self.conn = rpyc.connect(serverIP, port)
