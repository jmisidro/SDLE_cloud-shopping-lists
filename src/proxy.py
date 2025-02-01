import zmq

class Proxy:
    def __init__(self, xsub_addr="tcp://*:5555", xpub_addr="tcp://*:5556"):
        self.context = zmq.Context()
        self.xpub_socket = self.context.socket(zmq.XPUB)
        self.xpub_socket.bind(xpub_addr)
        self.xsub_socket = self.context.socket(zmq.XSUB)
        self.xsub_socket.bind(xsub_addr)

    def start(self):
        zmq.proxy(self.xsub_socket, self.xpub_socket)

if __name__ == "__main__":
    proxy = Proxy()
    proxy.start()