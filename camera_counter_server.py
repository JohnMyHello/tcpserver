from camera_counter_process_data import ProcessData
from time import strftime
import SocketServer


class MyServer(SocketServer.BaseRequestHandler):
    def handle(self):
        print('%s got connection from: %s' % (strftime('%Y-%m-%d %H:%M:%S'), self.client_address))
        process_data = ProcessData()
        while True:
            try:
                conn = self.request
                data = conn.recv(1024)
                self.request.settimeout(100)
                if not data:
                    break
                else:
                    # print [data]
                    #
                    process_data.distributing_data(data)

            except Exception as err:
                debug_msg = 'Error: %s' % err
                process_data.is_online(is_online=0)
                process_data.debug_msg(debug_msg)
                break


if __name__ == '__main__':
    # host = '192.168.31.199'
    host = '120.25.204.96'
    port = 21567
    address = (host, port)
    server = SocketServer.ThreadingTCPServer(address, MyServer)
    server.serve_forever()
