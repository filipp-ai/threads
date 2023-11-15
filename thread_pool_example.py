# SuperFastPython.com
# scan a range of port numbers on a host concurrently
from socket import AF_INET
from socket import SOCK_STREAM
from socket import socket
from multiprocessing.pool import ThreadPool
import time


# returns True if a connection can be made, False otherwise
def test_port_number(host, port):
    # create and configure the socket
    with socket(AF_INET, SOCK_STREAM) as sock:
        # set a timeout of a few seconds
        sock.settimeout(3)
        # connecting may fail
        try:
            # attempt to connect
            sock.connect((host, port))
            # a successful connection was made
            return True
        except:
            # ignore the failure
            return False


# scan port numbers on a host
def port_scan(host, ports):
    print(f'Scanning {host}...')
    # create the thread pool
    with ThreadPool(len(ports)) as pool:
        # prepare the arguments
        args = [(host, port) for port in ports]
        # dispatch all tasks
        results = pool.starmap(test_port_number, args)
        # report results in order
        for port, is_open in zip(ports, results):
            if is_open:
                print(f'> {host}:{port} open')


# protect the entry point
if __name__ == '__main__':
    # define host and port numbers to scan
    host = 'python.org'
    ports = range(1024)
    # test the ports
    t = time.time()
    port_scan(host, ports)
    print(f'{time.time()-t=}')
