import pickle
import socket
import sys

HOST, PORT = '133.40.162.192', 18447
data = " ".join(sys.argv[1:])

def fetchHeader():
    try:
        sock = socket.create_connection(address=(HOST, PORT),
                                        timeout=1.0)
        sock.sendall('hdr\n')
    except Exception as e:
        logging.error("failed to send: %s" % (e))
        return []

    try:
        received = ""
        while True:
            try:
                oneBlock = sock.recv(1024)
                received = received + oneBlock
            except socket.error, e:
                if e.errno != errno.EINTR:
                    raise
            if oneBlock == '':
                break
        
    finally:
        sock.close()

    return pickle.loads(received)


        
