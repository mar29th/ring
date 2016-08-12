import argparse

import ring

ctx = ring.Context()
connection = ctx.connection(ring.REPLIER)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', help='server port', type=int, default=9000)
    args = parser.parse_args()

    connection.bind(('', args.port))
    print 'Port: %d' % (connection.getsockname()[1],)

    try:
        while 1:
            connection.send_pyobj(connection.recv_pyobj())
    finally:
        connection.close()
