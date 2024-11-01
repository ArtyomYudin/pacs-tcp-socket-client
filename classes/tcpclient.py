import socket
import ssl
from time import sleep

class TcpClient:

    def __init__(self, host, port, server_key, server_cert, logger):
        self.host = host
        self.port = port
        self.server_key = server_key
        self.server_cert = server_cert
        self.logger = logger
        self.connected = False

    def connect(self):
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.options &= ~ssl.OP_NO_TLSv1_3 & ~ssl.OP_NO_TLSv1_2 & ~ssl.OP_NO_TLSv1_1
        context.verify_mode = ssl.CERT_REQUIRED
        context.minimum_version = ssl.TLSVersion.TLSv1
        #context.set_ciphers('AES256-SHA')
        context.set_ciphers('DEFAULT@SECLEVEL=0')
        context.load_verify_locations(self.server_cert)
        context.load_cert_chain(certfile=self.server_cert, keyfile=self.server_key)

        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client = context.wrap_socket(client, server_hostname='SKD')
        client.settimeout(3)

        try:
            client.connect((self.host, self.port))
            client.settimeout(None)
            self.connected = True
            self.logger.info(f'Connection to server {self.host} on port {self.port}')
        except socket.gaierror as e:
            client.close()
            return self.logger.warning('Address-related error connecting to server: %s' % e)
        except ConnectionRefusedError:
            client.close()
            self.connected = False
            while not self.connected:
                try:
                    client.connect((self.host, self.port))
                    self.logger.info(f're-Connection to server {self.host} on port {self.port}')
                    connected = True
                except socket.error:
                    sleep(2)
        except OSError as e:
            client.close()
            self.connected = False
            while not self.connected:
                try:
                    client.connect((self.host, self.port))
                    self.logger.info(f're-Connection to server {self.host} on port {self.port}')
                    self.connected = True
                except socket.error:
                    sleep(2)
                    self.logger.info(f'!!!! {e}')

        #if client is None:
        #    self.logger.info('could not open socket')
        #    sys.exit(1)
        return client