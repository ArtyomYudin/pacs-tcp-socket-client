import json
import logging
import socket
import ssl
import struct
import sys

from classes.db import DB


class TcpClient:
    __filter_events_command = json.dumps({
        'Command': 'filterevents',
        'Id': 1,
        'Version': 1,
        'Filter': 1,
    })

    __ping_command = json.dumps({
        'Command': 'ping',
        'Id': 1,
        'Version': 1,
    })

    def __init__(self, host, port, server_key, server_cert, is_verbose=False):
        self.host = host
        self.port = port
        self.server_key = server_key
        self.server_cert = server_cert
        if is_verbose:
            self.log_level=logging.DEBUG
        else:
            self.log_level=logging.INFO

        log_format = logging.Formatter('[%(asctime)s] [%(levelname)s] - %(message)s')
        self.logger = logging.getLogger('pacs_tcp_client')
        self.logger.setLevel(self.log_level)

        # writing to stdout
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(self.log_level)
        handler.setFormatter(log_format)
        self.logger.addHandler(handler)

        self.db = DB(user='itsupport', password='gRzXJHxq7qLM', database='itsupport', host='postgresql')


    def __create_buffer(self, post_json_data):
        buffer = post_json_data.encode('utf-8')
        buffer_with_byte = bytearray(4 + len(buffer))
        struct.pack_into('<I', buffer_with_byte, 0, len(buffer))
        buffer_with_byte[4:] = buffer
        return bytes(buffer_with_byte)

    async def connect(self):
        connected = False
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
            connected = True
            self.logger.info(f'Connection to server {self.host} on port {self.port}')
        except socket.gaierror as e:
            client.close()
            return self.logger.warning('Address-related error connecting to server: %s' % e)
        except ConnectionRefusedError:
            client.close()
            connected = False
            while not connected:
                try:
                    client.connect((self.host, self.port))
                    self.logger.info(f're-Connection to server {self.host} on port {self.port}')
                    connected = True
                except socket.error:
                    sleep(2)
        except OSError as e:
            client.close()
            connected = False
            while not connected:
                try:
                    client.connect((self.host, self.port))
                    self.logger.info(f're-Connection to server {self.host} on port {self.port}')
                    connected = True
                except socket.error:
                    sleep(2)
                    self.logger.info(f'!!!! {e}')

        #if client is None:
        #    self.logger.info('could not open socket')
        #    sys.exit(1)

        with client:
            client.sendall(self.__create_buffer(self.__filter_events_command))
            #print(create_buffer(filter_events_command))
            while True:
                data = client.recv(1024)
                if data:
                    received = json.loads(data[4:].decode('utf-8'))
                    match received['Command']:
                        case 'ping':
                            client.sendall(self.__create_buffer(self.__ping_command))
                            self.logger.debug(f'RECEIVED: {received}')
                        case 'events':
                            await self.db.execute('''INSERT INTO public.pacs_event(created, ap_id_id, owner_id_id, card, code) VALUES('31.10.2024 17:27:14', 1, 1, 1, 1)''' )
                            self.logger.debug(f'RECEIVED: {received}')