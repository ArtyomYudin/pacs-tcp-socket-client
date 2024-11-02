import asyncio
import json
import os
from multiprocessing.reduction import recvfds

from classes.db import DB
from classes.tcpclient import TcpClient
from utils.logger import get_logger
from utils.functions import create_buffer, datetime_to_timestamp, insert_event_to_db, chunk_data, load_system_ap, \
    load_system_card_owner

HOST = '172.20.57.7'  # The remote host
PORT: int = 24532

server_key = f'{os.path.dirname(__file__)}/certs/key.pem'
server_cert = f'{os.path.dirname(__file__)}/certs/cert.pem'

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

__userlist_command = json.dumps({
    'Command': 'userlist',
    'Id': 1,
    'Version': 1,
})

__aplist_command = json.dumps({
    'Command': 'aplist',
    'Id': 1,
    'Version': 1,
})


async def receive_data(client):
    with client:
        client.sendall(create_buffer(__filter_events_command))
        client.sendall(create_buffer(__aplist_command))
        client.sendall(create_buffer(__userlist_command))

        while True:
            data = chunk_data(client)
            #data = client.recv(1024)
            if data:
                received = json.loads(data[4:].decode('utf-8'))
                match received['Command']:
                    case 'ping':
                        client.sendall(create_buffer(__ping_command))
                        logger.debug(f'RECEIVED: {received}')
                    case 'events':
                        logger.debug(f'RECEIVED: {received}')
                        await insert_event_to_db(db, received['Data'])
                    case 'userlist':
                        await load_system_card_owner(db, received['Data'])
                        #logger.debug(f'RECEIVED: {received}')
                    case 'aplist':
                        await load_system_ap(db, received['Data'])
                        #logger.debug(f'RECEIVED: {received}')

if __name__ == '__main__':

    logger = get_logger(True)

    pacs_tcp_client = TcpClient(host=HOST, port=PORT, server_key=server_key, server_cert=server_cert, logger=logger)
    db = DB(user='itsupport', password='gRzXJHxq7qLM', database='itsupport', host='postgresql')

    tcp_client = pacs_tcp_client.connect()

    asyncio.run(receive_data(tcp_client))