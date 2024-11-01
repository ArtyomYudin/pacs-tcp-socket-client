import asyncio
import json
import os

from classes.db import DB
from classes.tcpclient import TcpClient
from utils.logger import get_logger
from utils.functions import create_buffer, datetime_to_timestamp

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

async def receive_data(client):
    with client:
        client.sendall(create_buffer(__filter_events_command))
        # print(create_buffer(filter_events_command))
        while True:
            data = client.recv(1024)
            if data:
                received = json.loads(data[4:].decode('utf-8'))
                match received['Command']:
                    case 'ping':
                        client.sendall(create_buffer(__ping_command))
                        logger.debug(f'RECEIVED: {received}')
                    case 'events':
                        logger.debug(f'RECEIVED: {received}')
                        ev_time = received['Data'][0]['EvTime']
                        ev_ap = received['Data'][0]['EvAddr']
                        ev_owner = received['Data'][0]['EvUser']
                        ev_card = received['Data'][0]['EvCard']
                        ev_code = received['Data'][0]['EvCode']
                        await db.execute('''
                            INSERT INTO public.pacs_event(created, ap_id, owner_id, card, code)
                            VALUES($1, $2, $3, $4, $5)
                            ''', datetime_to_timestamp(ev_time), ev_ap, ev_owner, ev_card, ev_code)


if __name__ == '__main__':

    logger = get_logger(True)

    pacs_tcp_client = TcpClient(host=HOST, port=PORT, server_key=server_key, server_cert=server_cert, logger=logger)
    db = DB(user='itsupport', password='gRzXJHxq7qLM', database='itsupport', host='postgresql')

    tcp_client = pacs_tcp_client.connect()

    asyncio.run(receive_data(tcp_client))