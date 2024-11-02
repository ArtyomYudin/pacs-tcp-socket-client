import struct
from datetime import datetime as dt


def create_buffer(post_json_data):
    buffer = post_json_data.encode('utf-8')
    buffer_with_byte = bytearray(4 + len(buffer))
    struct.pack_into('<I', buffer_with_byte, 0, len(buffer))
    buffer_with_byte[4:] = buffer
    return bytes(buffer_with_byte)

def datetime_to_timestamp(date_time):
    time_object = dt.strptime(date_time,"%d.%m.%Y %H:%M:%S")
    #ts = int(round(dt.timestamp(time_object)))
    return time_object

def chunk_data(client):
    buff_size = 1024 # 4 KiB
    data = b''
    while True:
        part = client.recv(buff_size)
        data += part
        if len(part) < buff_size:
            # either 0 or end of data
            break
    return data

async def insert_event_to_db(db, received):
    for event_data in received:
        ev_time = event_data['EvTime']
        ev_ap = event_data['EvAddr']
        ev_owner = None if event_data['EvUser'] == 0 else event_data['EvUser']
        ev_card = event_data['EvCard']
        ev_code = event_data['EvCode']
        await db.execute('''
            INSERT INTO public.pacs_event(created, ap_id, owner_id, card, code)
            VALUES($1, $2, $3, $4, $5)
            ''', datetime_to_timestamp(ev_time), ev_ap, ev_owner, ev_card, ev_code)


async def load_system_ap(db, ap_list):
    for ap in ap_list:
        system_id = ap['Id']
        name = ap['Name']
        await db.execute('''
                    INSERT INTO public.pacs_accesspoint(system_id, name)
                    VALUES($1, $2)
                    ON CONFLICT (system_id)
                    DO
                    UPDATE
                    SET name=$2
                    ''', system_id, name)


async def load_system_card_owner(db, user_list):
    for user in user_list:
        system_id = user['Id']
        firstname = user['FirstName']
        secondname = user['SecondName']
        lastname = user['LastName']
        await db.execute('''
                            INSERT INTO public.pacs_cardowner(system_id, firstname, secondname, lastname)
                            VALUES($1, $2, $3, $4)
                            ON CONFLICT (system_id)
                            DO
                            UPDATE
                            SET firstname=$2, secondname=$3, lastname=$4
                            ''', system_id, firstname, secondname, lastname)