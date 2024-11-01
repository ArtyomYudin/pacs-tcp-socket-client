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