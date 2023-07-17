import json
import pickle

class Message:
    ALLOWED_TYPES = [
        "GET",
        "GET_OK",
        "GET_NULL",
        "PUT",
        "PUT_OK",
        "REPLICATION",
        "REPLICATION_OK",
        "ERROR",
    ]

    def __init__(self, type, key, value, timestamp, c_IP, c_port, s_IP, s_port):
        self.key = key
        self.value = value
        self.timestamp = timestamp
        self.type = self.validate_type(type)
        self.c_IP = c_IP
        self.c_port = c_port
        self.s_IP = s_IP
        self.s_port = s_port

    def validate_type(self, type):
        if type not in self.ALLOWED_TYPES:
            raise ValueError(
                f"Invalid message type. Allowed types: {', '.join(self.ALLOWED_TYPES)}"
            )
        return type

    def to_json(self):
        return json.dumps(self.__dict__)

def codify_message(message):
    serialized_message = pickle.dumps(message)
    message_length = len(serialized_message).to_bytes(4, byteorder='big')
    return message_length + serialized_message

def decodify_message(message):
    message_length = int.from_bytes(message[:4], byteorder='big')
    serialized_message = message[4:4+message_length]
    return pickle.loads(serialized_message)

    
