import pickle


class Message:
    # Lista de tipos de mensagens permitidos
    ALLOWED_TYPES = [
        "GET",
        "GET_OK",
        "GET_NULL",
        "PUT",
        "PUT_OK",
        "PUT_ERROR",
        "REPLICATION",
        "REPLICATION_OK",
        "ERROR",
    ]

    # Método construtor da classe Message
    def __init__(self, type, key, value, timestamp, c_IP, c_port, s_IP, s_port):
        # Define os atributos da classe com os valores fornecidos
        self.key = key
        self.value = value
        self.timestamp = timestamp
        # Valida o tipo da mensagem e define o atributo type
        self.type = self.validate_type(type)
        self.c_IP = c_IP
        self.c_port = c_port
        self.s_IP = s_IP
        self.s_port = s_port

    # Método para validar o tipo da mensagem
    def validate_type(self, type):
        # Verifica se o tipo fornecido está na lista de tipos permitidos
        if type not in self.ALLOWED_TYPES:
            # Caso contrário, lança uma exceção com uma mensagem de erro
            raise ValueError(
                f"Invalid message type. Allowed types: {', '.join(self.ALLOWED_TYPES)}"
            )
        return type


# Função para codificar uma mensagem em bytes para ser enviada através de um socket
def codify_message(message):
    # Serializa a mensagem usando o módulo pickle
    serialized_message = pickle.dumps(message)
    # Obtém o tamanho da mensagem serializada em bytes e converte para um inteiro de 4 bytes em ordem big-endian
    message_length = len(serialized_message).to_bytes(4, byteorder="big")
    # Retorna a concatenação do tamanho da mensagem e da mensagem serializada
    return message_length + serialized_message


# Função para decodificar uma mensagem recebida em bytes através de um socket
def decodify_message(message):
    # Obtém o tamanho da mensagem a partir dos primeiros 4 bytes e converte para um inteiro
    message_length = int.from_bytes(message[:4], byteorder="big")
    # Obtém a mensagem serializada a partir dos próximos bytes até o tamanho da mensagem
    serialized_message = message[4 : 4 + message_length]
    # Desserializa a mensagem usando o módulo pickle e retorna o objeto Message resultante
    return pickle.loads(serialized_message)
