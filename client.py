import socket
import sys
import random

from message import Message
from message import codify_message
from message import decodify_message


class Client:
    def __init__(self, IP, port, s1_IP, s1_port, s2_IP, s2_port, s3_IP, s3_port):
        self.IP = IP

        self.servers = [(s1_IP, s1_port), (s2_IP, s2_port), (s3_IP, s3_port)]

        self.s = socket.socket()
        self.s.bind((self.IP, port))
        # O socket começa a ouvir por conexões.
        self.s.listen(2)
        # A porta real em que o socket está ouvindo é obtida e armazenada no atributo port.
        self.port = self.s.getsockname()[1]

        self.files = {}

        print("Client started at IP: " + self.IP + " Port: " + str(self.port))

    def get_random_server(self):
        server = random.choice(self.servers)
        return server

    def get(self, key):
        try:
            server = self.get_random_server()
            s = socket.socket()
            s.connect(server)

            ts = 0

            item = self.files.get(key)

            if item is not None:
                ts = item["timestamp"]

            message = Message(
                "GET", key, None, ts, self.IP, self.port, server[0], server[1]
            )

            s.sendall(codify_message(message))
            json_str_received = s.recv(1024)

            response = decodify_message(json_str_received)

            if response.type == "GET_OK":
                self.files[key] = {
                    "value": response.value,
                    "timestamp": response.timestamp,
                }
                print(
                    self.GET_CLIENT_PRINT(
                        server[0],
                        server[1],
                        key,
                        response.value,
                        ts,
                        response.timestamp,
                    )
                )

            elif response.type == "GET_NULL":
                print(self.GET_NULL_CLIENT_PRINT(response.key))

            elif response.type == "ERROR":
                print(response.value)

            else:
                print("Error to communicate with server")

            s.close()

        except Exception as e:
            print("Internal error: {}", e)
            s.close()
            sys.exit()

    def put(self, key, value):
        try:
            server = self.get_random_server()
            s = socket.socket()
            s.connect(server)

            ts = 0

            message = Message(
                "PUT", key, value, ts, self.IP, self.port, server[0], server[1]
            )

            s.sendall(codify_message(message))
            json_str_received = s.recv(1024)

            response = decodify_message(json_str_received)

            if response.type == "PUT_OK":
                self.files[key] = {"value": value, "timestamp": response.timestamp}
                print(
                    self.PUT_CLIENT_PRINT(
                        key, value, response.timestamp, server[0], server[1]
                    )
                )

            elif response.type == "ERROR":
                print(response.value)

            else:
                print("Error to communicate with server")

            s.close()

        except Exception as e:
            print("Internal error: {}", e)
            s.close()
            sys.exit()

    def run(self):
        # Entra em um loop infinito onde o usuário pode escolher entre diferentes ações relacionadas ao Client.
        while True:
            # Solicita ao usuário que insira a opção desejada:
            # "PUT" para registrar um arquivo no servidor
            # "GET" para baixar um arquivo do servidor
            choice = input("Choise an option: PUT or GET\n")

            # Com base na opção escolhida, são solicitados os parâmetros relevantes e os métodos apropriados do Client são chamados.

            if choice == "PUT" or choice == "put":
                key = input("Enter the key to register: \n")
                value = input("Enter the value to register: \n")
                self.put(int(key), value)

            elif choice == "GET" or choice == "get":
                key = input("Enter the key to search: \n")
                self.get(int(key))

            else:
                # Se uma opção inválida for fornecida, o loop é encerrado.
                print("Ending proccess")
                sys.exit()

    def GET_CLIENT_PRINT(self, s_IP, s_port, key, value, c_timestamp, s_timestamp):
        return "GET key:{} value:{} obtido do servidor {}:{}, meu timestamp {} e do servidor {}.".format(
            key, value, s_IP, s_port, c_timestamp, s_timestamp
        )

    def PUT_CLIENT_PRINT(self, key, value, s_timestamp, s_IP, s_port):
        return (
            "PUT_OK key:{} value:{} timestamp:{} realizada no servidor {}:{}.".format(
                key, value, s_timestamp, s_IP, s_port
            )
        )
    
    def GET_NULL_CLIENT_PRINT(self, key):
        return ("GET key:{} não encontrado.".format(key))


choice = input("type INIT to start a Client\n")

if choice == "INIT" or choice == "" or choice == "init":
    c_IP = input("Enter the Client IP: \n")
    if c_IP == "":
        c_IP = "127.0.0.1"

    while True:
        c_port_input = input("Enter the Client port: \n")
        if c_port_input == "":
            c_port = 0
            break
        elif c_port_input.isdigit():
            c_port = int(c_port_input)
            break
        else:
            print("The Client port should be either an integer or empty.")

    s1_IP = input("Enter the Server 1 IP: \n")
    if s1_IP == "":
        s1_IP = "127.0.0.1"

    while True:
        s1_port_input = input("Enter the Server 1 port: \n")
        if s1_port_input == "":
            s1_port = 10097
            break
        elif s1_port_input.isdigit():
            s1_port = int(s1_port_input)
            break
        else:
            print("The Server 1 port should be either an integer or empty.")

    s2_IP = input("Enter the Server 2 IP: \n")
    if s2_IP == "":
        s2_IP = "127.0.0.1"

    while True:
        s2_port_input = input("Enter the Server 2 port: \n")
        if s2_port_input == "":
            s2_port = 10098
            break
        elif s2_port_input.isdigit():
            s2_port = int(s2_port_input)
            break
        else:
            print("The Server 2 port should be either an integer or empty.")

    s3_IP = input("Enter the Server 3 IP: \n")
    if s3_IP == "":
        s3_IP = "127.0.0.1"

    while True:
        s3_port_input = input("Enter the Server 3 port: \n")
        if s3_port_input == "":
            s3_port = 10099
            break
        elif s3_port_input.isdigit():
            s3_port = int(s3_port_input)
            break
        else:
            print("The Server 3 port should be either an integer or empty.")

    c = Client(c_IP, c_port, s1_IP, s1_port, s2_IP, s2_port, s3_IP, s3_port)
    c.run()

else:
    print("Ending proccess")
    sys.exit()
