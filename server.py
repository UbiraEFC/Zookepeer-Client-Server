import socket
import sys

from threading import Thread
from message import Message
from message import codify_message
from message import decodify_message


class Server:
    def __init__(
        self, IP, port, s1_IP, s1_port, s2_IP, s2_port, master_IP, master_port
    ):
        self.IP = IP
        self.port = port
        self.servers = [(s1_IP, s1_port), (s2_IP, s2_port)]
        self.master_IP = master_IP
        self.master_port = master_port
        self.isMaster = self.IP == self.master_IP and self.port == self.master_port

        # Um socket s é criado e vinculado ao endereço IP e porta fornecidos.
        self.s = socket.socket()
        self.s.bind((self.IP, self.port))
        self.s.listen(5)

        self.files = {}

        print("Server started at IP: " + self.IP + " Port: " + str(self.port))

    def run(self):
        while True:
            # O método run é executado em um loop infinito.

            # Ele aguarda e aceita uma conexão de cliente usando o socket.
            c, addr = self.s.accept()

            # Em seguida, cria uma nova thread para manipular a conexão com o outro servidor ou cliente.
            t = Thread(target=self.server_listen, args=(c, addr))
            t.start()

    def server_listen(self, c, addr):
        try:
            json_str_received = c.recv(1024)
            request = decodify_message(json_str_received)

            if request.type == "GET":
                item = self.files.get(request.key)

                if not item:
                    print(
                        self.GET_SERVER_PRINT(
                            request.c_IP,
                            request.c_port,
                            request.key,
                            "NULL",
                            request.timestamp,
                            0,
                        )
                    )

                    response = Message(
                        "GET_NULL",
                        request.key,
                        None,
                        None,
                        request.c_IP,
                        request.c_port,
                        self.IP,
                        self.port,
                    )
                    c.sendall(codify_message(response))

                elif item["timestamp"] >= request.timestamp:
                    print(
                        self.GET_SERVER_PRINT(
                            request.c_IP,
                            request.c_port,
                            request.key,
                            item["value"],
                            request.timestamp,
                            item["timestamp"],
                        )
                    )

                    response = Message(
                        "GET_OK",
                        request.key,
                        item["value"],
                        item["timestamp"],
                        request.c_IP,
                        request.c_port,
                        self.IP,
                        self.port,
                    )
                    c.sendall(codify_message(response))

                else:
                    print(
                        self.GET_SERVER_PRINT(
                            request.c_IP,
                            request.c_port,
                            request.key,
                            self.ERROR(),
                            request.timestamp,
                            item["timestamp"],
                        )
                    )

                    response = Message(
                        "ERROR",
                        request.key,
                        self.ERROR(),
                        None,
                        request.c_IP,
                        request.c_port,
                        self.IP,
                        self.port,
                    )
                    c.sendall(codify_message(response))

                c.close()

            elif request.type == "PUT":
                if self.isMaster:
                    print(
                        self.PUT_SERVER_MASTER_PRINT(
                            request.c_IP, request.c_port, request.key, request.value
                        )
                    )
                    item = self.files.get(request.key)

                    if not item:
                        ts = 0

                        self.files[request.key] = {
                            "value": request.value,
                            "timestamp": ts,
                        }

                        replication = Message(
                            "REPLICATION",
                            request.key,
                            request.value,
                            ts,
                            request.c_IP,
                            request.c_port,
                            self.IP,
                            self.port,
                        )

                        s = socket.socket()
                        s.connect(self.servers[0])
                        s.sendall(codify_message(replication))
                        json_str_received = s.recv(1024)
                        s.close()

                        s = socket.socket()
                        s.connect(self.servers[1])
                        s.sendall(codify_message(replication))
                        json_str_received = s.recv(1024)
                        s.close()

                        print(
                            self.REPLICATION_SERVER_MASTER_PRINT(
                                request.c_IP,
                                request.c_port,
                                request.key,
                                request.timestamp,
                            )
                        )

                        response = Message(
                            "PUT_OK",
                            request.key,
                            request.value,
                            ts,
                            request.c_IP,
                            request.c_port,
                            self.IP,
                            self.port,
                        )
                        c.sendall(codify_message(response))

                    elif item["timestamp"] == request.timestamp:
                        self.files[request.key] = {
                            "value": request.value,
                            "timestamp": request.timestamp + 1,
                        }

                        replication = Message(
                            "REPLICATION",
                            request.key,
                            request.value,
                            request.timestamp + 1,
                            request.c_IP,
                            request.c_port,
                            self.IP,
                            self.port,
                        )

                        s = socket.socket()
                        s.connect(self.servers[0])
                        s.sendall(codify_message(replication))
                        json_str_received = s.recv(1024)
                        s.close()

                        s = socket.socket()
                        s.connect(self.servers[1])
                        s.sendall(codify_message(replication))
                        json_str_received = s.recv(1024)
                        s.close()

                        print(
                            self.REPLICATION_SERVER_MASTER_PRINT(
                                request.c_IP,
                                request.c_port,
                                request.key,
                                request.timestamp + 1,
                            )
                        )

                        response = Message(
                            "PUT_OK",
                            request.key,
                            request.value,
                            request.timestamp + 1,
                            request.c_IP,
                            request.c_port,
                            self.IP,
                            self.port,
                        )
                        c.sendall(codify_message(response))

                    elif item["timestamp"] > request.timestamp:
                        response = Message(
                            "PUT_OK",
                            request.key,
                            item["value"],
                            item["timestamp"],
                            request.c_IP,
                            request.c_port,
                            self.IP,
                            self.port,
                        )
                        c.sendall(codify_message(response))

                    else:
                        self.files[request.key] = {
                            "value": request.value,
                            "timestamp": request.timestamp,
                        }

                        replication = Message(
                            "REPLICATION",
                            request.key,
                            request.value,
                            request.timestamp,
                            request.c_IP,
                            request.c_port,
                            self.IP,
                            self.port,
                        )

                        s = socket.socket()
                        s.connect(self.servers[0])
                        s.sendall(codify_message(replication))
                        json_str_received = s.recv(1024)
                        s.close()

                        s = socket.socket()
                        s.connect(self.servers[1])
                        s.sendall(codify_message(replication))
                        json_str_received = s.recv(1024)
                        s.close()

                        print(
                            self.REPLICATION_SERVER_MASTER_PRINT(
                                request.c_IP,
                                request.c_port,
                                request.key,
                                request.timestamp,
                            )
                        )

                        response = Message(
                            "PUT_OK",
                            request.key,
                            request.value,
                            request.timestamp,
                            request.c_IP,
                            request.c_port,
                            self.IP,
                            self.port,
                        )
                        c.sendall(codify_message(response))

                else:
                    print(self.PUT_SERVER_SLAVE_PRINT(request.key, request.value))
                    s = socket.socket()
                    s.connect((self.master_IP, self.master_port))
                    s.sendall(codify_message(request))
                    json_str_received = s.recv(1024)
                    request = decodify_message(json_str_received)

                    if request.type == "PUT_OK":
                        response = Message(
                            "PUT_OK",
                            request.key,
                            request.value,
                            request.timestamp,
                            request.c_IP,
                            request.c_port,
                            self.IP,
                            self.port,
                        )
                        c.sendall(codify_message(response))

                c.close()

            elif request.type == "REPLICATION":
                print(
                    self.REPLICATION_SERVER_SLAVE_PRINT(
                        request.key, request.value, request.timestamp
                    )
                )
                self.files[request.key] = {
                    "value": request.value,
                    "timestamp": request.timestamp,
                }

                response = Message(
                    "REPLICATION_OK",
                    request.key,
                    request.value,
                    request.timestamp,
                    request.c_IP,
                    request.c_port,
                    self.IP,
                    self.port,
                )
                c.sendall(codify_message(response))
                c.close()

        except Exception as e:
            print("Internal server error: {}", e)
            c.close()
            sys.exit()

    def REPLICATION_SERVER_SLAVE_PRINT(self, key, value, timestamp):
        return "REPLICATION key:{} value:{} timestamp:{}.".format(key, value, timestamp)

    def REPLICATION_SERVER_MASTER_PRINT(self, c_IP, c_port, key, s_timestamp):
        return "Enviando PUT_OK ao Cliente {}:{} da key:{} ts:{}.".format(
            c_IP, c_port, key, s_timestamp
        )

    def PUT_SERVER_SLAVE_PRINT(self, key, value):
        return "Encaminhando PUT key:{} value: {}.".format(key, value)

    def PUT_SERVER_MASTER_PRINT(self, c_IP, c_port, key, value):
        return "Cliente {}:{} PUT key:{} value:{}.".format(c_IP, c_port, key, value)

    def GET_SERVER_PRINT(self, c_IP, c_port, key, value, c_timestamp, s_timestamp):
        return "Cliente {}:{} GET key:{} ts:{}. Meu ts é {}, portanto devolvendo {}.".format(
            c_IP, c_port, key, c_timestamp, s_timestamp, value
        )

    def ERROR(self):
        return "TRY_OTHER_SERVER_OR_LATER"


choice = input("type INIT to start a Server\n")

if choice == "INIT" or choice == "" or choice == "init":
    s_IP = input("Enter your own server IP: \n")
    if s_IP == "":
        s_IP = "127.0.0.1"

    while True:
        s_port_input = input("Enter your own server port: \n")
        if s_port_input.isdigit():
            s_port = int(s_port_input)
            break
        else:
            print("The server port should be either an integer or empty.")

    s1_IP = input("Enter the server 1 IP: \n")
    if s1_IP == "":
        s1_IP = "127.0.0.1"

    while True:
        s1_port_input = input("Enter the server 1 port: \n")
        if s1_port_input.isdigit():
            s1_port = int(s1_port_input)
            break
        else:
            print("The server port should be either an integer or empty.")

    s2_IP = input("Enter the server 2 IP: \n")
    if s2_IP == "":
        s2_IP = "127.0.0.1"

    while True:
        s2_port_input = input("Enter the server 2 port: \n")
        if s2_port_input.isdigit():
            s2_port = int(s2_port_input)
            break
        else:
            print("The server port should be either an integer or empty.")

    master_IP = input("Enter the master IP: \n")
    if master_IP == "":
        master_IP = "127.0.0.1"

    while True:
        master_port_input = input("Enter the master port: \n")
        if master_port_input.isdigit():
            master_port = int(master_port_input)
            break
        else:
            print("The server port should be either an integer or empty.")

    s = Server(s_IP, s_port, s1_IP, s1_port, s2_IP, s2_port, master_IP, master_port)
    s.run()

else:
    print("Ending process")
    sys.exit()
