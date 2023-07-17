import socket
import sys

from threading import Thread
from message import Message
from message import codify_message
from message import decodify_message


class Server:
    # Método construtor da classe Server
    def __init__(
        self, IP, port, s1_IP, s1_port, s2_IP, s2_port, master_IP, master_port
    ):
        # Define os atributos da classe com os valores fornecidos
        self.IP = IP
        self.port = port
        self.servers = [(s1_IP, s1_port), (s2_IP, s2_port)]
        self.master_IP = master_IP
        self.master_port = master_port
        # Verifica se este servidor é o líder (master)
        self.isMaster = self.IP == self.master_IP and self.port == self.master_port

        # Cria um socket e vincula-o ao endereço IP e porta fornecidos
        self.s = socket.socket()
        self.s.bind((self.IP, self.port))
        # Coloca o socket em modo de escuta para aceitar conexões de clientes
        self.s.listen(5)

        # Inicializa o dicionário de arquivos vazio
        self.files = {}

        # Imprime informações sobre o servidor iniciado
        print("Server started at IP: " + self.IP + " Port: " + str(self.port))

    # Método para executar o servidor
    def run(self):
        while True:
        # O método run é executado em um loop infinito

            # Aguarda e aceita uma conexão de cliente usando o socket
            c, addr = self.s.accept()

            # Cria uma nova thread para manipular a conexão com o cliente ou outro servidor
            t = Thread(target=self.server_listen, args=(c, addr))
            t.start()


    # Método para escutar requisições de clientes
    def server_listen(self, c, addr):
        try:
            # Recebe a mensagem do cliente
            json_str_received = c.recv(1024)
            # Decodifica a mensagem recebida
            request = decodify_message(json_str_received)

            # Verifica se a requisição é do tipo GET
            if request.type == "GET":
                # Obtém o item com a chave especificada na requisição
                item = self.files.get(request.key)

                # Caso o item não exista
                if not item:
                    # Imprime informações sobre a requisição GET
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

                    # Cria uma mensagem de resposta do tipo GET_NULL
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
                    # Envia a resposta ao cliente
                    c.sendall(codify_message(response))

                # Caso o timestamp do item seja maior ou igual ao timestamp da requisição
                elif item["timestamp"] >= request.timestamp:
                    # Imprime informações sobre a requisição GET
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

                    # Cria uma mensagem de resposta do tipo GET_OK com o valor e timestamp do item
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
                    # Envia a resposta ao cliente
                    c.sendall(codify_message(response))

                else:
                    # Imprime informações sobre a requisição GET com uma mensagem de erro
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

                    # Cria uma mensagem de resposta do tipo ERROR com uma mensagem de erro
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
                    # Envia a resposta ao cliente
                    c.sendall(codify_message(response))

                # Fecha a conexão com o cliente
                c.close()

                # Verifica se a requisição é do tipo PUT
            elif request.type == "PUT":
                # Caso este servidor seja o líder (master)
                if self.isMaster:
                    # Imprime informações sobre a requisição PUT
                    print(
                        self.PUT_SERVER_MASTER_PRINT(
                            request.c_IP, request.c_port, request.key, request.value
                        )
                    )
                    # Obtém o item com a chave especificada na requisição
                    item = self.files.get(request.key)

                    # Caso o item não exista
                    if not item:
                        # Define o timestamp como 0
                        ts = 0

                        # Adiciona o item ao dicionário de arquivos com o valor e timestamp especificados
                        self.files[request.key] = {
                            "value": request.value,
                            "timestamp": ts,
                        }

                        # Cria uma mensagem de replicação com os dados da requisição PUT
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

                        # Conecta ao primeiro servidor escravo e envia a mensagem de replicação
                        s = socket.socket()
                        s.connect(self.servers[0])
                        s.sendall(codify_message(replication))
                        json_str_received = s.recv(1024)
                        s.close()

                        # Conecta ao segundo servidor escravo e envia a mensagem de replicação
                        s = socket.socket()
                        s.connect(self.servers[1])
                        s.sendall(codify_message(replication))
                        json_str_received = s.recv(1024)
                        s.close()

                        # Imprime informações sobre a replicação
                        print(
                            self.REPLICATION_SERVER_MASTER_PRINT(
                                request.c_IP,
                                request.c_port,
                                request.key,
                                request.timestamp,
                            )
                        )

                        # Cria uma mensagem de resposta do tipo PUT_OK com os dados da requisição PUT
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
                        # Envia a resposta ao cliente
                        c.sendall(codify_message(response))

                    # Caso o timestamp do item seja igual ao timestamp da requisição
                    elif item["timestamp"] == request.timestamp:
                        # Atualiza o item no dicionário de arquivos com o valor e timestamp especificados
                        self.files[request.key] = {
                            "value": request.value,
                            "timestamp": request.timestamp + 1,
                        }

                        # Cria uma mensagem de replicação com os dados da requisição PUT e timestamp incrementado
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

                        # Conecta ao primeiro servidor escravo e envia a mensagem de replicação
                        s = socket.socket()
                        s.connect(self.servers[0])
                        s.sendall(codify_message(replication))
                        json_str_received = s.recv(1024)
                        s.close()

                        # Conecta ao segundo servidor escravo e envia a mensagem de replicação
                        s = socket.socket()
                        s.connect(self.servers[1])
                        s.sendall(codify_message(replication))
                        json_str_received = s.recv(1024)
                        s.close()

                        # Imprime informações sobre a replicação
                        print(
                            self.REPLICATION_SERVER_MASTER_PRINT(
                                request.c_IP,
                                request.c_port,
                                request.key,
                                request.timestamp + 1,
                            )
                        )

                        # Cria uma mensagem de resposta do tipo PUT_OK com os dados da requisição PUT e timestamp incrementado
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
                        # Envia a resposta ao cliente
                        c.sendall(codify_message(response))

                    # Caso o timestamp do item seja maior que o timestamp da requisição
                    elif item["timestamp"] > request.timestamp:
                        # Cria uma mensagem de resposta do tipo PUT_OK com os dados do item existente
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
                        # Envia a resposta ao cliente
                        c.sendall(codify_message(response))

                    else:
                        # Atualiza o item no dicionário de arquivos com o valor e timestamp especificados
                        self.files[request.key] = {
                            "value": request.value,
                            "timestamp": request.timestamp,
                        }

                        # Cria uma mensagem de replicação com os dados da requisição PUT
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

                        # Conecta ao primeiro servidor escravo e envia a mensagem de replicação
                        s = socket.socket()
                        s.connect(self.servers[0])
                        s.sendall(codify_message(replication))
                        json_str_received = s.recv(1024)
                        s.close()

                        # Conecta ao segundo servidor escravo e envia a mensagem de replicação
                        s = socket.socket()
                        s.connect(self.servers[1])
                        s.sendall(codify_message(replication))
                        json_str_received = s.recv(1024)
                        s.close()

                        # Imprime informações sobre a replicação
                        print(
                            self.REPLICATION_SERVER_MASTER_PRINT(
                                request.c_IP,
                                request.c_port,
                                request.key,
                                request.timestamp,
                            )
                        )

                        # Cria uma mensagem de resposta do tipo PUT_OK com os dados da requisição PUT
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
                    # Caso este servidor não seja o líder (master)
                    # Imprime informações sobre a requisição PUT
                    print(self.PUT_SERVER_SLAVE_PRINT(request.key, request.value))
                    # Conecta ao servidor líder (master) e envia a requisição PUT recebida
                    s = socket.socket()
                    s.connect((self.master_IP, self.master_port))
                    s.sendall(codify_message(request))
                    # Recebe a resposta do servidor líder (master)
                    json_str_received = s.recv(1024)
                    request = decodify_message(json_str_received)

                    # Verifica se a resposta é do tipo PUT_OK
                    if request.type == "PUT_OK":
                        # Cria uma mensagem de resposta do tipo PUT_OK com os dados da resposta do servidor líder (master)
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
                        # Envia a resposta ao cliente
                        c.sendall(codify_message(response))

                # Fecha a conexão com o cliente
                c.close()

            # Verifica se a requisição é do tipo REPLICATION
            elif request.type == "REPLICATION":
                # Imprime informações sobre a replicação
                print(
                    self.REPLICATION_SERVER_SLAVE_PRINT(
                        request.key, request.value, request.timestamp
                    )
                )
                # Adiciona o item ao dicionário de arquivos com o valor e timestamp especificados
                self.files[request.key] = {
                    "value": request.value,
                    "timestamp": request.timestamp,
                }

                # Cria uma mensagem de resposta do tipo REPLICATION_OK com os dados da requisição REPLICATION
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
                # Envia a resposta ao cliente
                c.sendall(codify_message(response))
                # Fecha a conexão com o cliente
                c.close()

        # Trata exceções que possam ocorrer durante a execução do método
        except Exception as e:
            print("Internal server error: {}", e)
            c.close()
            sys.exit()


    # Método para imprimir informações de replicação de um servidor escravo
    def REPLICATION_SERVER_SLAVE_PRINT(self, key, value, timestamp):
        return "REPLICATION key:{} value:{} timestamp:{}.".format(key, value, timestamp)

    # Método para imprimir informações de replicação de um servidor líder (master)
    def REPLICATION_SERVER_MASTER_PRINT(self, c_IP, c_port, key, s_timestamp):
        return "Enviando PUT_OK ao Cliente {}:{} da key:{} ts:{}.".format(
            c_IP, c_port, key, s_timestamp
        )

    # Método para imprimir informações de um PUT em um servidor escravo
    def PUT_SERVER_SLAVE_PRINT(self, key, value):
        return "Encaminhando PUT key:{} value: {}.".format(key, value)

    # Método para imprimir informações de um PUT em um servidor líder (master)
    def PUT_SERVER_MASTER_PRINT(self, c_IP, c_port, key, value):
        return "Cliente {}:{} PUT key:{} value:{}.".format(c_IP, c_port, key, value)

    # Método para imprimir informações de um GET em um servidor
    def GET_SERVER_PRINT(self, c_IP, c_port, key, value, c_timestamp, s_timestamp):
        return "Cliente {}:{} GET key:{} ts:{}. Meu ts é {}, portanto devolvendo {}.".format(
            c_IP, c_port, key, c_timestamp, s_timestamp, value
        )

    # Método para retornar uma mensagem de erro
    def ERROR(self):
        return "TRY_OTHER_SERVER_OR_LATER"



# Solicita ao usuário para iniciar um servidor
choice = input("type INIT to start a Server\n")

# Verifica se a escolha do usuário é para iniciar um servidor
if choice == "INIT" or choice == "" or choice == "init":
    # Solicita ao usuário para inserir o IP do servidor
    s_IP = input("Enter your own server IP: \n")
    # Se o usuário não inserir um IP, o padrão será 127.0.0.1
    if s_IP == "":
        s_IP = "127.0.0.1"

    # Solicita ao usuário para inserir a porta do servidor
    while True:
        s_port_input = input("Enter your own server port: \n")
        # Verifica se a porta inserida é um número inteiro
        if s_port_input.isdigit():
            s_port = int(s_port_input)
            break
        else:
            # Caso contrário, informa ao usuário que a porta deve ser um número inteiro ou vazia
            print("The server port should be either an integer or empty.")

    # Solicita ao usuário para inserir o IP do servidor 1
    s1_IP = input("Enter the server 1 IP: \n")
    # Se o usuário não inserir um IP, o padrão será 127.0.0.1
    if s1_IP == "":
        s1_IP = "127.0.0.1"

    # Solicita ao usuário para inserir a porta do servidor 1
    while True:
        s1_port_input = input("Enter the server 1 port: \n")
        # Verifica se a porta inserida é um número inteiro
        if s1_port_input.isdigit():
            s1_port = int(s1_port_input)
            break
        else:
            # Caso contrário, informa ao usuário que a porta deve ser um número inteiro ou vazia
            print("The server port should be either an integer or empty.")

    # Solicita ao usuário para inserir o IP do servidor 2
    s2_IP = input("Enter the server 2 IP: \n")
    # Se o usuário não inserir um IP, o padrão será 127.0.0.1
    if s2_IP == "":
        s2_IP = "127.0.0.1"

    # Solicita ao usuário para inserir a porta do servidor 2
    while True:
        s2_port_input = input("Enter the server 2 port: \n")
        # Verifica se a porta inserida é um número inteiro
        if s2_port_input.isdigit():
            s2_port = int(s2_port_input)
            break
        else:
            # Caso contrário, informa ao usuário que a porta deve ser um número inteiro ou vazia
            print("The server port should be either an integer or empty.")

    # Solicita ao usuário para inserir o IP do líder (master)
    master_IP = input("Enter the master IP: \n")
    # Se o usuário não inserir um IP, o padrão será 127.0.0.1
    if master_IP == "":
        master_IP = "127.0.0.1"

    # Solicita ao usuário para inserir a porta do líder (master)
    while True:
        master_port_input = input("Enter the master port: \n")
        # Verifica se a porta inserida é um número inteiro
        if master_port_input.isdigit():
            master_port = int(master_port_input)
            break
        else:
            # Caso contrário, informa ao usuário que a porta deve ser um número inteiro ou vazia
            print("The server port should be either an integer or empty.")

    # Cria uma instância da classe Server com os IPs e portas dos servidores e líder (master)
    s = Server(s_IP, s_port, s1_IP, s1_port, s2_IP, s2_port, master_IP, master_port)
    # Executa o método run da instância de Server criada acima
    s.run()

else:
    # Caso a escolha do usuário não seja para iniciar um servidor, encerra o processo.
    print("Ending process")
    sys.exit()