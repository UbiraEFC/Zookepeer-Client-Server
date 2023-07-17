import socket
import sys
import random

from message import Message
from message import codify_message
from message import decodify_message


class Client:
    # Método construtor da classe Client
    def __init__(self, IP, port, s1_IP, s1_port, s2_IP, s2_port, s3_IP, s3_port):
        # Define os atributos da classe com os valores fornecidos
        self.IP = IP

        # Armazena os IPs e portas dos servidores em uma lista de tuplas
        self.servers = [(s1_IP, s1_port), (s2_IP, s2_port), (s3_IP, s3_port)]

        # Cria um socket e vincula-o ao endereço IP e porta fornecidos
        self.s = socket.socket()
        self.s.bind((self.IP, port))
        # Coloca o socket em modo de escuta para aceitar conexões de servidores
        self.s.listen(2)
        # Obtém a porta real em que o socket está ouvindo e armazena no atributo port
        self.port = self.s.getsockname()[1]

        # Inicializa o dicionário de arquivos vazio
        self.files = {}

        # Imprime informações sobre o cliente iniciado
        print("Client started at IP: " + self.IP + " Port: " + str(self.port))

    # Método para obter um servidor aleatório da lista de servidores
    def get_random_server(self):
        server = random.choice(self.servers)
        return server

    # Método para baixar um arquivo do servidor
    def get(self, key):
        try:
            # Obtém um servidor aleatório
            server = self.get_random_server()
            # Cria um socket e conecta-se ao servidor obtido
            s = socket.socket()
            s.connect(server)

            # Define o timestamp como 0
            ts = 0

            # Obtém o item com a chave especificada
            item = self.files.get(key)

            # Caso o item exista, atualiza o valor do timestamp
            if item is not None:
                ts = item["timestamp"]

            # Cria uma mensagem do tipo GET com a chave fornecida e o timestamp atualizado
            message = Message(
                "GET", key, None, ts, self.IP, self.port, server[0], server[1]
            )

            # Envia a mensagem ao servidor
            s.sendall(codify_message(message))
            # Recebe a resposta do servidor
            json_str_received = s.recv(1024)

            # Decodifica a resposta recebida
            response = decodify_message(json_str_received)

            # Verifica se a resposta é do tipo GET_OK
            if response.type == "GET_OK":
                # Adiciona o item ao dicionário de arquivos com o valor e timestamp especificados
                self.files[key] = {
                    "value": response.value,
                    "timestamp": response.timestamp,
                }
                # Imprime informações sobre a requisição GET realizada pelo cliente
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

            # Verifica se a resposta é do tipo GET_NULL
            elif response.type == "GET_NULL":
                # Imprime informações sobre uma requisição GET que não encontrou o item especificado
                print(self.GET_NULL_CLIENT_PRINT(response.key))

            # Verifica se a resposta é do tipo ERROR
            elif response.type == "ERROR":
                # Imprime a mensagem de erro recebida na resposta
                print(response.value)

            else:
                # Caso contrário, informa que houve um erro de comunicação com o servidor
                print("Error to communicate with server")

            # Fecha a conexão com o servidor
            s.close()

        # Trata exceções que possam ocorrer durante a execução do método
        except Exception as e:
            print("Internal error: {}", e)
            s.close()
            sys.exit()

    # Método para registrar um arquivo no servidor
    def put(self, key, value):
        try:
            # Obtém um servidor aleatório
            server = self.get_random_server()
            # Cria um socket e conecta-se ao servidor obtido
            s = socket.socket()
            s.connect(server)

            # Define o timestamp como 0
            ts = 0

            # Cria uma mensagem do tipo PUT com a chave e o valor fornecidos
            message = Message(
                "PUT", key, value, ts, self.IP, self.port, server[0], server[1]
            )

            # Envia a mensagem ao servidor
            s.sendall(codify_message(message))
            # Recebe a resposta do servidor
            json_str_received = s.recv(1024)

            # Decodifica a resposta recebida
            response = decodify_message(json_str_received)

            # Verifica se a resposta é do tipo PUT_OK
            if response.type == "PUT_OK":
                # Adiciona o item ao dicionário de arquivos com o valor e timestamp especificados
                self.files[key] = {"value": value, "timestamp": response.timestamp}
                # Imprime informações sobre a requisição PUT realizada pelo cliente
                print(
                    self.PUT_CLIENT_PRINT(
                        key, value, response.timestamp, server[0], server[1]
                    )
                )

            # Verifica se a resposta é do tipo ERROR
            elif response.type == "ERROR":
                # Imprime a mensagem de erro recebida na resposta
                print(response.value)

            else:
                # Caso contrário, informa que houve um erro de comunicação com o servidor
                print("Error to communicate with server")

            # Fecha a conexão com o servidor
            s.close()

        # Trata exceções que possam ocorrer durante a execução do método
        except Exception as e:
            print("Internal error: {}", e)
            s.close()
            sys.exit()

    # Método para executar o cliente
    def run(self):
        # Entra em um loop infinito onde o usuário pode escolher entre diferentes ações relacionadas ao cliente
        while True:
            # Solicita ao usuário que insira a opção desejada:
            # "PUT" para registrar um arquivo no servidor
            # "GET" para baixar um arquivo do servidor
            choice = input("Choise an option: PUT or GET\n")

            # Com base na opção escolhida, são solicitados os parâmetros relevantes e os métodos apropriados do cliente são chamados

            if choice == "PUT" or choice == "put":
                # Solicita ao usuário para inserir a chave e o valor a serem registrados
                key = input("Enter the key to register: \n")
                value = input("Enter the value to register: \n")
                # Chama o método put do cliente com a chave e o valor fornecidos
                self.put(int(key), value)

            elif choice == "GET" or choice == "get":
                # Solicita ao usuário para inserir a chave a ser buscada
                key = input("Enter the key to search: \n")
                # Chama o método get do cliente com a chave fornecida
                self.get(int(key))

            else:
                # Se uma opção inválida for fornecida, o loop é encerrado
                print("Ending proccess")
                sys.exit()

    # Método para imprimir informações sobre uma requisição GET realizada pelo cliente
    def GET_CLIENT_PRINT(self, s_IP, s_port, key, value, c_timestamp, s_timestamp):
        return "GET key:{} value:{} obtido do servidor {}:{}, meu timestamp {} e do servidor {}.".format(
            key, value, s_IP, s_port, c_timestamp, s_timestamp
        )

    # Método para imprimir informações sobre uma requisição PUT realizada pelo cliente
    def PUT_CLIENT_PRINT(self, key, value, s_timestamp, s_IP, s_port):
        return (
            "PUT_OK key:{} value:{} timestamp:{} realizada no servidor {}:{}.".format(
                key, value, s_timestamp, s_IP, s_port
            )
        )

    # Método para imprimir informações sobre uma requisição GET que não encontrou o item especificado
    def GET_NULL_CLIENT_PRINT(self, key):
        return ("GET key:{} não encontrado.".format(key))


# Solicita ao usuário para iniciar um cliente
choice = input("type INIT to start a Client\n")

# Verifica se a escolha do usuário é para iniciar um cliente
if choice == "INIT" or choice == "" or choice == "init":
    # Solicita ao usuário para inserir o IP do cliente
    c_IP = input("Enter the Client IP: \n")
    # Se o usuário não inserir um IP, o padrão será 127.0.0.1
    if c_IP == "":
        c_IP = "127.0.0.1"

    # Solicita ao usuário para inserir a porta do cliente
    while True:
        c_port_input = input("Enter the Client port: \n")
        # Se o usuário não inserir uma porta, o padrão será 0
        if c_port_input == "":
            c_port = 0
            break
        # Verifica se a porta inserida é um número inteiro
        elif c_port_input.isdigit():
            c_port = int(c_port_input)
            break
        else:
            # Caso contrário, informa ao usuário que a porta deve ser um número inteiro ou vazia
            print("The Client port should be either an integer or empty.")

    # Solicita ao usuário para inserir o IP do servidor 1
    s1_IP = input("Enter the Server 1 IP: \n")
    # Se o usuário não inserir um IP, o padrão será 127.0.0.1
    if s1_IP == "":
        s1_IP = "127.0.0.1"

    # Solicita ao usuário para inserir a porta do servidor 1
    while True:
        s1_port_input = input("Enter the Server 1 port: \n")
        # Se o usuário não inserir uma porta, o padrão será 10097
        if s1_port_input == "":
            s1_port = 10097
            break
        # Verifica se a porta inserida é um número inteiro
        elif s1_port_input.isdigit():
            s1_port = int(s1_port_input)
            break
        else:
            # Caso contrário, informa ao usuário que a porta deve ser um número inteiro ou vazia
            print("The Server 1 port should be either an integer or empty.")

    # Solicita ao usuário para inserir o IP do servidor 2
    s2_IP = input("Enter the Server 2 IP: \n")
    # Se o usuário não inserir um IP, o padrão será 127.0.0.1
    if s2_IP == "":
        s2_IP = "127.0.0.1"

    # Solicita ao usuário para inserir a porta do servidor 2
    while True:
        s2_port_input = input("Enter the Server 2 port: \n")
        # Se o usuário não inserir uma porta, o padrão será 10098
        if s2_port_input == "":
            s2_port = 10098
            break
        # Verifica se a porta inserida é um número inteiro
        elif s2_port_input.isdigit():
            s2_port = int(s2_port_input)
            break
        else:
            # Caso contrário, informa ao usuário que a porta deve ser um número inteiro ou vazia
            print("The Server 2 port should be either an integer or empty.")

    # Solicita ao usuário para inserir o IP do servidor 3
    s3_IP = input("Enter the Server 3 IP: \n")
    # Se o usuário não inserir um IP, o padrão será 127.0.0.1
    if s3_IP == "":
        s3_IP = "127.0.0.1"

        # Solicita ao usuário para inserir a porta do servidor 3
    while True:
        s3_port_input = input("Enter the Server 3 port: \n")
        # Se o usuário não inserir uma porta, o padrão será 10099
        if s3_port_input == "":
            s3_port = 10099
            break
        # Verifica se a porta inserida é um número inteiro
        elif s3_port_input.isdigit():
            s3_port = int(s3_port_input)
            break
        else:
            # Caso contrário, informa ao usuário que a porta deve ser um número inteiro ou vazia
            print("The Server 3 port should be either an integer or empty.")

    # Cria uma instância da classe Client com os IPs e portas do cliente e servidores
    c = Client(c_IP, c_port, s1_IP, s1_port, s2_IP, s2_port, s3_IP, s3_port)
    # Executa o método run da instância de Client criada acima
    c.run()

else:
    # Caso a escolha do usuário não seja para iniciar um cliente, encerra o processo.
    print("Ending proccess")
    sys.exit()
    