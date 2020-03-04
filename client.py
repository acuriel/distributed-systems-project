import os
import threading
import time
from base64 import b64decode

import Pyro4
from Pyro4 import socketutil

import others
from myParser import Parser, Command
from others import Strings

Pyro4.config.COMMTIMEOUT = 10


@Pyro4.expose
class Client:
    UPDATE_SERVERS_TIMEOUT = 2
    UPDATE_SERVERS_TIME = 2
    WAIT_ANSWERS_TIME = 60
    WAIT_AND_TRY_AGAIN_TIME = 5
    MAX_ATTEMPT_NUMBER = 2
    FILE_PART_SIZE = 10000000  # 10mb

    DOWNLOAD_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'downloads')

    def __init__(self):

        daemon = Pyro4.Daemon(host=others.get_ip())

        self.client_uri = daemon.register(self)
        print(self.client_uri)

        self.parser = Parser()
        self.main_server = None  # almacena la uri del servidor principal

        self.current_request_id = 0

        self.start_time_current_request = None
        self.current_request_reports = []
        self.expected_replies = 0
        self.id_lock = threading.Lock()  # para cambiar current_request_id y expected_replies
        self.list_lock = threading.Lock()  # para agregar y eliminar cosas de current_request_reports

        threading.Thread(target=daemon.requestLoop).start()

    def call_exec_cmd(self, request_id, server, command, params):
        server = Pyro4.Proxy(server)
        server.exec_cmd(request_id, command.value, params, self.client_uri)

    def create_filename(self, tags, filename):
        s = ''
        for t in tags:
            s += t
            s += '_'

        s += '()'
        s += filename

        return s

    def start_client(self):
        self.update_main_server()

        while True:
            try:
                s = input()
                command, params = self.parser.parse(s)

                if self.main_server is not None:
                    if command is not None:

                        if command == command.cp:
                            # esto es porque el cp debe comprobar que el archivo exista y enviar el size
                            path, tags, filename = params
                            if os.path.exists(path):
                                fd = os.stat(path)
                                size = fd.st_size
                                params = (path, tags, filename, size)
                            else:
                                print(Strings.FILE_NOT_FOUND.format(path))
                                continue

                        with self.id_lock:
                            self.current_request_id = self.current_request_id + 1

                        with self.list_lock:
                            self.current_request_reports = []

                        future = time.time() + self.WAIT_ANSWERS_TIME
                        attemp = 1
                        with self.id_lock:
                            self.expected_replies = 0

                        while attemp <= self.MAX_ATTEMPT_NUMBER:
                            try:
                                with self.id_lock:
                                    self.expected_replies += 1
                                self.call_exec_cmd(self.current_request_id, self.main_server, command, params)
                                break
                            except:
                                with self.id_lock:
                                    self.expected_replies -= 1
                                print(Strings.UNREACHEABLE_SERVER_ERROR.format(self.main_server))
                                self.main_server = None
                                self.update_main_server()
                                attemp += 1

                        while True:
                            if self.expected_replies <= 0 or time.time() > future:
                                break

                        with self.id_lock:
                            self.current_request_id = 0
                            self.expected_replies = 0

                        if command == command.ls:
                            if len(self.current_request_reports) > 0:
                                print('Archivos hallados')
                                for f in self.current_request_reports:
                                    print(f)
                            else:
                                print('No se encontraron archivos con las caracteristicas definidas')
                            print()

                        if command == command.info:
                            if len(self.current_request_reports) > 0:
                                if self.current_request_reports[0] is not None:
                                    for f in self.current_request_reports:
                                        print(f)
                                else:
                                    print('No se encontro el archivo solicitado')
                            print()

                        if command == command.rm:
                            if len(self.current_request_reports) > 0 and self.current_request_reports[0] is not None:
                                print('Archivos eliminados')
                                for t in self.current_request_reports:
                                    print(t)
                            else:
                                print('No se encontraron archivos para eliminar')
                            print()

                        if command == command.get:
                            if len(self.current_request_reports) > 0:
                                if self.current_request_reports[0] is not None:
                                    # significa que se encontro alguien que tuviera el archivo
                                    tags, filename = params
                                    servers = []
                                    for s in self.current_request_reports:
                                        servers.append(s)
                                    params = (tags, filename, servers)
                                    threading.Thread(target=self.client_get, args=params).start()
                            else:
                                print('No se encontro el archivo {0}'.format(params))

                        if command == command.cp:
                            if len(self.current_request_reports) > 0:
                                if self.current_request_reports[0] is not None:
                                    # significa que el servidor principal encontro algun server que recibiera el archivo
                                    path, tags, filename, size = params
                                    params = (self.current_request_reports[0], path, tags, filename,)
                                    threading.Thread(target=self.client_cp, args=params).start()
                                else:
                                    print('El archivo {0} ya existe'.format(path))

                    else:
                        print('comando invalido')

                else:
                    print('No se encuentran servidores')
            except:
                continue

    def client_cp(self, server_uri, path, tags, filename, offset=0, attempt=0):
        # copiar el archivo por partes
        completed = False
        while attempt < self.MAX_ATTEMPT_NUMBER:
            if os.path.exists(path):
                try:
                    fd = open(path, 'a+b')
                    fd.seek(offset)
                    content = fd.read(self.FILE_PART_SIZE)
                    fd.close()
                except:
                    print(Strings.FILE_LOST_SUDDENLY.format(path))
                    break

                if len(content) > 0:
                    try:
                        server = Pyro4.Proxy(server_uri)
                        print('Enviando offset{0}, tags:{1}, filename{2}'.format(offset, tags, filename))
                        server.fill_file(tags, filename, content, offset)
                        offset += len(content)
                    except:
                        time.sleep(self.WAIT_AND_TRY_AGAIN_TIME)
                        print(Strings.TRYING_AGAIN.format('copiar', path))
                        attempt += 1
                        continue

                else:
                    try:
                        server = Pyro4.Proxy(server_uri)
                        server.fill_file(tags, filename, content, -1)
                        print('Se termino de enviar tags:{0}, filename{1}'.format(tags, filename))
                    except:
                        pass
                    print(Strings.SUCCESFUL_OPERATION.format('copiar', path))
                    completed = True
                    break

            else:
                print(Strings.FILE_LOST_SUDDENLY.format(path))

            if completed:
                print(Strings.OPERATION_FAIL.format('copiar ', path))

    def client_get(self, tags, filename, servers):
        # seleccionar server para copiar
        attempt = -1
        completed = False
        offset = 0

        # crear el archivo en la carpeta predefinida
        path = os.path.join(self.DOWNLOAD_PATH, filename)
        if offset == 0:
            try:
                if os.path.exists(path):
                    print(Strings.FILE_ALREADY_EXISTS.format(path))
                    return
                fd = open(path, 'x')
                fd.close()
                print('Creando archivo tags:{0}, filename:{1}'.format(tags, filename))
            except:
                print(Strings.FILE_LOST_SUDDENLY.format(path))
                print(Strings.OPERATION_FAIL.format('get ', (tags, filename)))
                return

        i = 0
        # empezar a descargar el archivo
        while i != len(servers):
            if os.path.exists(path):
                try:
                    server = Pyro4.Proxy(servers[i])
                    correct, content = server.get_part(tags, filename, offset, self.FILE_PART_SIZE)
                    print('Recibiendo offset:{0}, tags:{1}, filename:{2}'.format(offset, tags, filename))
                    if correct:
                        content = b64decode(content['data'])
                        offset += len(content)
                    else:
                        break
                except:
                    time.sleep(self.WAIT_AND_TRY_AGAIN_TIME)
                    print(Strings.TRYING_AGAIN.format('get', path))
                    if attempt < self.MAX_ATTEMPT_NUMBER:
                        attempt += 1
                    else:
                        attempt = 0
                        i += 1
                    continue

                if len(content) > 0:
                    try:
                        size = os.stat(path).st_size
                        if size <= offset:
                            fd = open(path, 'a+b')
                            fd.seek(offset)
                            fd.write(content)
                            fd.close()
                    except:
                        print(Strings.FILE_LOST_SUDDENLY.format((tags, filename)))
                        break
                else:
                    print(Strings.SUCCESFUL_OPERATION.format('copiar', (tags, filename)))
                    completed = True
                    break
            else:
                print(Strings.FILE_LOST_SUDDENLY.format(path))
                break

        if completed is False:
            print(Strings.OPERATION_FAIL.format('get ', (tags, filename)))

    def report(self, request_id, command, output):
        if request_id == self.current_request_id and self.expected_replies > 0:
            command = Command[command]

            if command == Command.ls or command == Command.info or command == Command.rm or command == Command.cp or command == Command.get:
                with self.list_lock:
                    if len(output) > 0:
                        self.current_request_reports.extend(output)

            with self.id_lock:
                self.expected_replies -= 1

    def update_main_server(self):
        while self.main_server is None:
            self.scan_loop()
            time.sleep(self.UPDATE_SERVERS_TIME)

    def scan_loop(self):
        scanner = socketutil.createBroadcastSocket()
        scanner.settimeout(self.UPDATE_SERVERS_TIMEOUT)

        main_server = None
        try:
            scanner.sendto(b'get_uri_client', ('255.255.255.255', 1212))
        except:
            print('Error al hacer broadcast')

        while True:
            try:
                data, address = scanner.recvfrom(512)
                main_server = data.decode()
                break
            except:
                break

        self.main_server = main_server
        print('Servidor Principal:{0}'.format(self.main_server))


if __name__ == '__main__':
    client = Client()
    client.start_client()

