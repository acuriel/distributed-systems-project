import os
import queue
import threading
import time
from base64 import b64decode

import Pyro4
import shutil
from Pyro4 import socketutil

import others
import storage
from myParser import Command
from others import Strings

Pyro4.config.COMMTIMEOUT = 100


class Server:
    UPDATE_SERVERS_TIMEOUT = 10
    UPDATE_SERVERS_TIME = 10
    WAIT_ANSWERS_TIME = 25
    CLIENT_PORT = 1212
    SERVERS_PORT = 1212
    IS_DISCONNECTED_TIME_LOOP = 2
    UNEXPECTED_REQUEST = 'Unexpected request: {0}'
    COPIES_NUMBER = 1
    MAX_ATTEMPT_NUMBER = 2
    WAIT_AND_TRY_AGAIN = 5
    FILE_PART_SIZE = 10000000  # 10mb

    def __init__(self):
        threading.Thread(target=self.response_receive_uri).start()

        self.queue = queue.Queue()
        self.not_saved_operations = queue.Queue()
        self.server_uri = None

        self.storage = storage.Storage()

        self.free_space = 0

        self.servers = []
        self.servers_lock = threading.Lock()  # para el uso y la modificacion de la lista de servidores

        self.up_to_date = False
        self.localhost = False
        self.disconnected = True

        self.im_main_server = False
        self.main_server_lock = threading.Lock()

        self.main_server_founded = False
        self.founded_lock = threading.Lock()

        self.main_server_uri = None

        last = self.storage.last_serial()
        self.id_lock = threading.Lock()
        if last is not None:
            self.last_id = last
        else:
            self.last_id = 0

        self.ip = '127.0.0.1'

        self.pending_replic = []
        self.pending_replic_lock = threading.Lock()

        self.process_queue_bool = False

        self.DIRECTORY = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'Files')

        threading.Thread(target=self.process_queue).start()
        threading.Thread(target=self.__is_disconnected_loop).start()
        threading.Thread(target=self.save_new_operations).start()
        threading.Thread(target=self.main_server_loop).start()

    @staticmethod
    def report_to_client(request_id, command, output, client_uri):
        try:
            client = Pyro4.Proxy(client_uri)
            client.report(request_id, command.value, output)
        except:
            print('Se perdio la conexion inesperadamente')

    @Pyro4.expose
    @property
    def get_server_uri(self):
        return self.server_uri

    @Pyro4.expose
    @get_server_uri.setter
    def get_server_uri(self, pserver_uri):
        self.server_uri = pserver_uri

    @staticmethod
    def address_in_server(hash):
        return os.path.join(os.path.dirname(os.path.abspath(__file__)), hash)

    def main_server_cp(self, path, tags, filename, size, client_uri, request_id):
        with self.servers_lock:
            done = False
            for s in self.servers:
                try:
                    server = Pyro4.Proxy(s)
                    space = server.space_available()
                    if space > size:
                        server.cp(path, tags, filename, size, client_uri, request_id)
                        done = True
                        break
                except:
                    continue

            if not done:
                if self.space_available > size:
                    self.cp(path, tags, filename, size, client_uri, request_id)

    @Pyro4.expose
    def cp(self, path, tags, filename, size, client_uri, request_id):
        if not self.storage.exists_file(tags, filename):
            print('Creando archivo tags: {0}, filename: {1}'.format(tags, filename))
            serial = self.get_mark()

            with self.servers_lock:
                connected_servers = []
                for s in self.servers:
                    try:
                        server = Pyro4.Proxy(s)
                        params = (tags, filename, size)
                        server.recieve_new_operations(serial, Command.cp.value, params)
                        connected_servers.append(s)
                    except:
                        continue

                    self.servers = connected_servers

            self.storage.create_new_entry(serial, tags, filename, '', size)
            address = self.storage.get_address(tags, filename)
            try:
                if not os.path.exists(address):
                    address = os.path.join(self.DIRECTORY, address)
                    fd = open(address, 'x')
                    fd.close()
            except:
                pass
            self.report_to_client(request_id, Command.cp, [self.server_uri], client_uri)
        else:
            self.report_to_client(request_id, Command.cp, [None], client_uri)

    @Pyro4.expose
    def fill_file(self, tags, filename, content, offset=0):
        if offset == -1:
            print('Se termino de copiar el archivo tags:{0}, filename:{1}'.format(tags, filename))
            print('Replicando... tags:{0}  filename:{1}'.format(tags, filename))
            self.replicate_files(tags, self.COPIES_NUMBER, filename)
            return None

        print('Copiando la parte:{0}, de tags:{1}, filename:{2}'.format(offset, tags, filename))
        address = self.storage.get_address(tags, filename)
        address = os.path.join(self.DIRECTORY, address)
        if os.path.exists(address):
            try:
                stat = os.stat(address)
                size = stat.st_size
                if stat.st_size <= offset:
                    fd = open(address, 'a+b')
                    fd.write(b64decode(content['data']))
                    fd.close()
            except:
                pass
        else:
            pass

    def get(self, tags, filename, client_uri, request_id):
        result = []

        if self.storage.exists_file(tags, filename):
            with self.servers_lock:
                connected_servers = []
                for s in self.servers:
                    try:
                        server = Pyro4.Proxy(s)
                        if server.i_have_this_file(tags, filename):
                            result.append(s)
                        connected_servers.append(s)
                    except:
                        continue

                    self.servers = connected_servers

        if self.i_have_this_file(tags, filename):
            result.append(self.server_uri)

        self.report_to_client(request_id, Command.get, result, client_uri)

    @Pyro4.expose
    def get_part(self, tags, filename, offset, part_size):
        address = self.storage.get_address(tags, filename)
        address = os.path.join(self.DIRECTORY, address)

        if os.path.exists(address):
            try:
                fd = open(address, 'a+b')
                fd.seek(offset)
                content = fd.read(part_size)
                fd.close()
                print('Enviando parte:{0}, de tags:{1}, flename:{2}'.format(offset, tags, filename))
                return True, content
            except:
                pass

        return False, None

    def ls(self, tags, client_uri, request_id, ):
        list = self.storage.get_files(tags)
        self.report_to_client(request_id, Command.ls, list, client_uri)

    def rm(self, tags, filename, client_uri, request_id):
        """
        :param tags: etiquetas asociadas al fichero separadas por el caracter /
        :param filename: nombre del fichero
        :return: 0
        """
        result = self.storage.exists_file(tags, filename)
        output = []

        if result:
            serial = self.get_mark()
            with self.servers_lock:
                connected_servers = []
                for s in self.servers:
                    try:
                        server = Pyro4.Proxy(s)
                        params = ('', tags, filename)
                        server.recieve_new_operations(serial, Command.rm.value, params)
                        connected_servers.append(s)
                    except:
                        continue

                    self.servers = connected_servers

            address = self.storage.get_address(tags, filename)
            if address is not None:
                address = os.path.join(self.DIRECTORY, address)
                if os.path.exists(address):
                    print('Eliminando tags:{0}, filename:{1}'.format(tags, filename))
                    try:
                        os.remove(address)
                    except:
                        pass

            self.storage.modify_status_and_serial(serial, tags, others.Status.remove.value, filename)

            self.report_to_client(request_id, Command.rm, [(tags, filename)], client_uri)
        else:
            self.report_to_client(request_id, Command.rm, [None], client_uri)

    def rmr(self, tags, client_uri, request_id):
        """
        :param tags: etiquetas asociadas al fichero separadas por el caracter /
        :param option: recursivo r
        :return: 0
        """
        serial = self.get_mark()
        files = self.storage.get_files(tags)
        if files:
            with self.servers_lock:
                connected_servers = []
                for s in self.servers:
                    try:
                        server = Pyro4.Proxy(s)
                        server.recieve_new_operations(serial, Command.rm.value, ('-r', tags))
                        connected_servers.append(s)
                    except:
                        pass

                    self.servers = connected_servers

        deleted_files = []
        for f in files:
            addresses = self.storage.get_addresses_set(tags, f)
            for address in addresses:
                address = os.path.join(self.DIRECTORY, address)
                if address is not '':
                    if os.path.exists(address):
                        try:
                            os.remove(address)
                            print('Eliminando tags:{0}, filename:{1}'.format(tags, os.path.basename(address)))
                            deleted_files.append(f)
                        except:
                            pass

        self.storage.modify_status_and_serial(serial, tags, others.Status.remove.value)

        self.report_to_client(request_id, Command.rm, deleted_files, client_uri)

    def info(self, tags, filename, client_uri, request_id):
        info = self.storage.info(tags, filename)

        if info is not None:
            self.report_to_client(request_id, Command.info, [info.name, info.tags, info.size, info.owner], client_uri)
        else:
            self.report_to_client(request_id, Command.info, [None], client_uri)


    @Pyro4.expose
    def exists_file(self, tags, filename):
        return storage.exists_file(tags, filename)

    @Pyro4.expose
    def i_have_this_file(self, tags, filename):
        return self.storage.get_address(tags, filename) is not None

    @Pyro4.expose
    def exec_cmd(self, request_id, command, params, client_uri, ):
        self.queue.put((request_id, command, params, client_uri,))

    def process_queue(self):

        while True:
            if not self.queue.empty() and self.up_to_date and self.process_queue_bool:
                request_id, command, params, client_uri = self.queue.get()
                command = Command[command]

                if command == Command.ls:
                    print('ejecutando ls...')
                    tags, = params
                    params = (tags, client_uri, request_id)
                    threading.Thread(target=self.ls, args=params).start()

                if command == Command.info:
                    print('ejecutando info...')
                    tags, filename = params
                    params = tags, filename, client_uri, request_id
                    threading.Thread(target=self.info, args=params).start()

                if command == Command.cp:
                    print('ejecutando cp...')
                    file_path, tags, filename, size = params
                    threading.Thread(target=self.cp,
                                     args=(file_path, tags, filename, size, client_uri, request_id)).start()

                if command == Command.rm:
                    print('ejecutando rm...')
                    if params[0] != '-r':
                        # rm SIN -r
                        tags, filename = params
                        self.rm(tags, filename, client_uri, request_id)
                    else:
                        # rm CON -r
                        option, tags = params
                        self.rmr(tags, client_uri, request_id)
                        pass

                if command == Command.get:
                    print('ejecutando get...')
                    tags, filename = params
                    threading.Thread(target=self.get, args=(tags, filename, client_uri, request_id)).start()

    def response_receive_uri(self):
        listener = socketutil.createBroadcastSocket(('', self.SERVERS_PORT))
        while True:
            try:
                data, address = listener.recvfrom(512)
                with self.main_server_lock:
                    if (data.decode() == 'get_uri_client' and self.im_main_server) or data.decode() == 'get_uri':
                        listener.sendto(self.server_uri.encode(), address)

                    elif self.im_main_server:
                        s = data.decode().split()
                        if s[0] == 'receive_uri':
                            with self.servers_lock:
                                if s[1] is not None and not self.servers.__contains__(s[1]) and self.server_uri != s[1]:
                                    self.servers.append(s[1])
            except:
                continue

    def search_others_servers(self):
        # while True:
        scanner = socketutil.createBroadcastSocket()
        scanner.settimeout(self.UPDATE_SERVERS_TIMEOUT)

        new_servers = []
        try:
            scanner.sendto(b'get_uri', ('255.255.255.255', self.SERVERS_PORT))
        except:
            print('Error al hacer broadcast')

        while True:
            try:
                data, address = scanner.recvfrom(512)
                if data.decode() != self.server_uri:
                    new_servers.append(data.decode())
            except:
                break

        with self.servers_lock:
            self.servers = new_servers

        time.sleep(self.UPDATE_SERVERS_TIME)

    def __is_disconnected_loop(self):
        while True:
            if not self.localhost:
                try:
                    self.ip = others.get_ip()
                    if self.ip == '127.0.0.1':
                        self.disconnected = True
                        self.up_to_date = False
                        self.queue = queue.Queue()
                        with self.main_server_lock:
                            self.im_main_server = False
                            # aryan
                            if not self.last_time_conected:
                                self.last_time_conected = 5
                                # aryan end


                    elif self.disconnected:
                        # mandar a todos los otros servers mi uri
                        scanner = socketutil.createBroadcastSocket()
                        scanner.settimeout(self.UPDATE_SERVERS_TIMEOUT)

                        try:
                            s = 'receive_uri {0}'.format(self.server_uri)
                            scanner.sendto(s.encode(), ('255.255.255.255', self.SERVERS_PORT))
                        except:
                            print('Error al hacer broadcast')

                        self.disconnected = False
                        print('Conectado')

                        # aryan
                        self.last_time_conected = None
                        print("> Eliminando replicas inconclusas")
                        with self.pending_replic_lock:
                            for corrupted_file in self.pending_replic:
                                os.remove(os.path.join(self.DIRECTORY,
                                                       self.storage.get_address(corrupted_file[1], corrupted_file[0])))
                                self.storage.update_address(corrupted_file[1], corrupted_file[0])
                        print("> Todo eliminado")
                        print(self.pending_replic)
                        # aryan end

                        self.__update()
                    elif not self.up_to_date:
                        self.__update()

                    time.sleep(self.IS_DISCONNECTED_TIME_LOOP)
                except:
                    continue
            else:
                self.up_to_date = True
                time.sleep(self.IS_DISCONNECTED_TIME_LOOP)

    def __update(self):

        if not self.im_main_server and self.main_server_uri is not None:
            main = Pyro4.Proxy(self.main_server_uri)
            last_serial = self.storage.last_serial()
            if last_serial is None:
                last_serial = 0
            try:
                entries = main.latest_op(last_serial)
            except:
                pass
            if entries is not None:
                for e in entries:
                    tags = e['tags']
                    filename = e['name']
                    my_entry = self.storage.get_file(tags, filename)
                    if my_entry:
                        # la entrada existe en mi base de datos lo que tengo que hacer es actualizar
                        self.storage.modify_status_and_serial(e['serial'], tags, e['status'], filename)
                        if e['status'] == others.Status.remove.value:
                            address = self.storage.get_address(tags, filename)
                            if address is not None:
                                address = os.path.join(self.DIRECTORY, address)
                                if os.path.exists(address):
                                    try:
                                        os.remove(address)
                                    except:
                                        pass

                    else:
                        # la entrada hay que crearla
                        self.storage.create_new_entry_without_address(e['serial'], tags, filename, e['owner'],
                                                                      e['size'])

            if not self.disconnected:
                last_serial = self.storage.last_serial()
                if last_serial is not None:
                    self.last_id = last_serial
                self.up_to_date = True

    @Pyro4.expose
    def get_last_serial(self):
        while True:
            if self.not_saved_operations.empty():
                return self.storage.last_serial()
            with self.founded_lock:
                if self.main_server_founded:
                    return None

    @Pyro4.expose
    def recieve_new_operations(self, serial, command, params):
        self.not_saved_operations.put((serial, command, params))

    # def save_new_operations(self):
    #     while True:
    #         if self.up_to_date and not self.not_saved_operations.empty():
    #             serial, command, params = self.not_saved_operations.get()
    #
    #             command = Command[command]
    #             if command is command.cp:
    #                 tags, filename, size = params
    #                 if self.storage.exists_file(tags, filename):
    #                     self.storage.remove_one(tags, filename)
    #                 self.storage.create_new_entry_without_address(serial, tags, filename, '', size)
    #
    #             if command is command.rm:
    #                 option, tags, filename = params
    #                 if self.storage.exists_file(tags, filename):
    #                     if option == '':
    #                         self.storage.modify_status_and_serial(serial, tags, others.Status.remove.value, filename)
    #                     else:
    #                         self.storage.modify_status_and_serial(serial, tags, others.Status.remove.value)
    #
    #                     address = self.storage.get_address(tags, filename)
    #                     if address is not '':
    #                         address = os.path.join(self.DIRECTORY, address)
    #                         if os.path.exists(address):
    #                             try:
    #                                 os.remove(address)
    #                             except:
    #                                 pass
    def save_new_operations(self):
        while True:
            if self.up_to_date and not self.not_saved_operations.empty():
                serial, command, params = self.not_saved_operations.get()

                command = Command[command]
                if command is command.cp:
                    tags, filename, size = params
                    if self.storage.exists_file(tags, filename):
                        self.storage.remove_one(tags, filename)

                    with self.id_lock:
                        if self.last_id < serial:
                            self.last_id = serial

                    self.storage.create_new_entry_without_address(serial, tags, filename, '', size)

                if command is command.rm:
                    option, tags, filename = params
                    if option == '':
                        # SIN -r
                        if self.storage.exists_file(tags, filename):
                            with self.id_lock:
                                if self.last_id < serial:
                                    self.last_id = serial

                            self.storage.modify_status_and_serial(serial, tags, others.Status.remove.value, filename)
                            address = self.storage.get_address(tags, filename)
                            if address is not '' and address is not None:
                                address = os.path.join(self.DIRECTORY, address)
                                if os.path.exists(address):
                                    try:
                                        os.remove(address)
                                    except:
                                        pass

                    else:
                        # CON -r
                        addresses = self.storage.get_addresses_set(tags)
                        for address in addresses:
                            if address is not '' and address is not None:
                                address = os.path.join(self.DIRECTORY, address)
                                if os.path.exists(address):
                                    try:
                                        os.remove(address)
                                    except:
                                        pass
                        if self.id_lock:
                            if self.last_id < serial:
                                self.last_id = serial
                        self.storage.modify_status_and_serial(serial, tags, others.Status.remove.value)

    def get_mark(self):
        with self.id_lock:
            self.last_id = self.last_id + 1

        return self.last_id

    def check_im_main_server(self):
        self.search_others_servers()
        self.process_queue_bool = False
        im = False
        if len(self.servers) == 0:
            self.im_main_server = True
            self.main_server_uri = self.server_uri
            im = True
        elif self.up_to_date:
            max_serial = self.get_last_serial()
            im = True
            for s in self.server_uri:
                try:
                    server = Pyro4.Proxy(s)
                    serial = server.get_last_serial()
                    if serial > max_serial:
                        im = False
                        break

                    if serial == max_serial:
                        if self.server_uri > s:
                            im = False
                except:
                    continue

        if im:
            with self.main_server_lock:
                self.im_main_server = True
                self.main_server_uri = self.server_uri

            time.sleep(self.UPDATE_SERVERS_TIME)

            # para evitar incoherencias si existen mas de un servidor principal
            b, uri = self.check_main_server_is_alive()
            if b and uri < self.main_server_uri:
                self.im_main_server = False
                self.main_server_uri = uri

            self.process_queue_bool = True
            self.up_to_date = True

            self.search_others_servers()

    def check_main_server_is_alive(self):
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
                if data.decode() != self.server_uri:
                    return True, data.decode()
            except:
                break

        return False, None

    def main_server_loop(self):
        while True:
            if not self.disconnected:
                with self.main_server_lock:
                    im_main_server = self.im_main_server

                if not self.im_main_server:
                    b, uri = self.check_main_server_is_alive()

                    if not b:
                        b, uri = self.check_main_server_is_alive()

                    if not b:
                        self.check_im_main_server()
                    else:
                        self.main_server_uri = uri
                        self.main_server_founded = b
                else:
                    self.search_others_servers()

            time.sleep(self.UPDATE_SERVERS_TIME)

    @Pyro4.expose
    def replicate_files(self, tags, n, filename=None):
        if filename is None:
            ls = self.storage.get_files(tags)
            for filename in ls:
                index = 0
                size = self.storage.get_file(tags, filename).size
                servers = self.get_servers_with_space_available(size)

                for server_uri in servers:
                    if index == n:
                        break
                    self.replicate_single_file_other_server(tags, filename, server_uri)
                    index += 1
        else:
            index = 0
            size = self.storage.get_file(tags, filename).size
            servers = self.get_servers_with_space_available(size)
            for server_uri in servers:
                if index == n:
                    break
                self.replicate_single_file_other_server(tags, filename, server_uri)
                index += 1

    @Pyro4.expose
    def replicate_single_file_other_server(self, tags, filename, server_uri):
        server = Pyro4.Proxy(server_uri)
        path = self.storage.get_address(tags, filename)
        size = self.storage.get_file(tags, filename).size
        try:
            server.cp_replicate(path, tags, filename, size, self.server_uri)
        except:
            pass

    @Pyro4.expose
    def get_servers_with_space_available(self, space):
        ls = []
        for server_uri in self.servers:
            if server_uri != self.server_uri:

                server = Pyro4.Proxy(server_uri)
                if server.space_available() >= space:
                    ls.append(server_uri)
        return ls

    @Pyro4.expose
    def cp_replicate(self, serial, tags, filename, size, server_uri):

        if self.space_available() >= size:
            print(">>> copiando")
            with self.pending_replic_lock:
                self.pending_replic.append((filename, tags))
                print("> Agregando a la lista: " + filename)
                print(self.pending_replic)
            result = self.storage.exists_file(tags, filename)

            if not result:
                server = Pyro4.Proxy(server_uri)
                self.storage.create_new_entry(serial, tags, filename, '', size)
            else:
                # actualizar la entrada en la BD
                server = Pyro4.Proxy(server_uri)
                self.storage.update_address_serial(tags, filename, serial)
            try:
                old_address = self.storage.get_address(tags, filename)
                address = os.path.join(self.DIRECTORY, old_address)

                if not os.path.exists(address):
                    fd = open(address, 'x')
                    fd.close()
                server.server_cp(self.server_uri, tags, filename)
            except:
                pass

    @Pyro4.expose
    def server_cp(self, server_uri, tags, filename, offset=0, attempt=0):
        # copiar el archivo por partes

        address = self.storage.get_address(tags, filename)
        address = os.path.join(self.DIRECTORY, address)

        while attempt < self.MAX_ATTEMPT_NUMBER:
            if os.path.exists(address):
                try:
                    fd = open(address, 'a+b')
                    fd.seek(offset)
                    content = fd.read(self.FILE_PART_SIZE)
                    fd.close()
                except:
                    print(Strings.FILE_LOST_SUDDENLY).format(address)
                    break

                if len(content) > 0:
                    try:
                        server = Pyro4.Proxy(server_uri)
                        server.fill_file_replication(tags, filename, content, offset)
                        print('Mandando a replicar tags:{0},filename:{1}, offset:{2}'.format(tags, filename, offset))
                        offset += len(content)
                    except:
                        time.sleep(self.WAIT_AND_TRY_AGAIN)
                        print(Strings.TRYING_AGAIN.format('replicar', address))
                        attempt += 1
                        continue

                else:
                    try:
                        server = Pyro4.Proxy(server_uri)
                        server.fill_file_replication(tags, filename, content, offset=-1)
                        print('Terminando de Replicar tags:{0}, filename:{1}'.format(tags, filename))
                        break
                    except:
                        time.sleep(self.WAIT_AND_TRY_AGAIN)
                        print(Strings.TRYING_AGAIN.format('replicar', address))
                        attempt += 1
                        continue

    @Pyro4.expose
    def fill_file_replication(self, tags, filename, content, offset=0):
        print('Llenando replicacion... tags:{0}   filename{1}  offset:{2}'.format(tags, filename, offset))
        if offset == -1:
            with self.pending_replic_lock:
                self.pending_replic.remove((filename, tags))
                print("> Quitando de la lista")
                print(self.pending_replic)

            pass
        else:
            address = self.storage.get_address(tags, filename)
            address = os.path.join(self.DIRECTORY, address)
            if os.path.exists(address):
                try:
                    size = os.stat(address).st_size
                    if size <= offset:
                        fd = open(address, 'a+b')
                        fd.seek(offset)
                        fd.write(b64decode(content['data']))
                        fd.close()
                except:
                    pass
            else:
                pass

    @Pyro4.expose
    def space_available(self):
        return shutil.disk_usage(self.DIRECTORY)[2]

    @Pyro4.expose
    def latest_op(self, serial):
        return self.storage.serial_filter(serial)


if __name__ == '__main__':
    server = Server()
    daemon = Pyro4.Daemon(host=others.get_ip())

    server.server_uri = str(daemon.register(server))

    print(server.get_server_uri)

    daemon.requestLoop()

