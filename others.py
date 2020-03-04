def format_ls_list(list):
    if len(list) > 0:
        list.sort()

        distinc_list = [list[0]]

        for i in range(1, list.__len__()):
            if list[i] is not list[i - 1]:
                distinc_list.append(list[i])

    return list


# ---------***---------***---------***---------***---------***---------***

import os
import socket


def get_ip():
    if os.name == 'nt':
        return socket.gethostbyname(socket.gethostname())

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # doesn't even have to be reachable
        s.connect(('10.255.255.255', 0))
        IP = s.getsockname()[0]
    except:
        IP = '127.0.0.1'
    finally:
        s.close()
    return IP


# ---------***---------***---------***---------***---------***---------***


# def get_free_space(path):
#     # Unix
#     s = os.statvfs(path)
#     return s.f_bsize * s.f_bavail


# ---------***---------***---------***---------***---------***---------***

from enum import Enum


class Strings:
    UNREACHEABLE_SERVER_ERROR = 'servidor {0} inalcanzable'
    FILE_NOT_FOUND = 'no se encuentra el archivo {0}'
    FILE_LOST_SUDDENLY = 'el archivo {0} ha sido eliminado inesperadamente'
    AVAILABLE_SERVER_NOT_FOUND = 'no se encuentra un servidor disponible para {0} {1}'
    SUCCESFUL_OPERATION = 'la operacion {0} {1} ha sido completada con exito'
    TRYING_AGAIN = 'intentando nuevamente {0} {1}'
    OPERATION_FAIL = 'no se pudo realizar la operacion {0} {1}.'
    START_OPERATION = 'comenzo la operacion {0} con el server {1}'
    FILE_ALREADY_EXISTS = 'ya existen el file {0}'


# ---------***---------***---------***---------***---------***---------***
class Status(Enum):
    exists = 0
    remove = 1
