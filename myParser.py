from enum import Enum


class Command(Enum):
    ls = 'ls'
    cp = 'cp'
    info = 'info'
    rm = 'rm'
    get = 'get'
    fill = 'fill'
    get_part = 'get_part'


class Parser:
    def __init__(self):
        pass

    @staticmethod
    def __remove_white_spaces(string):
        new_string = ''
        for char in string:
            if char != ' ':
                new_string += char
        return string

    @staticmethod
    def __get_param(string, pos):
        param = ''
        start_param = False

        for i in range(pos, len(string)):
            if string[i] != ' ':
                param += string[i]
                start_param = True
            elif start_param:
                break

            i += 1

        if param == '':
            return None, None

        return param, i

    @staticmethod
    def __split(string, char):  # diferencia con el split estandar: no agrega strings vacios
        list = []
        current = ''

        for s in string:
            if s == char and current != '':
                list.append(current)
                current = ''
            else:
                current += s

        if current != '':
            list.append(current)

        return list

    @staticmethod
    def __identify_command(string):
        for i in Command:
            if i.value == string:
                return True, i

        return False, None

    def __check_tags_filename(self, params):
        if len(params) == 1:
            tags_filename_list = self.__split(params[0], '/')
            if len(tags_filename_list) >= 1:
                filename = tags_filename_list[len(tags_filename_list) - 1]
                tags_filename_list.pop(len(tags_filename_list) - 1)

            return tags_filename_list, filename

        return None

    def __check_params_ls(self, params):  # ls <tags>
        if len(params) == 1:
            param_list = self.__split(params[0], '/')
            # se aceptan los tags vacios
            return (param_list,)

        return None

    def __check_params_cp(self, params):  # cp <file_path> <tags>/<file_name>
        if len(params) == 2:
            file_path, tags_filename = params

            tags_filename_list, filename = self.__check_tags_filename([params[1]])
            #
            # tags_filename_list = self.__split(tags_filename, '/')
            # if len(tags_filename_list) >= 1:
            #     filename = tags_filename_list[len(tags_filename_list) - 1]
            #     tags_filename_list.pop(len(tags_filename_list) - 1)

            return file_path, tags_filename_list, filename

        return None

    def __check_params_info(self, params):  # info <tags>/<file_name>
        return self.__check_tags_filename(params)

    def __check_params_rm(self, params):  # rm <tags>/<file_name> • rm –r <tags>
        if len(params) == 1:
            return self.__check_tags_filename(params)

        if len(params) == 2 and params[0] == '-r':
            params = self.__split(params[1], '/')
            return params

    def __check_params_get(self, params):  # get <tags>/<file_name>
        return self.__check_tags_filename(params)

    def parse(self, string):
        command, pos = self.__get_param(string, 0)
        pos += 1
        result, command = self.__identify_command(command)
        if result is False:
            return None, None

        params = []

        if command is not command.cp and command is not command.rm:
            params.append(string[pos:])

            if command is command.ls:
                params = self.__check_params_ls(params)

            if command is command.get:
                params = self.__check_params_get(params)

            if command is command.info:
                params = self.__check_params_info(params)

        elif command is command.cp:
            p, pos = self.__get_param(string, pos)
            while p is not None:
                params.append(p)
                p, pos = self.__get_param(string, pos)

            if command is command.cp:
                params = self.__check_params_cp(params)

        elif command is command.rm:
            p, new_pos = self.__get_param(string, pos)
            if p == '-r':
                params.append('-r')
                new_pos += 1
                params.append(string[new_pos:])
                params = ('-r', self.__check_params_rm(params))
            else:
                if command is command.rm:
                    params.append(string[pos:])
                    params = self.__check_params_rm(params)

        if params is None:
            return None, None

        return command, params
