import _sha1
import datetime

from pymongo import MongoClient

from file import File
from others import Status


class Storage:
    def __init__(self):
        client = MongoClient('localhost', 27017)
        self.database = client.database
        self.database.drop_collection('collection')
        self.collection = self.database.collection


    @staticmethod
    def _hash(tags, filename):
        sep = '/'
        l = sorted(tags)
        s = ""
        for t in l:
            s += t + sep
        s += filename
        return _sha1.sha1(s.encode()).hexdigest()

    def exists_file(self, tags, filename):
        f = {'tags': tags, 'name': filename, 'status': Status.exists.value}
        files = self.collection.find_one(f)
        return files is not None

    def create_new_entry(self, serial, tags, filename, owner, size):
        if not self.exists_file(tags, filename):
            f = {'serial': serial, 'hash_key': self._hash(tags, filename), 'name': filename, 'tags': tags,
                 'owner': owner,
                 'size': size, 'm_date': datetime.datetime.now().__str__(), 'status': Status.exists.value}
            self.collection.insert_one(f)
            return f

    def create_new_entry_without_address(self, serial, tags, filename, owner, size):
        if not self.exists_file(tags, filename):
            f = {'serial': serial, 'hash_key': '', 'name': filename, 'tags': tags, 'owner': owner,
                 'size': size, 'm_date': datetime.datetime.now().__str__(), 'status': Status.exists.value}
            self.collection.insert_one(f)
            return f

    def get_files(self, tags):
        """
        :param tags:
        :return: the name of each file
        """
        files = self.collection.find({'tags': {'$all': tags}})
        if files is not None:
            return [f['name'] for f in files]
        return []

    def get_file(self, tags, filename):
        f = self.collection.find_one({'hash_key': self._hash(tags, filename)})
        if f is not None:
            file = File(f['name'], f['tags'], f['size'], f['owner'], f['m_date'])
            return file

    def info(self, tags, filename):
        f = self.get_file(tags, filename)
        if f is not None:
            return f

    def get_address(self, tags, filename):
        if filename != '':
            result = self.collection.find_one({'tags': tags, 'name': filename})
            if result is not None:
                return result['hash_key']

    def get_addresses_set(self, tags, filename=''):
        if filename != '':
            files = self.collection.find({'tags': {'$all': tags}, 'name': filename})
            if files is not None:
                return [f['hash_key'] for f in files]
        else:
            files = self.collection.find({'tags': {'$all': tags}})
            if files is not None:
                return [f['hash_key'] for f in files]
        return []

    def remove_one(self, tags, filename):
        return self.collection.delete_one({'name': filename, 'tags': {'$all': tags}}).deleted_count

    def remove_many(self, tags):
        return self.collection.delete_many({'tags': {'$all': tags}}).deleted_count


    def modify_status_and_serial(self, serial, tags, status, filename='', ):
        """
            :param serial:
            :param tags: etiquetas asociadas al archivo
            :param status:
            :param filename: nombre del archivo
            :return: cantidad de archivos que han sido modificados
            """
        if filename is '':
            return self.collection.update_many({'tags': {'$all': tags}},
                                               {'$set': {'serial': serial, 'status': status}}).modified_count
        else:
            return self.collection.update_one({'tags': {'$all': tags}, 'name': filename},
                                              {'$set': {'serial': serial, 'status': status}}).modified_count

    def last_serial(self):
        """
        :return: archivo con mayor serial
        """
        files = self.collection.find().sort('serial', -1)
        if files is not None:
            for f in files:
                return f['serial']

    def serial_filter(self, serial):
        """
        :param serial: marca en el tiempo del archivo
        :return: lista con los archivos que tienen el
        serial mayor que el que se pasa por parametro
        """
        return [f for f in self.collection.find({'serial': {'$gte': serial}})]

    def update_address_serial(self, tags, filename, serial):
        address = self._hash(tags, filename)
        self.collection.update_one({'tags': {'$all': tags}, 'name': filename},
                                   {'$set': {'serial': serial, 'hash_key': address}})


    def update_address(self, tags, filename):
        address = self._hash(tags, filename)
        self.collection.update_one({'tags': {'$all': tags}, 'name': filename},
                                   {'$set': {'hash_key': address}})