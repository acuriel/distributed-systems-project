class File:
    def __init__(self, name, tags, size=-1, owner='', m_date=-1, addr=''):
        self.__name = name
        self.__tags = tags
        self.__size = size
        self.__owner = owner
        self.__m_date = m_date
        self.__addr = addr

    @property
    def name(self):
        return self.__name

    @property
    def tags(self):
        return self.__tags

    @property
    def size(self):
        return self.__size

    @property
    def owner(self):
        return self.__owner

    @property
    def modification_date(self):
        return self.__m_date

    @property
    def address(self):
        return self.__addr
