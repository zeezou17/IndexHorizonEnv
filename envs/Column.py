class Column:

    def __init__(self, name: str, datatype: str, size: int):
        self._name = name
        self._datatype = datatype
        self._size = size

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, column_name: str):
        self._name = column_name

    @property
    def datatype(self):
        return self._datatype

    @datatype.setter
    def datatype(self, data_type: str):
        self._datatype = data_type

    @property
    def size(self):
        return self._size

    @size.setter
    def size(self, size_of_data: int):
        self._size = size_of_data

    def __str__(self):
        return "[ name :" + self._name + ", datatype : " + self._datatype + ", size( in bytes) : " + str(
            self._size) + " ]"
