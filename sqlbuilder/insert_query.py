#-*- coding: utf-8 -*-


class INSERT(object):

    def __init__(self, schema, target):
        self._sql = u"INSERT INTO {}.{}".format(
            schema, target)

    def VALUES(self, fields):
        values = ", ".join(["%(" + field + ")s" for field in fields])
        self._sql += "({}) values({})".format(", ".join(fields), values)
        self._sql += " returning id"

    def __str__(self):
        return self._sql

    @property
    def sql(self):
        return self.__str__()
