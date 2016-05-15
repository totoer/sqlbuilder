#-*- coding: utf-8 -*-


class C(object):

    def _get_target(self, target, field=None):
        return u"{}.{}".format(target, field) \
            if field is not None else target

    def __init__(self, target, field=None):
        self._first = self._get_target(target, field)

    def LT(self, target, field=None):
        second = self._get_target(target, field)
        return u"{}<{}".format(self._first, second)

    def GT(self, target, field=None):
        second = self._get_target(target, field)
        return u"{}>{}".format(self._first, second)

    def LTE(self, target, field=None):
        second = self._get_target(target, field)
        return u"{}<={}".format(self._first, second)

    def GTE(self, target, field=None):
        second = self._get_target(target, field)
        return u"{}>={}".format(self._first, second)

    def EQUAL(self, target, field=None):
        second = self._get_target(target, field)
        return u"{}={}".format(self._first, second)

    def IN(self, target, field=None):
        second = self._get_target(target, field)
        return u"{} IN {}".format(self._first, second)

    def TS(self, target, field=None):
        second = self._get_target(target, field)
        return u"to_tsvector({}) @@ to_tsquery({})".format(
            self._first, second)

    def LIKE(self, target, field=None):
        second = self._get_target(target, field)
        return u"{} LIKE {}".format(
            self._first, second)

    def ILIKE(self, target, field=None):
        second = self._get_target(target, field)
        return u"{} ILIKE {}".format(
            self._first, second)

    def IS_NULL(self):
        return u"{} is null".format(self._first)

    def IS_NOT_NULL(self):
        return u"{} is not null".format(self._first)


def AND(*conditions):
    return u" AND ".join(conditions)


def OR(*conditions):
    return u" OR ".join(conditions)


def ARRAY_AGG(target, field, alias=None):
    alias = alias or field
    return u"array_agg({}.{}) AS {}".format(
        target, field, alias)


def AVG(target, field, alias=None):
    alias = alias or field
    return u"avg({}.{}) AS {}".format(
        target, field, alias)


def COUNT(target, field, alias=None):
    alias = alias or field
    return u"count({}.{}) AS {}".format(
        target, field, alias)


def MAX(target, field, alias=None):
    alias = alias or field
    return u"max({}.{}) AS {}".format(
        target, field, alias)


def MIN(target, field, alias=None):
    alias = alias or field
    return u"min({}.{}) AS {}".format(
        target, field, alias)


def SUM(target, field, alias=None):
    alias = alias or field
    return u"sum({}.{}) AS {}".format(
        target, field, alias)


class SELECT(object):

    def __init__(self, *fields):
        self._fields = fields
        self._from = None
        self._joins = []
        self._conditions = []
        self._order_by = None
        self._group_by = None
        self._limit = None
        self._offset = None

    def WITH(self, *selects):
        pass

    def WITH_RECURSIVE(self, ):
        pass

    def FROM(self, schema, table_name, alias=None):
        alias = alias or table_name
        self._from = u"FROM {}.{} AS {}".format(
            schema, table_name, alias)

        return self

    def LEFT_JOIN(self, schema, table_name, *conditions):
        conditions = u" AND ".join(conditions)
        self._joins.append(u"LEFT JOIN {}.{} AS {} ON {}".format(
            schema, table_name, table_name, conditions))

        return self

    def RIGHT_JOIN(self, schema, table_name, *conditions):
        conditions = u" AND ".join(conditions)
        self._joins.append(u"RIGHT JOIN {}.{} AS {} ON {}".format(
            schema, table_name, table_name, conditions))

        return self

    def CROSS_JOIN(self, schema, table_name, *conditions):
        conditions = u" AND ".join(conditions)
        self._joins.append(u"CROSS JOIN {}.{} AS {} ON {}".format(
            schema, table_name, table_name, conditions))

        return self

    def WHERE(self, *conditions):
        self._conditions.append(u" AND ".join(conditions))

        return self

    def GROUP_BY(self, *fields):
        self._group_by = u",".join(fields)

        return self

    def ORDER_BY(self, *fields):
        self._order_by = u",".join(fields)

        return self

    def LIMIT(self, limit):
        self._limit = limit

        return self

    def OFFSET(self, offset):
        self._offset = offset

        return self

    def __str__(self):
        if self._from is not None:
            sql_seq = [u"SELECT"]
            
            if self._fields:
                fields = []
                for item in self._fields:
                    if isinstance(item, tuple) and len(item) == 2:
                        fields.append(
                            u"{}.{} AS {}".format(item[0], item[1], item[1]))

                    elif isinstance(item, tuple) and len(item) == 3:
                        fields.append(
                            u"{}.{} AS {}".format(item[0], item[1], item[2]))

                    else:
                        fields.append(item)

                sql_seq.append(u", ".join(fields))

            else:
                sql_seq.append(u"*")

            sql_seq.append(self._from)

            sql_seq.extend(self._joins)

            if self._conditions:
                sql_seq.append(
                    u"WHERE {}".format(" AND ".join(self._conditions)))

            if self._group_by is not None:
                sql_seq.append(
                    u"GROUP BY {}".format(self._group_by))

            if self._order_by is not None:
                sql_seq.append(
                    u"ORDER BY {}".format(self._order_by))

            if self._offset is not None:
                sql_seq.append(
                    u"OFFSET {}".format(self._offset))

            if self._limit is not None:
                sql_seq.append(
                    u"LIMIT {}".format(self._limit))

            return u" ".join(sql_seq)

        else:
            raise Exception()

    @property
    def sql(self):
        return self.__str__()
