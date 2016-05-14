#-*- coding: utf-8 -*-


class C(object):

    def _get_target(self, target, field=None):
        return "{}.{}".format(target, field) \
            if field is not None else target

    def __init__(self, target, field=None):
        self._first = self._get_target(target, field)

    def LT(self, target, field=None):
        second = self._get_target(target, field)
        return "{}<{}".format(self._first, second)

    def GT(self, target, field=None):
        second = self._get_target(target, field)
        return "{}>{}".format(self._first, second)

    def LTE(self, target, field=None):
        second = self._get_target(target, field)
        return "{}<={}".format(self._first, second)

    def GTE(self, target, field=None):
        second = self._get_target(target, field)
        return "{}>={}".format(self._first, second)

    def EQUAL(self, target, field=None):
        second = self._get_target(target, field)
        return "{}={}".format(self._first, second)

    def IN(self, target, field=None):
        second = self._get_target(target, field)
        return "{} IN {}".format(self._first, second)

    def TS(self, target, field=None):
        second = self._get_target(target, field)
        return "to_tsvector({}) @@ to_tsquery({})".format(
            self._first, second)

    def LIKE(self, target, field=None):
        second = self._get_target(target, field)
        return "{} LIKE {}".format(
            self._first, second)

    def IS_NULL(self):
        return "{} is null".format(self._first)

    def IS_NOT_NULL(self):
        return "{} is not null".format(self._first)


def AND(*conditions):
    return " AND ".join(conditions)


def OR(*conditions):
    return " OR ".join(conditions)


def ARRAY_AGG(target, field, alias=None):
    alias = alias or field
    return "array_agg({}.{}) AS {}".format(
        target, field, alias)


def AVG(target, field, alias=None):
    alias = alias or field
    return "avg({}.{}) AS {}".format(
        target, field, alias)


def COUNT(target, field, alias=None):
    alias = alias or field
    return "count({}.{}) AS {}".format(
        target, field, alias)


def MAX(target, field, alias=None):
    alias = alias or field
    return "max({}.{}) AS {}".format(
        target, field, alias)


def MIN(target, field, alias=None):
    alias = alias or field
    return "min({}.{}) AS {}".format(
        target, field, alias)


def SUM(target, field, alias=None):
    alias = alias or field
    return "sum({}.{}) AS {}".format(
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

    def FROM(self, schema, table_name):
        self._from = "FROM {}.{} AS {}".format(
            schema, table_name, table_name)

        return self

    def LEFT_JOIN(self, schema, table_name, *conditions):
        conditions = " AND ".join(conditions)
        self._joins.append("LEFT JOIN {}.{} AS {} ON {}".format(
            schema, table_name, table_name, conditions))

        return self

    def RIGHT_JOIN(self, schema, table_name, *conditions):
        conditions = " AND ".join(conditions)
        self._joins.append("RIGHT JOIN {}.{} AS {} ON {}".format(
            schema, table_name, table_name, conditions))

        return self

    def CROSS_JOIN(self, schema, table_name, *conditions):
        conditions = " AND ".join(conditions)
        self._joins.append("CROSS JOIN {}.{} AS {} ON {}".format(
            schema, table_name, table_name, conditions))

        return self

    def WHERE(self, *conditions):
        self._conditions.append(" AND ".join(conditions))

        return self

    def GROUP_BY(self, *fields):
        self._group_by = ",".join(fields)

        return self

    def ORDER_BY(self, *fields):
        self._order_by = ",".join(fields)

        return self

    def LIMIT(self, limit):
        self._limit = limit

        return self

    def OFFSET(self, offset):
        self._offset = offset

        return self

    def __str__(self):
        if self._from is not None:
            sql_seq = ["SELECT"]
            
            if self._fields:
                fields = []
                for item in self._fields:
                    if isinstance(item, tuple) and len(item) == 2:
                        fields.append(
                            "{}.{} AS {}".format(item[0], item[1], item[1]))

                    elif isinstance(item, tuple) and len(item) == 3:
                        fields.append(
                            "{}.{} AS {}".format(item[0], item[1], item[2]))

                    else:
                        fields.append(item)

                sql_seq.append(", ".join(fields))

            else:
                sql_seq.append("*")

            sql_seq.append(self._from)

            sql_seq.extend(self._joins)

            if self._conditions:
                sql_seq.append(
                    "WHERE {}".format(" AND ".join(self._conditions)))

            if self._group_by is not None:
                sql_seq.append(
                    "GROUP BY {}".format(self._group_by))

            if self._order_by is not None:
                sql_seq.append(
                    "ORDER BY {}".format(self._order_by))

            if self._offset is not None:
                sql_seq.append(
                    "OFFSET {}".format(self._offset))

            if self._limit is not None:
                sql_seq.append(
                    "LIMIT {}".format(self._limit))

            return " ".join(sql_seq)

        else:
            raise Exception()
