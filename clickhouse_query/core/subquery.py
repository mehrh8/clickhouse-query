"""
Subquery functionality for ClickHouse queries.
"""

from clickhouse_query import mixins, utils


class Subquery(mixins.ASMixin):
    """
    Represents a subquery in SQL queries.

    A subquery allows you to nest one query inside another.
    """

    def __init__(self, inner_queryset):
        """
        Initialize a subquery with an inner queryset.

        Args:
            inner_queryset: The QuerySet to use as a subquery
        """
        self._inner_queryset = inner_queryset

    def __sql__(self, *, uid_generator):
        """
        Generate SQL for the subquery.

        Args:
            uid_generator: Generator for parameter UIDs

        Returns:
            Tuple of (sql_string, params_dict)
        """
        inner_queryset_sql, sql_params = utils.get_sql(self._inner_queryset, uid_generator=uid_generator)
        sql = "({inner_queryset})".format(inner_queryset=inner_queryset_sql)
        return sql, sql_params


__all__ = ["Subquery"]
