# ClickHouse Query
**!!! This repository is not complete, so the following codes may not work in current version. !!!**

Clickhouse Query is a Python library for building complex ClickHouse queries by a fluent API and a set of functions.

## Installation

```bash
pip install clickhouse-query
```

## Usage

**!!! This repository is not complete, so the following codes may not work in current version. !!!**

```python
import clickhouse_query as ch

q = (
    ch.QuerySet()
    .from_("my_table")
    .prewhere(date__year__gte="2024")
    .select(count=ch.Count())
    .limit(10)
)

sql, sql_params  = ch.get_sql(q)
# sql = 'SELECT count() AS count FROM click PREWHERE (greaterOrEquals(toYear(date), %(__U_1)s)) LIMIT %(__U_2)f'
# sql_params = {'__U_1': 2024, '__U_2': 10}

data = q.execute()
# data = [{'count': 100}]
```

## Custom Functions

If there is not a function that you need, you can easily create it by extending the `Function` class. like this:

```python
class MyFunction(ch.Function):
    function = "my_function"
```

## Custom Aggregation Functions

If there is not a aggregation function that you need, you can easily create it by extending the `AggregationFunction` class. like this:

```python
class MyAggregationFunction(ch.AggregationFunction):
    function = "my_aggregation_function"
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details
