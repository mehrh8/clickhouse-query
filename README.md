# ClickHouse Query

**ClickHouse Query** is a Python library designed to simplify the construction of complex ClickHouse queries. It provides a fluent API and a comprehensive set of functions for this purpose.


## Installation

You can install the ClickHouse Query library using pip. Run the following command in your terminal:


```bash
pip install clickhouse-query
```

## Usage

Here’s an example of how to use the ClickHouse Query library:

```python
import clickhouse_query as ch
from clickhouse_query import functions as chf

# Create a QuerySet instance for your table
q = (
    ch.QuerySet("my_table", clickhouse_client=...)
    .prewhere(date__year__gte=2020)  # Add a prewhere condition
    .group_by("status")  # Group by status
    .select("status", s=chf.sum("price"))  # Select status and sum
    .order_by("-s")  # Order by sum in descending order
    .limit(10)  # Limit the results to 10
)

# Get the SQL query and parameters
sql, sql_params  = ch.get_sql(q)
# sql = 'SELECT status, sum(price) AS cnt FROM my_table PREWHERE (greaterOrEquals(toYear(date), %(__U_1)f)) GROUP BY status ORDER BY cnt DESC LIMIT %(__U_2)f'
# sql_params = {'__U_1': 2020, '__U_2': 10}

# Execute the query
data = q.execute()
# data = [{"status": "status1", "s": 200}, {"status": "status2", "s": 100}, ...]
```

In this example, `sql` will contain the generated SQL query and `sql_params` will contain its parameters. The `execute` method will execute the query and return the data.



## Docs

For more detailed information on how to use the ClickHouse Query library, please refer to the documentation in the [docs](docs/doc.md) directory.



## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details
