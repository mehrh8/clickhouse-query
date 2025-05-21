# ClickHouse Query

[![PyPI version](https://img.shields.io/pypi/v/clickhouse-query.svg)](https://pypi.org/project/clickhouse-query/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**ClickHouse Query** is a Python library that functions as a powerful SQL generator for ClickHouse. It simplifies the construction of complex queries by offering a fluent, Pythonic API and a comprehensive set of helper functions.

## Features

*   Fluent and intuitive API for building ClickHouse queries.
*   Comprehensive set of functions for data manipulation and aggregation.
*   Support for PREWHERE, GROUP BY, ORDER BY, LIMIT, and other ClickHouse clauses.
*   Easy integration with your existing ClickHouse client.
*   Automatic generation of parameterized SQL queries to prevent SQL injection.

## Installation

You can install the ClickHouse Query library using pip. Run the following command in your terminal:


```bash
pip install clickhouse-query
```

## Usage

Here's an example of how to use the ClickHouse Query library:

```python
import clickhouse_query as ch
from clickhouse_query import functions as chf

# Create a QuerySet instance for your table.
# Replace "my_table" with the actual name of your table.
# The `clickhouse_client` parameter should be your configured ClickHouse client instance.
q = (
    ch.QuerySet("my_table", clickhouse_client=...)
    # Filters rows before reading data from disk, improving query performance.
    # This example filters for records where the year of the 'date' column is 2020 or later.
    .prewhere(date__year__gte=2020)
    # Groups the data by the 'status' column.
    .group_by("status")
    # Selects the 'status' column and calculates the sum of the 'price' column, aliasing it as 's'.
    .select("status", s=chf.sum("price"))
    # Orders the results by the sum of 'price' ('s') in descending order.
    .order_by("-s")
    # Limits the number of returned rows to 10.
    .limit(10)
)

# Get the generated SQL query and its parameters.
# This is useful for debugging or understanding the underlying query.
sql, sql_params  = ch.get_sql(q)
# Example output:
# sql = 'SELECT status, sum(price) AS s FROM my_table PREWHERE (greaterOrEquals(toYear(date), %(__U_1)f)) GROUP BY status ORDER BY s DESC LIMIT %(__U_2)f'
# sql_params = {'__U_1': 2020, '__U_2': 10}

# Execute the query against your ClickHouse database.
# The `execute()` method returns a list of dictionaries, where each dictionary represents a row.
data = q.execute()
# Example output:
# data = [{"status": "status1", "s": 200}, {"status": "status2", "s": 100}, ...]
```

In this example, `sql` will contain the generated SQL query and `sql_params` will contain its parameters. The `execute` method will execute the query and return the data.



## Docs

For more detailed information on how to use the ClickHouse Query library, please refer to the documentation in the [docs](docs/doc.md) directory.

## Contributing

Contributions are welcome! If you'd like to contribute, please follow these steps:

1.  Fork the repository.
2.  Create a new branch for your feature or bug fix.
3.  Make your changes and commit them with clear messages.
4.  Push your changes to your fork.
5.  Submit a pull request.

## Tests

To run the tests for this library, you can use your preferred test runner (e.g., pytest). Make sure you have the necessary dependencies installed.

```bash
# Example using pytest
pytest
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details
