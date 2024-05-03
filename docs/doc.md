## !!! THIS DOCUMENT IS NOT COMPLETED !!!


## QuerySet
The `QuerySet` is a core component of `clickhouse-query` that is used for constructing queries. It includes several methods that correspond to different parts of SQL.

### select
The `select` method corresponds to the `SELECT` clause in SQL. It allows you to specify the columns that you want to include in your query.

Here are some examples of how to use the `select` method:

- **Without using the select method:**

  If you don't specify any columns using the `select` method, the query will return all columns from the table.

  ```python
  q = ch.QuerySet("my_table")
  # Generated Query: SELECT * FROM my_table
  ```

- **With arguments:**
  You can specify the columns you want to include in your query as arguments to the `select` method
  ```python
  q = ch.QuerySet("my_table").select("col1", "col2")
  # Generated Query: SELECT col1, col2 FROM my_table
  ```
- **With keyword arguments (recommended, see execute):**
  You can also use keyword arguments to specify the columns for your query. This allows you to rename the columns in the result set.
  ```python
  q = ch.QuerySet("my_table").select(c1="col1", c2="col2")
  # Generated Query: SELECT col1 AS c1, col2 AS c2 FROM my_table
  ```

To clear the select clause, pass `None` to it:
```python
q = ch.QuerySet("my_table").select(c1="col1", c2="col2").select(None)
# Generated Query: SELECT * FROM my_table
```

### distinct
The `distinct` method corresponds to the `DISTINCT` clause in SQL.

Here are some examples of how to use the `distinct` method:

- **without arguments:**
  If you don't specify any columns using the `distinct` method, the distinct applies on all columns
  ```python
  q = ch.QuerySet("my_table").select(c1="col1", c2="col2").distinct()
  # Generated Query: SELECT DISTINCT col1 AS c1, col2 AS c2 FROM my_table
  ```

- **With arguments:**
  You can specify the columns you want to include in your query as arguments to the `distinct` method
  ```python
  q = ch.QuerySet("my_table").select("col1", "col2").distinct("col1")
  # Generated Query: SELECT DISTINCT ON (col1) col1, col2 FROM my_table

To clear the distinct clause, pass `None` to it:
```python
q = ch.QuerySet("my_table").select(c1="col1", c2="col2").distinct("col1").distinct(None)
# Generated Query: SELECT col1, col2 FROM my_table
```

### from_
The `from_` method corresponds to the `FROM` clause in SQL.

Here are some examples of how to use the `from_` method:

- **with str argument**
  If a string passed to `from_` method, it means the table name.
  ```python
  q = ch.QuerySet().from_("my_table")
  # Generated Query: SELECT * FROM my_table
  ```
- **with function or expression argument**
  you can passed function on expression to `from_` method.
  ```python
  q = ch.QuerySet().from_(chf.numbers(5))
  # Generated Query: SELECT * FROM numbers(5)
  ```
  It also supports subqueries:
  ```python
  sub_q = ch.SubQuery(ch.QuerySet("my_table").where(a=1))
  q = ch.QuerySet().from_(sub_q)
  # Generated Query: SELECT * FROM (SELECT * FROM my_table WHERE a = 1)
  ```

The `FROM` clause can also be specified in the `QuerySet` arguments:

```python
q = ch.QuerySet("my_table")
# Generated Query: SELECT * FROM my_table
```

To clear the from clause, pass `None` to it:
```python
q = ch.QuerySet().from_("my_table").from_(None)
# Generated Query: SELECT *
```

### where, prewhere, having
The `where`, `prewhere` and `having` methods corresponds to the `WHERE`, `PREWHERE` and `HAVING` clause in SQL. These clauses have similar functionalities. Here are some examples of how to use these methods:

- **with args:**
  You can pass conditions as arguments to these methods. The `and` operator applies to these conditions. For example:
  ```python
  q = ch.QuerySet("my_table").where(chf.equals("a", 1), chf.equals("b", 2))
  # Generated Query: SELECT * FROM my_table WHERE and(equals(a, 1), equals(b, 2))
  ```
- **with keyword arguments**
  Using keyword arguments simplifies the process. For instance, the previous query can be written as:
  ```python
  q = ch.QuerySet("my_table").where(a=1, b=2)
  # Generated Query: SELECT * FROM my_table WHERE and(equals(a, 1), equals(b, 2))
  ```
  You can also use other comparison methods:
  ```python
  q = ch.QuerySet("my_table").where(a__lt=1, b__gte=2, c__isnull=True)
  # Generated Query: SELECT * FROM my_table WHERE and(less(a, 1), greaterOrEquals(b, 2), isNull(c))
  ```
  You can also use some pre-defined functions:
  ```python
  q = ch.QuerySet("my_table").where(date__day__lt=15)
  # Generated Query: SELECT * FROM my_table WHERE less(equlastoDayOfYear(date), 15)
  ```

To clear these clauses, pass `None` to them:
```python
q = ch.QuerySet("my_table").where(a=1).where(None)
# Generated Query: SELECT * FROM my_table
```

### group_by
The `group_by` method corresponds to the `GROUP BY` clause in SQL.

Here are some examples of how to use the `group_by` method:
- **with arguments:**
  you can passed strings or expressions to this function.
  ```python
  q = ch.QuerySet("my_table").select("col1", "col2", s=chf.sum("col3")).group_by("col1", "col2")
  # Generated Query: SELECT col1, col2, sum(col3) FROM my_table GROUP BY col1, col2
  ```

To clear `GROUP BY` clauses, pass `None` to it:
```python
q = ch.QuerySet("my_table").select(s=chf.sum("col3")).group_by("col1", "col2").group_by(None)
# Generated Query: SELECT sum(col3) FROM my_table
```

### order_by
The `order_by` method corresponds to the `ORDER BY` clause in SQL.

Here are some examples of how to use the `order_by` method:
- **with arguments:**
  To specify descending order, prepend the field name with a `-` character. For example:
  ```python
  q = ch.QuerySet("my_table").order_by("col1", "-col2")
  # Generated Query: SELECT * FROM my_table ORDER BY col1 ASC, col2 DESC
  ```

To clear `ORDER BY` clause, pass `None` to it:
```python
q = ch.QuerySet("my_table").order_by("col1", "-col2").order_by(None)
# Generated Query: SELECT * FROM my_table
```

### limit
The `limit` method corresponds to the `LIMIT OFFSET` clause in SQL.

Here are some examples of how to use the `limit` method:
- **only define `limit`:**
  ```python
  q = ch.QuerySet("my_table").limit(10)
  # Generated Query: SELECT * FROM my_table limit 10
  ```
- ** define `limit` and `offset`:**
  ```python
  q = ch.QuerySet("my_table").limit(10, offset=20)
  # Generated Query: SELECT * FROM my_table limit 10 OFFSET 20
  ```

To clear `LIMIT` clause, pass `None` to it:
```python
q = ch.QuerySet("my_table").limit(10, offset=20).limit(None)
# Generated Query: SELECT * FROM my_table
```

### limit_by
The `limit_by` method corresponds to the `LIMIT OFFSET BY` clause in SQL.

Here are some examples of how to use the `limit_by` method:
- **only define `limit`:**
  it is similar to `limit` method.
  ```python
  q = ch.QuerySet("my_table").limit)by(10)
  # Generated Query: SELECT * FROM my_table limit 10
  ```
- ** define `limit` and `offset`:**
  it is similar to `limit` method.
  ```python
  q = ch.QuerySet("my_table").limit(10, offset=20)
  # Generated Query: SELECT * FROM my_table limit 10 OFFSET 20
  ```
- ** define `BY` clause:
  ```python
  q = ch.QuerySet("my_table").limit(10, 'col1', 'col2', offset=20)
  # Generated Query: SELECT * FROM my_table limit 10 OFFSET 20 BY col1, col2
  ```

To clear `LIMIT BY` clause, pass `None` to it:
```python
q = ch.QuerySet("my_table").limit(10, 'col1', 'col2', offset=20).limit(None)
# Generated Query: SELECT * FROM my_table
```

## functions
The `functions` object is another key component. It generates the functions required for your queries.

You can import it as follows:

```python
from clickhouse_query import functions as chf
```

Here are some examples of how to use it:

```python
f = chf.count()
# Generated SQL: count()

f = chf.equals("a", "b")
# Generated SQL: equals(a, b)

f = chf.sumIf("a", chf.equals("b", 1))
# Generated SQL: sumIf(a, equals(b, 1))
```

## other classes
Several classes are defined for use in your queries:

### NULL
This corresponds to NULL in SQL:
```python
q = ch.QuerySet("my_table").select(ch.NULL)
# Generated Query: SELECT NULL
```

### F
This represents a field:
```python
q = ch.QuerySet("my_table").where(a=F("b"))
# Generated Query: SELECT * FROM my_table WHERE a = b
```

### Value
This represents a value:
```python
q = ch.QuerySet("my_table").select(ch.Value("a"))
# Generated Query: SELECT 'a' FROM my_table
```

### Q
This applies a function (`_connector`) to a group of conditions. By default, `_connector` is the `and` function:

```python
q = ch.QuerySet("my_table").where(Q(a=1, b=2))
# Generated Query: SELECT * FROM my_table WHERE and(equals(a, 1), equals(b, 2))
```

You can change `_connector`:

```python
q = ch.QuerySet("my_table").where(Q(a=1, b=2, _connector="or"))
# Generated Query: SELECT * FROM my_table WHERE or(equals(a, 1), equals(b, 2))
```

```python
q = ch.QuerySet("my_table").where(Q(a=1, b=2, _connector="myConnector"))
# Generated Query: SELECT * FROM my_table WHERE myConnector(equals(a, 1), equals(b, 2))
```

### Subquery
This defines a `SubQuery` to use in the `from_` method:
```python
sub_q = ch.SubQuery(ch.QuerySet("my_table").where(a=1))
q = ch.QuerySet().from_(sub_q)
# Generated Query: SELECT * FROM (SELECT * FROM my_table WHERE a = 1)
```