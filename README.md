# Pg Sql Helper

### tl;dr

An highly opinionated helpers to get relevant columns from a `SELECT * FROM my_table` sql query.


## Features

1. Allow to rename columns.

2. Convert timestamp to date by default

## Usage


```python

from pg_sql_helper import PgSqlHelper

helper = PgSqlHelper(db="mydb", user="me", password="1234")
# host and port are optional, default to localhost
# you may also add lang arguments like `lanf="fr_FR"`, default is 'en_US'

fields_list = helper.process(table)

```

Before to get columns you may prefer exclude some fields

```python
helper.set_columns_to_drop(["field1", "field2"])
# fields containing _id at the end of the name
helper.set_columns_to_drop_according_to_regex([r"_id$"])

```


## Use case

You dynamically want to produce dataset with relevant columns according your own wishes and use this tooling can help a lot.


## Installation

```bash
pip install git+https://github.com/bealdav/pg-sql-helper.git@main
```

## Roadmap

- [ ] Github Actions for CI/CD
- [ ] Add unit tests
