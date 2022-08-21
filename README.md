# headlight

A database migration toolkit.

![PyPI](https://img.shields.io/pypi/v/headlight)
![GitHub Workflow Status](https://img.shields.io/github/workflow/status/alex-oleshkevich/headlight/Lint)
![GitHub](https://img.shields.io/github/license/alex-oleshkevich/headlight)
![Libraries.io dependency status for latest release](https://img.shields.io/librariesio/release/pypi/headlight)
![PyPI - Downloads](https://img.shields.io/pypi/dm/headlight)
![GitHub Release Date](https://img.shields.io/github/release-date/alex-oleshkevich/headlight)

## Installation

Install `headlight` using PIP or poetry:

```bash
pip install headlight
# or
poetry add headlight
```

## Features

- TODO

## Usage

### Create migration file

```bash
# create migration with
headlight new --name initial
```

It will create a new python file in `migrations` directory

### Define schema

```python
# migrations/0000_initial.py

from headlight import Blueprint, types

date = "2022-08-21T16:19:13.465195"
author = "alex"
transactional = True


def migrate(schema: Blueprint) -> None:
    with schema.create_table('users') as table:
        table.autoincrements()
        table.add_column('first_name', types.VarCharType(256))
        table.add_column('last_name', types.VarCharType(256))
        table.add_column('email', types.VarCharType(256))
        table.add_column('password', types.VarCharType(512))
        table.add_column('active', types.BooleanType(), default='1')
        table.add_column('photo', types.VarCharType(512), null=True)
        table.add_column('deleted_at', types.DateTimeType(True), null=True)
        table.created_timestamp('joined_at')
        table.add_index(['(lower(email))'], unique=True)
```

### Execute migration

```bash
headlight upgrade
```

All migrations will be applied to the database

### Rollback migration

```bash
headlight downgrade
```

The last migration will be rolled back,
