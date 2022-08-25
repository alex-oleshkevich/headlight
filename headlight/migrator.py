from __future__ import annotations

from dataclasses import dataclass

import datetime
import getpass
import glob
import importlib
import io
import os
import sys
import time
import typing

from headlight.database import create_database
from headlight.drivers.base import AppliedMigration, DummyTransaction
from headlight.schema.builder import Blueprint
from headlight.schema.ops import Operation
from headlight.utils import colorize_sql

MIGRATION_TEMPLATE = """
from headlight import Blueprint, types

date = "{date}"
author = "{author}"
transactional = True


def migrate(schema: Blueprint) -> None:
    pass

"""


class MigrationError(Exception):
    def __init__(self, message: str, migration: Migration, stmt: str) -> None:
        super().__init__(message)
        self.migration = migration
        self.stmt = stmt


@dataclass
class Migration:
    name: str
    file: str
    revision: str
    transactional: bool
    ops: list[Operation]

    @classmethod
    def from_py_module(cls, py_module: str) -> Migration:
        mod = importlib.import_module(py_module)
        filename = os.path.basename(typing.cast(str, mod.__file__))
        revision = filename[:15]
        name, _, _ = filename[16:].rpartition(".")

        schema = Blueprint()
        mod.migrate(schema)

        return Migration(
            name=name,
            file=filename,
            revision=revision,
            ops=schema.get_ops(),
            transactional=getattr(mod, "transactional", True),
        )


@dataclass
class MigrationStatus:
    revision: str
    name: str
    filename: str
    applied: bool


class MigrateHooks:
    def before_migrate(self, migration: Migration) -> None:
        ...

    def after_migrate(self, migration: Migration, time_taken: float) -> None:
        ...

    def on_error(self, migration: Migration, exc: Exception, time_taken: float) -> None:
        ...


class Migrator:
    def __init__(self, url: str, directory: str, table_name: str = "migrations") -> None:
        self.db = create_database(url)
        self.directory = directory
        self.table = table_name

    def initialize_db(self) -> None:
        self.db.create_migrations_table(self.table)

    def get_migrations(self) -> list[Migration]:
        sys.path.insert(0, self.directory)
        migration_files = glob.glob(f"{self.directory}/*.py")
        return [
            Migration.from_py_module(os.path.basename(py_module.replace(".py", "")))
            for py_module in sorted(migration_files)
        ]

    def get_applied_migrations(self, limit: int | None = None) -> dict[str, AppliedMigration]:
        return {am["revision"]: am for am in self.db.get_applied_migrations(self.table, limit)}

    def get_pending_migrations(self) -> list[Migration]:
        applied = self.get_applied_migrations()
        return [migration for migration in self.get_migrations() if migration.revision not in applied]

    def upgrade(
        self,
        *,
        dry_run: bool = False,
        fake: bool = False,
        print_sql: bool = False,
        hooks: MigrateHooks | None = None,
    ) -> None:
        pending = self.get_pending_migrations()

        for migration in pending:
            self.apply_migration(migration, dry_run=dry_run, fake=fake, print_sql=print_sql, hooks=hooks)

    def downgrade(
        self,
        *,
        steps: int,
        fake: bool = False,
        dry_run: bool = False,
        print_sql: bool = False,
        hooks: MigrateHooks | None = None,
    ) -> None:
        applied = self.get_applied_migrations(steps)
        pending = [migration for migration in self.get_migrations() if migration.revision in applied]

        for migration in reversed(sorted(pending, key=lambda x: x.revision)):
            self.apply_migration(migration, dry_run=dry_run, fake=fake, print_sql=print_sql, hooks=hooks, upgrade=False)

    def reset(self, hooks: MigrateHooks | None = None) -> None:
        self.downgrade(steps=999_999, hooks=hooks)

    def apply_migration(
        self,
        migration: Migration,
        *,
        fake: bool,
        dry_run: bool,
        upgrade: bool = True,
        print_sql: bool = False,
        hooks: MigrateHooks | None = None,
        writer: io.StringIO = sys.stderr,
    ) -> None:
        tx = self.db.transaction() if migration.transactional else DummyTransaction(self.db)
        start_time = time.time()
        hooks = hooks or MigrateHooks()
        current_stmt = ""
        try:
            with tx, self.db.lock(self.table):
                hooks.before_migrate(migration)
                stmts = [(op.to_up_sql(self.db) if upgrade else op.to_down_sql(self.db)) for op in migration.ops]
                if not upgrade:
                    stmts = list(reversed(stmts))

                sql = ";\n".join(stmts) + ";"
                if print_sql:
                    writer.write("\n")
                    writer.write(colorize_sql(f"-- rev. {migration.revision} from {migration.file}"))
                    writer.write(colorize_sql(sql))
                    writer.write(colorize_sql(f"-- end rev. {migration.revision}"))
                    writer.write("\n")

                if not dry_run:
                    if not fake:
                        for stmt in stmts:
                            current_stmt = stmt
                            self.db.execute(stmt)

                    if upgrade:
                        self.db.add_applied_migration(self.table, migration.revision, migration.name)
                    else:
                        self.db.remove_applied_migration(self.table, migration.revision)

                time_taken = time.time() - start_time
                hooks.after_migrate(migration, time_taken)
        except Exception as ex:
            time_taken = time.time() - start_time
            hooks.on_error(migration, ex, time_taken)
            raise MigrationError(str(ex), migration, current_stmt) from ex

    def status(self) -> typing.Iterable[MigrationStatus]:
        applied = self.get_applied_migrations()
        for migration in self.get_migrations():
            yield MigrationStatus(
                name=migration.name,
                filename=migration.file,
                revision=migration.revision,
                applied=migration.revision in applied,
            )

    @classmethod
    def new(cls, database_url: str, directory: str = "migrations", table_name: str = "migrations") -> Migrator:
        migrator = Migrator(url=database_url, directory=directory, table_name=table_name)
        migrator.initialize_db()
        return migrator


def create_migration_template(directory: str, name: str) -> str:
    base_dir = os.path.abspath(directory)
    os.makedirs(base_dir, exist_ok=True)

    name = name or "unnamed"
    now = datetime.datetime.now()
    revision = now.strftime("%Y%m%d_%H%M%S")
    filename = f'{revision}_{name.replace(" ", "_").lower()}.py'
    path = os.path.join(base_dir, filename)
    with open(path, "w") as f:
        f.write(
            MIGRATION_TEMPLATE.format(
                name=name,
                revision=revision,
                author=getpass.getuser(),
                date=now.isoformat(),
                transactional="True",
            ).strip()
        )
    return path
