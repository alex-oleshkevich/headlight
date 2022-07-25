from __future__ import annotations

from dataclasses import dataclass

import datetime
import getpass
import glob
import os
import time
import typing
from re import A

from headlight.database import create_database
from headlight.drivers.base import AppliedMigration, DbDriver, DummyTransaction

MIGRATION_TEMPLATE = """
-- Author: {author}
-- Date: {date}
-- Transactional: {transactional}

{upgrade}

---- Keep this separator.

{downgrade}
"""


@dataclass
class Migration:
    name: str
    file: str
    revision: str
    transactional: bool
    upgrade_callback: typing.Callable[[DbDriver], None]
    downgrade_callback: typing.Callable[[DbDriver], None]

    @classmethod
    def from_sql_file(cls, file: str) -> Migration:
        filename = os.path.basename(file)
        revision = filename[:15]
        name, _, _ = filename[16:].rpartition('.')
        upgrade_commands = ''
        downgrade_commands = ''
        transactional = True
        parsing_state = 'header'

        with open(file) as f:
            for line in f.readlines():
                if parsing_state == 'header':
                    if line.startswith('-- Transactional'):
                        transactional = 'yes' in line.lower()

                if line.strip() == '' and parsing_state == 'header':
                    parsing_state = 'upgrade'

                if line.startswith('----'):
                    parsing_state = 'downgrade'
                    continue

                if parsing_state == 'upgrade' and not line.startswith('--'):
                    upgrade_commands += line

                if parsing_state == 'downgrade' and not line.startswith('--'):
                    downgrade_commands += line

        upgrade_commands = upgrade_commands.strip()
        downgrade_commands = downgrade_commands.strip()

        def upgrade_callback(db: DbDriver) -> None:
            if upgrade_commands:
                db.execute(upgrade_commands)

        def downgrade_callback(db: DbDriver) -> None:
            if downgrade_commands:
                db.execute(downgrade_commands)

        return Migration(
            name=name,
            file=file,
            revision=revision,
            transactional=transactional,
            upgrade_callback=upgrade_callback,
            downgrade_callback=downgrade_callback,
        )


@dataclass
class MigrationStatus:
    revision: str
    name: str
    filename: str
    applied: bool


class MigrateHooks:
    def before_migrate(self, migration: Migration):
        ...

    def after_migrate(self, migration: Migration, time_taken: float):
        ...

    def on_error(self, migration: Migration, exc: Exception, time_taken: float):
        ...


class Migrator:
    def __init__(self, url: str, directory: str, table_name: str = 'migrations') -> None:
        self.db = create_database(url)
        self.directory = directory
        self.table = table_name

    def initialize_db(self) -> None:
        self.db.create_migrations_table(self.table)

    def get_migrations(self) -> list[Migration]:
        sql_files = glob.glob(f'{self.directory}/*.sql')
        return [Migration.from_sql_file(sql_file) for sql_file in sorted(sql_files)]

    def get_applied_migrations(self, limit: int | None = None) -> dict[str, AppliedMigration]:
        return {am['revision']: am for am in self.db.get_applied_migrations(self.table, limit)}

    def get_pending_migrations(self) -> list[Migration]:
        applied = self.get_applied_migrations()
        return [migration for migration in self.get_migrations() if migration.revision not in applied]

    def upgrade(self, *, dry_run: bool = False, fake: bool = False, hooks: MigrateHooks | None = None) -> None:
        pending = self.get_pending_migrations()

        for migration in pending:
            self.apply_migration(migration, dry_run=dry_run, fake=fake, hooks=hooks)

    def downgrade(
        self,
        *,
        steps: int,
        fake: bool = False,
        dry_run: bool = False,
        hooks: MigrateHooks | None = None,
    ) -> None:
        applied = self.get_applied_migrations(steps)
        pending = [migration for migration in self.get_migrations() if migration.revision in applied]

        for migration in pending:
            self.apply_migration(migration, dry_run=dry_run, fake=fake, hooks=hooks, upgrade=False)

    def apply_migration(
        self,
        migration: Migration,
        *,
        fake: bool,
        dry_run: bool,
        upgrade: bool = True,
        hooks: MigrateHooks | None = None,
    ) -> None:
        tx = self.db.transaction() if migration.transactional else DummyTransaction()
        start_time = time.time()
        hooks = hooks or MigrateHooks()
        try:
            with tx:
                hooks.before_migrate(migration)
                if not dry_run:
                    if not fake:
                        if upgrade:
                            migration.upgrade_callback(self.db)
                            self.db.add_applied_migration(self.table, migration.revision, migration.name)
                        else:
                            migration.downgrade_callback(self.db)
                            self.db.remove_applied_migration(self.table, migration.revision)
                time_taken = time.time() - start_time
                hooks.after_migrate(migration, time_taken)
        except Exception as ex:
            time_taken = time.time() - start_time
            hooks.on_error(migration, ex, time_taken)
            raise

    def status(self) -> typing.Iterable[MigrationStatus]:
        applied = self.get_applied_migrations()
        for migration in self.get_migrations():
            yield MigrationStatus(
                name=migration.name,
                filename=migration.file,
                revision=migration.revision,
                applied=migration.revision in applied,
            )


def create_sql_migration(directory: str, name: str) -> str:
    base_dir = os.path.abspath(directory)
    os.makedirs(base_dir, exist_ok=True)

    name = name or 'unnamed'
    now = datetime.datetime.now()
    revision = now.strftime('%Y%m%d_%H%M%S')
    filename = f'{revision}_{name.replace(" ", "_").lower()}.sql'
    path = os.path.join(base_dir, filename)
    with open(path, 'w') as f:
        f.write(
            MIGRATION_TEMPLATE.format(
                name=name,
                revision=revision,
                author=getpass.getuser(),
                date=now.isoformat(),
                transactional='yes',
                upgrade='-- REPLACE THIS LINE WITH UPGRADE COMMANDS',
                downgrade='-- REPLACE THIS LINE WITH DOWNGRADE COMMANDS',
            ).strip()
        )
    return path
