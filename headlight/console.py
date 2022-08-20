import click
import os

from headlight.migrator import MigrateHooks, Migration, Migrator, create_migration_template

database_help = 'Database connection URL.'
migrations_help = 'Migrations directory.'
dry_run_help = 'Simulate migration execution (nothing will be applied to the database).'
table_help = 'History table name.'
revert_steps_help = 'The number of migrations to be reverted.'
fake_help = 'Write history table records without running any SQL command.'
print_help = 'Print generated SQL to stderr.'
migration_name_help = 'The name of the migration.'

DATABASE_ENVVAR = 'HL_DATABASE_URL'


class LoggingHooks(MigrateHooks):
    def before_migrate(self, migration: Migration) -> None:
        click.secho(
            '{status} {filename}'.format(
                status=click.style('Migrating'.ljust(10, ' '), fg='yellow'),
                filename=os.path.basename(migration.file),
            ),
            nl=False,
        )

    def after_migrate(self, migration: Migration, time_taken: float) -> None:
        click.secho(
            '\r{status} {filename} {time}'.format(
                status=click.style('Done'.ljust(10, ' '), fg='green'),
                time=click.style(f'({time_taken:.3f}s)', fg='cyan'),
                filename=os.path.basename(migration.file),
            )
        )

    def on_error(self, migration: Migration, exc: Exception, time_taken: float) -> None:
        click.secho(
            '\r{status} {filename}'.format(
                status=click.style('Fail'.ljust(10, ' '), fg='red'),
                filename=os.path.basename(migration.file),
            )
        )


@click.group()
def app() -> None:
    pass


@app.command()
@click.option('-d', '--database', help=database_help, envvar=DATABASE_ENVVAR)
@click.option(
    '-m',
    '--migrations',
    default='migrations',
    type=click.Path(file_okay=False, dir_okay=True, resolve_path=True),
    show_default=True,
    help=migrations_help,
)
@click.option('--dry-run', is_flag=True, default=False, show_default=True, help=dry_run_help)
@click.option('--table', default='migrations', show_default=True, help=table_help)
@click.option('--fake', is_flag=True, default=False, help=fake_help)
@click.option('--print-sql', is_flag=True, default=False, help=print_help)
def upgrade(
    *,
    database: str,
    migrations: str,
    table: str,
    fake: bool,
    dry_run: bool,
    print_sql: bool,
) -> None:
    migrator = Migrator(database, migrations, table)
    migrator.initialize_db()
    migrator.upgrade(fake=fake, dry_run=dry_run, print_sql=print_sql, hooks=LoggingHooks())


@app.command()
@click.option('-d', '--database', help=database_help, envvar=DATABASE_ENVVAR)
@click.option(
    '-m',
    '--migrations',
    default='migrations',
    type=click.Path(file_okay=False, dir_okay=True, resolve_path=True),
    show_default=True,
    help=migrations_help,
)
@click.option('--dry-run', is_flag=True, default=False, show_default=True, help=dry_run_help)
@click.option('--table', default='migrations', show_default=True, help=table_help)
@click.option('--fake', is_flag=True, default=False, help=fake_help)
@click.option('--steps', type=int, default=1, help=revert_steps_help, show_default=True)
@click.option('--print-sql', is_flag=True, default=False, help=print_help)
def downgrade(
    *,
    database: str,
    migrations: str,
    table: str,
    fake: bool,
    dry_run: bool,
    print_sql: bool,
    steps: int,
) -> None:
    migrator = Migrator(database, migrations, table)
    migrator.initialize_db()
    try:
        migrator.downgrade(dry_run=dry_run, fake=fake, steps=steps, print_sql=print_sql, hooks=LoggingHooks())
    except Exception as ex:
        click.secho(click.style(ex, fg='red'), err=True)


@app.command
@click.option(
    '-m',
    '--migrations',
    default='migrations',
    type=click.Path(file_okay=False, dir_okay=True),
    show_default=True,
    help=migrations_help,
)
@click.option('--name', help=migration_name_help)
def new(
    *,
    migrations: str,
    name: str,
) -> None:
    path = create_migration_template(migrations, name)
    filename = os.path.basename(path)
    click.secho('Created migration %s.' % click.style(filename, bold=True))


@app.command
@click.option(
    '-m',
    '--migrations',
    default='migrations',
    type=click.Path(file_okay=False, dir_okay=True, resolve_path=True),
    show_default=True,
    help=migrations_help,
)
@click.option('--table', default='migrations', show_default=True, help=table_help)
@click.option('-d', '--database', help=database_help, envvar='HL_DATABASE_URL')
def status(
    *,
    database: str,
    migrations: str,
    table: str,
) -> None:
    assert database
    migrator = Migrator(database, migrations, table)
    migrator.initialize_db()
    history = migrator.status()
    has_entries = False

    for migration in history:
        has_entries = True
        click.secho(
            '{status} {filename}'.format(
                status=(
                    click.style('Applied', fg='green') if migration.applied else click.style('Pending', fg='yellow')
                ),
                filename=os.path.basename(migration.filename),
            )
        )

    if not has_entries:
        click.secho("No migration entries in history.")


def main() -> None:
    app()


if __name__ == '__main__':
    main()
