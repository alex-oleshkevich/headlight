import click
import os

from headlight.migrator import MigrateHooks, Migration, Migrator, create_sql_migration

database_help = 'Database connection URL.'
migrations_help = 'Migrations directory.'
dry_run_help = 'Simulate migration execution (nothing will be applied to the database).'
table_help = 'History table name.'
revert_steps_help = 'The number of migrations to be reverted.'
fake_help = 'Write history table records without running any SQL command.'
migration_name_help = 'The name of the migration.'


class LoggingHooks(MigrateHooks):
    def before_migrate(self, migration: Migration):
        click.secho(
            '{status} {filename}'.format(
                status=click.style('Migrating', fg='yellow'),
                filename=os.path.basename(migration.file),
            ),
            nl=False,
        )

    def after_migrate(self, migration: Migration, time_taken: float):
        click.secho(
            '\r{status} {filename} {time}'.format(
                status=click.style('Done', fg='green'),
                time=click.style(f'({time_taken:.3f}s)', fg='cyan'),
                filename=os.path.basename(migration.file),
            )
        )

    def on_error(self, migration: Migration, exc: Exception, time_taken: float):
        click.secho(
            '\r{status} {filename}'.format(
                status=click.style('Fail', fg='red'),
                filename=os.path.basename(migration.file),
            )
        )


@click.group()
def app() -> None:
    pass


@app.command()
@click.option('-d', '--database', help=database_help)
@click.option(
    '-m',
    '--migrations',
    default='migrations',
    type=click.Path(file_okay=False, dir_okay=True),
    show_default=True,
    help=migrations_help,
)
@click.option('--dry-run', is_flag=True, default=False, show_default=True, help=dry_run_help)
@click.option('--table', default='migrations', show_default=True, help=table_help)
@click.option('--fake', is_flag=True, default=False, help=fake_help)
def upgrade(
    *,
    database: str,
    migrations: str,
    table: str,
    fake: bool,
    dry_run: bool,
) -> None:
    migrator = Migrator(database, migrations, table)
    migrator.initialize_db()
    migrator.upgrade(fake=fake, dry_run=dry_run, hooks=LoggingHooks())


@app.command()
@click.option('-d', '--database', help=database_help)
@click.option(
    '-m',
    '--migrations',
    default='migrations',
    type=click.Path(file_okay=False, dir_okay=True),
    show_default=True,
    help=migrations_help,
)
@click.option('--dry-run', is_flag=True, default=False, show_default=True, help=dry_run_help)
@click.option('--table', default='migrations', show_default=True, help=table_help)
@click.option('--fake', is_flag=True, default=False, help=fake_help)
@click.option('--steps', type=int, default=1, help=revert_steps_help, show_default=True)
def downgrade(
    *,
    database: str,
    migrations: str,
    table: str,
    fake: bool,
    dry_run: bool,
    steps: int,
) -> None:
    migrator = Migrator(database, migrations, table)
    migrator.initialize_db()
    try:
        migrator.downgrade(dry_run=dry_run, fake=fake, steps=steps, hooks=LoggingHooks())
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
    path = create_sql_migration(migrations, name)
    filename = os.path.basename(path)
    click.secho('Created migration %s.' % click.style(filename, bold=True))


@app.command
@click.option(
    '-m',
    '--migrations',
    default='migrations',
    type=click.Path(file_okay=False, dir_okay=True),
    show_default=True,
    help=migrations_help,
)
@click.option('--table', default='migrations', show_default=True, help=table_help)
@click.option('-d', '--database', help=database_help)
def status(
    *,
    database: str,
    migrations: str,
    table: str,
) -> None:
    migrator = Migrator(database, migrations, table)
    migrator.initialize_db()
    history = migrator.status()
    for migration in history:
        click.secho(
            '{status} {filename}'.format(
                status=(
                    click.style(f'Applied', fg='green') if migration.applied else click.style('Pending', fg='yellow')
                ),
                filename=os.path.basename(migration.filename),
            )
        )


def main() -> None:
    app()


if __name__ == '__main__':
    main()
