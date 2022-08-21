import sys


def supports_colors() -> bool:
    return hasattr(sys.stdout, 'isatty') and sys.stdout.isatty()


def colorize_sql(sql: str) -> str:
    try:
        import pygments
        from pygments.formatters import get_formatter_by_name
        from pygments.lexers import get_lexer_by_name

        if supports_colors():
            return pygments.highlight(sql, get_lexer_by_name('sql'), get_formatter_by_name('terminal'))
        return sql
    except ImportError:
        return sql
