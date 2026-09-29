"""The database the loader writes to and the dashboard reads from.

WAREHOUSE=redshift (the default) uses Amazon Redshift. WAREHOUSE=postgres uses
PostgreSQL, which costs almost nothing to keep running 24/7 next to the rest of
the pipeline. Both speak the same SQL for everything this project does.
"""
from config import settings


def connect(autocommit=False):
    if settings.WAREHOUSE == "postgres":
        import psycopg
        return psycopg.connect(**settings.postgres_connection_args(), autocommit=autocommit)

    import redshift_connector
    conn = redshift_connector.connect(**settings.redshift_connection_args())
    conn.autocommit = autocommit
    return conn


def connection_errors():
    """Exceptions that mean the connection itself broke (worth reconnecting)."""
    if settings.WAREHOUSE == "postgres":
        import psycopg
        return (psycopg.OperationalError, psycopg.InterfaceError)

    import redshift_connector
    return (redshift_connector.InterfaceError, redshift_connector.OperationalError)
