from __future__ import annotations

import wrds


def get_wrds_connection(
    *,
    username: str | None = None,
    password: str | None = None,
    autoconnect: bool = True,
) -> wrds.Connection:
    """Create a WRDS connection using explicit credentials or pgpass/env defaults."""
    kwargs: dict[str, str] = {}
    if username:
        kwargs["wrds_username"] = username
    if password:
        kwargs["wrds_password"] = password
    return wrds.Connection(autoconnect=autoconnect, **kwargs)


def run_wrds_sql(connection: wrds.Connection, sql: str):
    """Run a SQL query against WRDS."""
    return connection.raw_sql(sql)
