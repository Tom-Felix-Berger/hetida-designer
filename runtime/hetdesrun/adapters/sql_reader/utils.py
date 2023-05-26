import functools

from hetdesrun.adapters.sql_reader.config import (
    SQLReaderDBConfig,
    sql_reader_adapter_config,
)


def to_url_representation(path: str) -> str:
    """Convert path to a representation that can be used in urls/queries"""
    return path.replace("_", "-_-").replace("/", "__")


def from_url_representation(url_rep: str) -> str:
    """Reconvert url representation of path to actual path"""
    return url_rep.replace("__", "/").replace("-_-", "_")


@functools.cache
def get_configured_dbs_by_key() -> dict[str, SQLReaderDBConfig]:
    return {
        configured_db.key: configured_db
        for configured_db in sql_reader_adapter_config.sql_databases
    }