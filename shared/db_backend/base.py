"""Abstract base class for database backends."""

from abc import ABC, abstractmethod
from typing import Any


class DBBackend(ABC):
    """Interface that each database backend must implement."""

    @abstractmethod
    def get_connection(self) -> Any:
        """Return a new database connection."""
        ...

    @abstractmethod
    def get_dict_cursor(self, conn: Any) -> Any:
        """Return a cursor that produces dict-like rows."""
        ...

    @abstractmethod
    def get_server_side_cursor(self, conn: Any, name: str, itersize: int = 2000) -> Any:
        """Return a server-side (streaming) cursor for large result sets."""
        ...

    @abstractmethod
    def dialect(self) -> str:
        """Return 'postgresql' or 'mysql'."""
        ...

    def adapt_row(self, row: dict[str, Any] | None) -> dict[str, Any] | None:
        """Post-process a fetched row (e.g. deserialise JSON strings).

        Default implementation is identity; MySQL backend overrides this
        to convert JSON column strings into Python dicts.
        """
        return row

    def adapt_rows(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Post-process a list of fetched rows."""
        return [self.adapt_row(r) for r in rows]  # type: ignore[arg-type]
