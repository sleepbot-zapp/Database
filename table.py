from typing import List, Dict, Any

class Table:
    """
    A base class for a table where rows are instances of the table class.
    """

    def __init__(self, **kwargs):
        for col, value in kwargs.items():
            setattr(self, col, value)

    def insert(self):
        """
        Insert the current row into the table in the associated database.
        """
        self._db.insert_row(self)

    @classmethod
    def search(cls, **conditions) -> List['Table']:
        """
        Search for rows that match the given conditions in the associated database.
        """
        return cls._db.search_rows(cls, **conditions)

    @classmethod
    def update(cls, conditions: Dict[str, Any], updates: Dict[str, Any]):
        """
        Update rows in the table matching conditions with the provided updates in the associated database.
        """
        cls._db.update_rows(cls, conditions, updates)

    @classmethod
    def delete(cls, **conditions):
        """
        Delete rows matching the given conditions in the associated database.
        """
        cls._db.delete_rows(cls, **conditions)

    def __repr__(self):
        """
        String representation of a row.
        """
        return f"<{self.__class__.__name__} " + " ".join(f"{k}={v}" for k, v in self.__dict__.items()) + ">"