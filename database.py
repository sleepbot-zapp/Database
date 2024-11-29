import os
import pickle
import logging
from typing import List, Dict, Any
from table import Table

log_dir = 'db_engine/global'
if not os.path.exists(log_dir):
    os.makedirs(log_dir)
logging.basicConfig(
    filename=os.path.join(log_dir, 'database.log'),
    level=logging.INFO,
    format='%(asctime)s - %(message)s'
)


class Database:
    """
    A class representing a database and providing methods for creating tables,
    and performing operations (insert, search, update, delete) on them.
    """
    DB_ROOT = "db_engine/databases"

    def __init__(self, db_name: str):
        """
        Initialize the database, either creating it or loading an existing one.
        """
        self.db_name = db_name
        self.db_path = os.path.join(self.DB_ROOT, db_name)
        if not os.path.exists(self.db_path):
            raise FileNotFoundError(f"Database '{db_name}' does not exist.")
        self.tables = {}  
        self.is_active = True

    def create_table(self, table_class: type):
        if not self.is_active:
            raise RuntimeError("Cannot create table. Database connection is terminated.")
        
        """
        Register a table class in the database, dynamically associating the table
        with the current database instance.
        """
        table_name = table_class.__name__.lower()
        if table_name in self.tables:
            raise RuntimeError(f"Table '{table_name}' already exists.")
        
        table_class._db = self
        self.tables[table_name] = table_class
        logging.info(f"Table '{table_name}' created in database '{self.db_name}'.")

    def insert_row(self, row: Table):
        if not self.is_active:
            raise RuntimeError("Cannot insert row. Database connection is terminated.")
        
        """
        Insert a row into the table's binary file.
        """
        table_name = row.__class__.__name__.lower()
        table_file = os.path.join(self.db_path, f"{table_name}.bin")

        rows = self._load_table_data(table_name)
        rows.append(row)
        self._save_table_data(table_name, rows)

    def search_rows(self, table_class, **conditions) -> List[Table]:
        if not self.is_active:
            raise RuntimeError("Cannot search rows. Database connection is terminated.")
        
        """
        Search for rows in a given table that match the conditions.
        """
        table_name = table_class.__name__.lower()
        rows = self._load_table_data(table_name)
        return [row for row in rows if all(getattr(row, k) == v for k, v in conditions.items())]

    def update_rows(self, table_class, conditions: Dict[str, Any], updates: Dict[str, Any]):
        if not self.is_active:
            raise RuntimeError("Cannot update rows. Database connection is terminated.")
        
        """
        Update rows in a given table that match the conditions with the new values in updates.
        """
        table_name = table_class.__name__.lower()
        rows = self._load_table_data(table_name)

        for row in rows:
            if all(getattr(row, k) == v for k, v in conditions.items()):
                for key, value in updates.items():
                    setattr(row, key, value)

        self._save_table_data(table_name, rows)

    def delete_rows(self, table_class, **conditions):
        if not self.is_active:
            raise RuntimeError("Cannot delete rows. Database connection is terminated.")
        
        """
        Delete rows in a given table that match the conditions.
        """
        table_name = table_class.__name__.lower()
        rows = self._load_table_data(table_name)

        filtered_rows = [row for row in rows if not all(getattr(row, k) == v for k, v in conditions.items())]
        self._save_table_data(table_name, filtered_rows)

    def _save_table_data(self, table_name, rows):
        """
        Save table data to a binary file.
        """
        table_file = os.path.join(self.db_path, f"{table_name}.bin")
        with open(table_file, "wb") as f:
            pickle.dump(rows, f)

    def _load_table_data(self, table_name):
        """
        Load table data from a binary file.
        """
        table_file = os.path.join(self.db_path, f"{table_name}.bin")
        if not os.path.exists(table_file):
            return []
        with open(table_file, "rb") as f:
            return pickle.load(f)

    def terminate(self):
        """
        Close the database connection and save all changes to the disk.
        """
        if self.is_active:
            logging.info(f"Database '{self.db_name}' connection terminated.")
            self.is_active = False
        else:
            logging.warning(f"Database '{self.db_name}' is already terminated.")

class DBEngine:
    """
    A class to manage the creation of databases and logging operations.
    """
    DB_ROOT = "db_engine/databases"
    
    @staticmethod
    def create_database(db_name: str):
        """
        Create a new database directory if it doesn't already exist.
        Logs the operation to 'database.log'.
        """
        db_path = os.path.join(DBEngine.DB_ROOT, db_name)
        if not os.path.exists(db_path):
            os.makedirs(db_path)
            logging.info(f"Database '{db_name}' created.")
        else:
            logging.info(f"Database '{db_name}' already exists.")


def database(db: Database):
    def decorator(cls):
        logging.info(f"Database '{db.db_name}' connection generated.")
        cls._db = db
        return cls
    return decorator


if __name__ == "__main__":
    
    DBEngine.create_database('test_db')
    
    test_db = Database('test_db')
    
    @database(test_db)
    class Person(Table):
        name: str
        age: int
    
    p1 = Person(name="Hello", age=12)
    p1.insert()
    
    print(Person.search(name="Hello"))
    
    Person.update(conditions={"name": "Hello"}, updates={"age": 17})
    
    print(Person.search(name="Hello"))

    Person.delete(name="Hello")
    
    print(Person.search(name="Hello"))

    test_db.terminate()
