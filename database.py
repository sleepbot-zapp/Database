class TableMeta(type):
    """
    Metaclass for enforcing column types and enabling dynamic table creation.
    """
    def __new__(cls, name, bases, dct):
        annotations = dct.get('__annotations__', {})

        def __setattr__(self, name, value):
            # Enforce type constraints for annotated fields
            if name in annotations:
                expected_type = annotations[name]
                if not isinstance(value, expected_type):
                    raise TypeError(f"Attribute '{name}' must be of type {expected_type}, but got {type(value).__name__}.")
            object.__setattr__(self, name, value)  # Avoid recursion

        dct['__setattr__'] = __setattr__
        return super().__new__(cls, name, bases, dct)


class Table(metaclass=TableMeta):
    """
    Base Table class for defining and managing rows.
    """
    _database = None  # Reference to the database
    _table_name = None  # Name of the table

    def __init__(self, **kwargs):
        """
        Initialize a row with values for each column.
        Attributes must match the class-level annotations.
        """
        # Validate the presence of all annotated fields
        missing_fields = [field for field in self.__annotations__ if field not in kwargs]
        if missing_fields:
            raise ValueError(f"Missing required fields: {', '.join(missing_fields)}.")

        # Set attributes with type enforcement
        for field, value in kwargs.items():
            setattr(self, field, value)

    def insert(self):
        """
        Insert the current row into the database.
        Raises an error if the table is not registered.
        """
        if not self._database or not self._table_name or self._table_name not in self._database.tables:
            raise RuntimeError(f"Table '{self._table_name}' is not registered in the database.")
        
        # Insert row into the database
        self._database.tables[self._table_name].append(self)
        print(f"Row inserted into table '{self._table_name}': {self}")

    def __repr__(self):
        """
        String representation of a table row.
        """
        fields = ', '.join(f"{k}={v}" for k, v in self.__dict__.items())
        return f"<{self.__class__.__name__}({fields})>"

class Database:
    """
    A database for managing tables and their rows.
    """
    def __init__(self, db_name):
        self.db_name = db_name
        self.tables = {}  # Dictionary to store table rows by table name

    def create_table(self, table_class):
        """
        Register a table class in the database.
        """
        table_name = table_class.__name__
        if table_name in self.tables:
            raise RuntimeError(f"Table '{table_name}' is already registered.")

        # Attach database and table name to the table class
        table_class._database = self
        table_class._table_name = table_name

        # Initialize an empty list to store rows for this table
        self.tables[table_name] = []
        print(f"Table '{table_name}' created with columns: {', '.join(table_class.__annotations__.keys())}")

    def get_table(self, table_name):
        """
        Retrieve all rows for a given table.
        """
        if table_name not in self.tables:
            raise RuntimeError(f"Table '{table_name}' does not exist in the database.")
        return self.tables[table_name]

    def __repr__(self):
        """
        String representation of the database and its tables.
        """
        return f"<Database({self.db_name}, Tables: {list(self.tables.keys())})>"


new_db = Database("db")

class Person(Table):
    name: str
    age: int

person1 = Person(name="Hello", age=12)

try:
    person1.insert()
except RuntimeError as e:
    print(e) 
new_db.create_table(Person)

person1.insert()

print(new_db.get_table("Person"))
