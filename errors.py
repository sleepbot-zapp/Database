class DatabaseAlreadyExistsError(Exception):
    """Exception raised when a database is not found."""
    def __init__(self, db_name):
        self.db_name = db_name
        self.message = f"Database '{self.db_name}' already exists."
        super().__init__(self.message)
