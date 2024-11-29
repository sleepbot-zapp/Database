import os
from db_meta import DBEngineMeta


class DBEngine(metaclass=DBEngineMeta):
    def __init__(self, root_directory='db_engine'):
        """
        Initialize the DBEngine instance.
        Creates the root directory structure (databases and global).
        """
        self.root_directory = root_directory
        self.databases_dir = os.path.join(self.root_directory, 'databases')
        self.global_dir = os.path.join(self.root_directory, 'global')
        if not os.path.exists(self.root_directory):
            os.makedirs(self.root_directory)
            print(f"Root directory '{self.root_directory}' created.")
        if not os.path.exists(self.databases_dir):
            os.makedirs(self.databases_dir)
            print(f"Databases directory '{self.databases_dir}' created.")
        if not os.path.exists(self.global_dir):
            os.makedirs(self.global_dir)
            print(f"Global directory '{self.global_dir}' created.")

    def create_database(self, db_name: str) -> None:
        """
        Create a new database by creating a subdirectory inside the databases directory.
        """
        db_path = os.path.join(self.databases_dir, db_name)
        if not os.path.exists(db_path):
            os.makedirs(db_path)
            print(f"Database '{db_name}' created.")
        else:
            raise FileExistsError(f"Database '{db_name}' already exists.")

    def delete_database(self, db_name: str) -> None:
        """
        Delete a database by removing the corresponding directory from the databases directory.
        """
        db_path = os.path.join(self.databases_dir, db_name)
        if os.path.exists(db_path):
            os.rmdir(db_path)
            print(f"Database '{db_name}' deleted.")
        else:
            raise FileNotFoundError(f"Database '{db_name}' does not exist.")

    @property
    def databases(self) -> list:
        """
        List all databases (directories) present in the databases directory.
        """
        return [db for db in os.listdir(self.databases_dir) if os.path.isdir(os.path.join(self.databases_dir, db))]



if __name__ == "__main__":
    engine = DBEngine()
    engine.create_database('test_db')
    engine.delete_database("test_db")
    print(engine.databases)
