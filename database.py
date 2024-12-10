from typing import List, Dict, Any
import os
import time
from collections import deque
from datetime import datetime
import uuid
import stat
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives import hashes
import json


class DatabaseEngine:
    def __init__(self, base_dir="db_engine", passphrase="supersecret"):
        self.base_dir = base_dir
        self.databases_dir = os.path.join(base_dir, "databases")
        self.global_log_path = os.path.join(base_dir, "global", "global.log")
        self.active_connections = {}
        self.connection_queues = {}

        os.makedirs(self.databases_dir, exist_ok=True)
        os.makedirs(os.path.join(base_dir, "global"), exist_ok=True)

        self.passphrase = passphrase
        self._log_global("Database engine initialized.")

    def _log_global(self, message):
        """Logs a message to the global log."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_message = f"[{timestamp}] {message}\n"
        with open(self.global_log_path, "a") as log_file:
            log_file.write(log_message)

    def _log_database(self, database_name, message):
        """Logs a message to a specific database's log."""
        db_log_path = os.path.join(
            self.databases_dir, database_name, f"{database_name}.log"
        )
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_message = f"[{timestamp}] {message}\n"
        with open(db_log_path, "a") as log_file:
            log_file.write(log_message)

    def _initialize_database_if_needed(self, database_name):
        """Initialize the connection tracking for a database if not already set up."""
        if database_name not in self.active_connections:
            db_path = os.path.join(self.databases_dir, database_name)
            if not os.path.exists(db_path):
                self._log_global(
                    f"Failed to access database '{database_name}': does not exist."
                )
                raise FileNotFoundError(f"Database '{database_name}' does not exist.")

            self.active_connections[database_name] = None
            self.connection_queues[database_name] = deque()
            self._log_global(f"Database '{database_name}' initialized.")
            self._log_database(database_name, "Connection tracking initialized.")

    def create_database(self, database_name):
        """Creates a new database directory and log file, encrypting the database key."""
        db_path = os.path.join(self.databases_dir, database_name)
        if os.path.exists(db_path):
            self._log_global(
                f"Failed to create database '{database_name}': already exists."
            )
            raise FileExistsError(f"Database '{database_name}' already exists.")

        os.makedirs(db_path)

        db_key = str(uuid.uuid4())

        encrypted_key = self._encrypt_key(db_key)

        with open(os.path.join(db_path, "database.key"), "wb") as key_file:
            key_file.write(encrypted_key)

        self._set_read_only(os.path.join(db_path, "database.key"))

        db_log_path = os.path.join(db_path, f"{database_name}.log")
        with open(db_log_path, "w") as log_file:
            log_file.write("")

        self._log_global(f"Database '{database_name}' created.")
        self._log_database(database_name, f"Database '{database_name}' created.")

    def delete_database(self, database_name):
        """Deletes a database directory and its log file."""
        db_path = os.path.join(self.databases_dir, database_name)
        if not os.path.exists(db_path):
            self._log_global(
                f"Failed to delete database '{database_name}': does not exist."
            )
            raise FileNotFoundError(f"Database '{database_name}' does not exist.")

        if self.active_connections.get(database_name):
            raise RuntimeError(
                f"Cannot delete database '{database_name}': Active connections exist."
            )

        for file in os.listdir(db_path):
            os.remove(os.path.join(db_path, file))
        os.rmdir(db_path)

        self.active_connections.pop(database_name, None)
        self.connection_queues.pop(database_name, None)

        self._log_global(f"Database '{database_name}' deleted.")

    def _set_read_only(self, file_path):
        """Sets the file to read-only, so it cannot be modified or deleted."""

        current_permissions = stat.S_IMODE(os.lstat(file_path).st_mode)

        os.chmod(file_path, current_permissions & ~stat.S_IWRITE)

    def _encrypt_key(self, db_key: str) -> bytes:
        """Encrypt the database key using AES-256."""

        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=os.urandom(16),
            iterations=100000,
            backend=default_backend(),
        )
        aes_key = kdf.derive(self.passphrase.encode())

        iv = os.urandom(16)
        cipher = Cipher(
            algorithms.AES(aes_key), modes.CBC(iv), backend=default_backend()
        )
        encryptor = cipher.encryptor()

        padder = padding.PKCS7(128).padder()
        padded_data = padder.update(db_key.encode()) + padder.finalize()

        encrypted_data = encryptor.update(padded_data) + encryptor.finalize()

        return iv + encrypted_data

    def _decrypt_key(self, encrypted_key: bytes) -> str:
        """Decrypt the database key using AES-256."""
        iv = encrypted_key[:16]
        encrypted_data = encrypted_key[16:]

        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=os.urandom(16),
            iterations=100000,
            backend=default_backend(),
        )
        aes_key = kdf.derive(self.passphrase.encode())

        cipher = Cipher(
            algorithms.AES(aes_key), modes.CBC(iv), backend=default_backend()
        )
        decryptor = cipher.decryptor()

        decrypted_data = decryptor.update(encrypted_data) + decryptor.finalize()

        unpadder = padding.PKCS7(128).unpadder()
        unpadded_data = unpadder.update(decrypted_data) + unpadder.finalize()

        return unpadded_data.decode()


class DB:
    def __init__(self, engine: DatabaseEngine, database_name: str):
        self.engine = engine
        self.database_name = database_name
        self._connected = False

        self.engine._initialize_database_if_needed(database_name)

    def connect(self):
        """Connects to the database."""
        if self._connected:
            raise RuntimeError(f"Already connected to database '{self.database_name}'.")

        self.engine._initialize_database_if_needed(self.database_name)
        queue = self.engine.connection_queues[self.database_name]
        queue.append(os.getpid())

        while queue[0] != os.getpid():
            time.sleep(0.1)

        self.engine.active_connections[self.database_name] = os.getpid()
        self._connected = True

        self.engine._log_global(
            f"Connected to database '{self.database_name}' (PID: {os.getpid()})."
        )
        self.engine._log_database(
            self.database_name, f"Connection established (PID: {os.getpid()})."
        )
        return self

    def disconnect(self):
        """Disconnects from the database."""
        if not self._connected:
            raise RuntimeError(f"Not connected to database '{self.database_name}'.")

        self.engine.active_connections[self.database_name] = None
        self.engine.connection_queues[self.database_name].popleft()
        self._connected = False

        self.engine._log_global(
            f"Disconnected from database '{self.database_name}' (PID: {os.getpid()})."
        )
        self.engine._log_database(
            self.database_name, f"Connection closed (PID: {os.getpid()})."
        )

    def is_connected(self):
        """Check if connected to the database."""
        return self._connected


def database(db_instance):
    """Decorator to automatically associate a table with a database instance."""

    def decorator(cls):
        cls._db = db_instance
        return cls

    return decorator


class Table:
    """A base class for table operations."""

    _db = None

    def __init__(self, **kwargs):
        """Initialize a row in the table with the provided column values."""
        for col, value in kwargs.items():
            setattr(self, col, value)

        if self._db is not None:
            self._create_table_file()

    @classmethod
    def _create_table_file(cls):
        """Create a file with the class name in the respective database's directory."""
        if cls._db is None:
            raise RuntimeError(f"No database provided for {cls.__name__}.")

        db_path = os.path.join(cls._db.engine.databases_dir, cls._db.database_name)
        if not os.path.exists(db_path):
            raise FileNotFoundError(
                f"Database '{cls._db.database_name}' does not exist."
            )

        table_file_path = os.path.join(db_path, f"{cls.__name__}.table")
        if not os.path.exists(table_file_path):
            with open(table_file_path, "w") as table_file:
                print(f"Created new table file: {table_file_path}")

    @classmethod
    def insert(cls, row, database=None):
        """Insert the current row into the table in the associated database."""
        if not cls._is_connected():
            raise RuntimeError("Cannot perform operations: Database is disconnected.")

        db = database if database else cls._db
        if db is None:
            raise RuntimeError(f"No database provided for {cls.__name__}.")

        row_dict = {col: getattr(row, col) for col in row.__dict__}

        encrypted_row = cls._encrypt_data(row_dict, db.database_name)

        table_file_path = os.path.join(
            db.engine.databases_dir, db.database_name, f"{cls.__name__}.table"
        )

        with open(table_file_path, "a") as table_file:
            table_file.write(encrypted_row + "\n")

        print(f"Inserted row into {cls.__name__}")

    @classmethod
    def search(cls, surpress_print=False, database=None, **conditions) -> List["Table"]:
        """Search for rows that match the given conditions in the associated database."""
        if not cls._is_connected():
            raise RuntimeError("Cannot perform operations: Database is disconnected.")

        db = database if database else cls._db
        if db is None:
            raise RuntimeError(f"No database provided for {cls.__name__}.")

        table_file_path = os.path.join(
            db.engine.databases_dir, db.database_name, f"{cls.__name__}.table"
        )

        rows = []
        with open(table_file_path, "r") as table_file:
            for line in table_file:

                decrypted_row = cls._decrypt_data(line.strip(), db.database_name)

                if all(decrypted_row.get(k) == v for k, v in conditions.items()):
                    rows.append(decrypted_row)

        if not surpress_print:
            print(f"Found rows matching conditions {conditions}: {rows}")
        return rows

    @classmethod
    def update(cls, conditions: Dict[str, Any], updates: Dict[str, Any], database=None):
        """Update rows in the table matching conditions with the provided updates."""
        if not cls._is_connected():
            raise RuntimeError("Cannot perform operations: Database is disconnected.")

        db = database if database else cls._db
        if db is None:
            raise RuntimeError(f"No database provided for {cls.__name__}.")

        rows_to_update = cls.search(database=db, surpress_print=True, **conditions)

        if not rows_to_update:
            return []

        updated_rows = []
        table_file_path = os.path.join(
            db.engine.databases_dir, db.database_name, f"{cls.__name__}.table"
        )

        with open(table_file_path, "r") as table_file:
            lines = table_file.readlines()

        with open(table_file_path, "w") as table_file:
            for line in lines:
                row_data = cls._decrypt_data(line.strip(), db.database_name)

                if all(row_data.get(k) == v for k, v in conditions.items()):
                    row_data.update(updates)
                    updated_rows.append(row_data)

                encrypted_row = cls._encrypt_data(row_data, db.database_name)
                table_file.write(encrypted_row + "\n")
        print(
            f"Updated {len(rows_to_update)} {'row'if len(rows_to_update)==1 else 'rows'} from {cls.__name__}"
        )
        return updated_rows

    @classmethod
    def delete(cls, database=None, **conditions):
        """Delete rows matching the given conditions."""
        if not cls._is_connected():
            raise RuntimeError("Cannot perform operations: Database is disconnected.")

        db = database if database else cls._db
        if db is None:
            raise RuntimeError(f"No database provided for {cls.__name__}.")

        #
        rows_to_delete = cls.search(database=db, surpress_print=True, **conditions)

        if not rows_to_delete:
            return []

        deleted_rows = []
        table_file_path = os.path.join(
            db.engine.databases_dir, db.database_name, f"{cls.__name__}.table"
        )

        with open(table_file_path, "r") as table_file:
            lines = table_file.readlines()

        with open(table_file_path, "w") as table_file:
            for line in lines:
                row_data = cls._decrypt_data(line.strip(), db.database_name)

                if all(row_data.get(k) == v for k, v in conditions.items()):
                    deleted_rows.append(row_data)
                else:

                    encrypted_row = cls._encrypt_data(row_data, db.database_name)
                    table_file.write(encrypted_row + "\n")
        print(
            f"Deleted {len(rows_to_delete)} {'row'if len(rows_to_delete)==1 else 'rows'} from {cls.__name__}"
        )
        return deleted_rows

    @classmethod
    def _is_connected(cls):
        """Check if the current database connection is active."""
        return cls._db is not None and cls._db.is_connected()

    @classmethod
    def _get_decryption_key(cls, database_name: str) -> bytes:
        """Retrieve and decrypt the database's encryption key."""
        db_path = os.path.join(cls._db.engine.databases_dir, database_name)
        with open(os.path.join(db_path, "database.key"), "rb") as key_file:
            encrypted_key = key_file.read()

        decrypted_key = cls._decrypt_key(encrypted_key)
        return decrypted_key

    @classmethod
    def _decrypt_key(cls, encrypted_key: bytes) -> bytes:
        """Decrypt the encrypted database key and ensure it's of valid length."""

        digest = hashes.Hash(hashes.SHA256(), backend=default_backend())
        digest.update(encrypted_key)
        decrypted_key = digest.finalize()

        return decrypted_key

    @classmethod
    def _encrypt_data(cls, data: dict, database_name: str) -> str:
        """Encrypt the row data using the database's key."""
        decryption_key = cls._get_decryption_key(database_name)

        cipher = Cipher(
            algorithms.AES(decryption_key),
            modes.CBC(decryption_key[:16]),
            backend=default_backend(),
        )
        encryptor = cipher.encryptor()

        json_data = json.dumps(data).encode()
        padder = padding.PKCS7(128).padder()
        padded_data = padder.update(json_data) + padder.finalize()

        encrypted_data = encryptor.update(padded_data) + encryptor.finalize()

        return encrypted_data.hex()

    @classmethod
    def _decrypt_data(cls, encrypted_data: str, database_name: str) -> dict:
        """Decrypt the encrypted row data."""
        decryption_key = cls._get_decryption_key(database_name)

        cipher = Cipher(
            algorithms.AES(decryption_key),
            modes.CBC(decryption_key[:16]),
            backend=default_backend(),
        )
        decryptor = cipher.decryptor()

        encrypted_data = bytes.fromhex(encrypted_data)

        decrypted_data = decryptor.update(encrypted_data) + decryptor.finalize()

        unpadder = padding.PKCS7(128).unpadder()
        unpadded_data = unpadder.update(decrypted_data) + unpadder.finalize()

        return json.loads(unpadded_data.decode())

    def __repr__(self):
        """String representation of a row."""
        return (
            f"<{self.__class__.__name__} "
            + " ".join(f"{k}={v}" for k, v in self.__dict__.items())
            + ">"
        )
