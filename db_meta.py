import os
import datetime

class DBEngineMeta(type):
    def __new__(cls, name, bases, dct):
        
        log_file_path = os.path.join('db_engine', 'global', 'database_actions.log')
        global_dir = os.path.join('db_engine', 'global')
        
        
        os.makedirs(global_dir, exist_ok=True)
        if not os.path.exists(log_file_path):
            open(log_file_path, 'w').close()

        
        original_init = dct.get('__init__')

        def init_method(self, *args, **kwargs):
            if original_init:
                original_init(self, *args, **kwargs)
        
        dct['__init__'] = init_method

        
        if 'create_database' in dct:
            original_create_method = dct['create_database']

            def log_create_method(self, *args, **kwargs):
                try:
                    
                    result = original_create_method(self, *args, **kwargs)
                    
                    
                    cls._log_method(log_file_path, 'SUCCESS', original_create_method, args)
                    return result
                except FileExistsError as e:
                    
                    cls._log_method(log_file_path, 'ERROR', original_create_method, args, str(e))
                    raise e

            dct['create_database'] = log_create_method

        
        if 'delete_database' in dct:
            original_delete_method = dct['delete_database']

            def log_delete_method(self, *args, **kwargs):
                try:
                    
                    result = original_delete_method(self, *args, **kwargs)
                    
                    
                    cls._log_method(log_file_path, 'SUCCESS', original_delete_method, args)
                    return result
                except FileNotFoundError as e:
                    
                    cls._log_method(log_file_path, 'ERROR', original_delete_method, args, str(e))
                    raise e

            dct['delete_database'] = log_delete_method

        
        return super().__new__(cls, name, bases, dct)

    @staticmethod
    def _log_method(log_file_path, status, method, args, error_message=None):
        """Helper method to log database operations"""
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        func_name = method.__name__
        func_args = ', '.join([str(arg) for arg in args])
        log_message = f"[{timestamp}] {status} - Function: {func_name} | Args: ({func_args})"
        if error_message:
            log_message += f" | Error: {error_message}"
        
        
        with open(log_file_path, 'a') as log_file:
            log_file.write(log_message + "\n")
        print(f"Logged: {log_message}")

