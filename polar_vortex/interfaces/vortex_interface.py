from .log_interface import logger
from pathlib import Path
from typing import List, Any, Dict
from ..protocols.database_protocols import DatabaseConnection
import shelve

db_path = Path(__file__).parent / 'vortex_databases'
# db_path = Path().cwd() / 'database/vortex_databases'
if not db_path.exists(): db_path.mkdir()


class VortexInterface():
    
    '''
    VortexInterface provides a uniform interface to a shelve database.
    '''
    
    def __init__(self, connection:DatabaseConnection=None) -> None:
        self.connection = connection or DatabaseConnection()
        self._data = None

    @classmethod
    @property
    def databases(cls,) -> List[DatabaseConnection]:
        return map(DatabaseConnection,
                    map(lambda _: _.stem, 
                    filter(lambda _:_.suffix == '.dir',
                            db_path.iterdir())))
    @property
    @classmethod
    def all(cls,) -> List[Dict[str,Any]]:
        return cls.get()

    @classmethod
    def keys(cls, connection:DatabaseConnection=None) -> List[str]:
        database = connection.database
        match(database):
            case None: return map(cls.keys,cls.databases)
            case str():
                with open(db_path/database).as_posix() as db:
                    return list(db.keys())
            case _: return cls.keys(DatabaseConnection(str(database)))
    
    @property
    def save(self,) -> bool:
        self.upsert(self.connection)

    @classmethod
    def upsert(cls, connection:DatabaseConnection) -> bool:
        database, key, value, index = connection
        key = str(key)
        if not all([database, key, value]):
            raise ValueError('database, key, and value are required')
        with shelve.open((db_path/database).as_posix()) as db:
            match(index):
                case None: db[key] = value
                case _: db[key][index] = value
        return True

    @property
    def data(self) -> Any:
        if self._data is None:
            self._data = self.get(self.connection)
        return self._data

    @classmethod 
    def get(cls, connection:DatabaseConnection=None) -> Any:
        database, key, _, index = connection = connection or DatabaseConnection()
        match(connection):
            case DatabaseConnection(None,_,_,_): return map(cls.get, cls.databases)
            case DatabaseConnection(database,None,_,_):
                with shelve.open((db_path/database).as_posix()) as db:
                    return dict(db)
            case DatabaseConnection(database,key,_,None):
                with shelve.open((db_path/database).as_posix()) as db:
                    return db[key]
            case DatabaseConnection(database,key,_,index):
                with shelve.open((db_path/database).as_posix()) as db:
                    return db[key][index]

    def delete(self, 
               locked:bool=True,
               database_locked:bool=True,
               ) -> bool:
        if locked: return False
        connection = self.connection
        match(connection):
            case DatabaseConnection(None, _, _, _): raise ValueError('database is required')
            case DatabaseConnection(_, None, _, _) if database_locked: return False
            case DatabaseConnection(database, None, _, _):(db_path/database).unlink()
            case DatabaseConnection(database, key, _, index):
                with shelve.open((db_path/database).as_posix()) as db:
                    match(index):
                        case None:del db[key]
                        case i: del db[key][i]

    @classmethod
    def contains(cls,connection:DatabaseConnection=None) -> bool:
        database, key, _, _ = connection or DatabaseConnection()
        match(database):
            case None: return any(map(cls.contains, cls.databases))
            case str(): 
                with shelve.open((db_path/database).as_posix()) as db:
                    return key in db
                
    @classmethod
    def is_in(cls,connection:DatabaseConnection=None) -> bool:
        get = cls.get(connection)
        match(get):
            case list(): return any(map(cls.is_in, get))
            case dict(): connection.value in get.values()
            case _: connection.value == get or connection.value in get