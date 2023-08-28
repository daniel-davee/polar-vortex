from os import remove
from pathlib import Path
from typing import (
                    Optional,
                    Any, 
                    Iterator, 
                    Union,
                    )
from candy.candy_wrapper import Wrapper
from polar_vortex.interfaces.log_interface import logger
from polar_vortex.protocols.database_protocols import (
                                            DatabasePtr, 
                                            DataOperationPtr, 
                                            DataPoint, Data,
                                            Key)
import shelve


vortex_path = Path(__file__).parent / 'vortex_databases'
# db_path = Path().cwd() / 'database/vortex_databases'
if not vortex_path.exists(): vortex_path.mkdir()


class VortexInterface():
    
    '''
    VortexInterface provides a uniform interface to a shelve database.
    '''
    
    def __init__(self, db_ptr:DatabasePtr,data_point:Optional[DataPoint]=None) -> None:
        """
        Initilizes interface to shelve. If key is none entire db is retrive, 
        Args:
            db_ptr (DatabasePtr): Stores the name of the database and the key to data.
            If the key is None then entire db is accessable as dictionary.
            data_point (DataPoint, optional): If not None the DataPoint is upserted else 
            the values are taken from db. Defaults to None.
        """        

        self.values:dict[Key, DataPoint] = {}
        self._del_keys:set[Key] = set()
        self.db_ptr = db_ptr
        self.values = {}
        match(data_point):
            case None:
                self.values = self.get()
            case _:
                self.upsert(data_point)
        # Shelve can create mutiple files with same stem
        if not list(self.db_files):self.save()

    def __bool__(self,) -> bool:
        """
        Allow for testing if a db is or key is empty
        Returns:
            bool: if db_ptr.key is None
                return false is self.values is empty
                if key is not none then return False if key is not values
                or points to falsy data. 
        """        
        match(self.db_ptr):
            case DatabasePtr(_, None):
                return bool(self())
            case DatabasePtr(_, key):
                return bool(self()[key]) if key in self() else False
    
    def is_empty(self,) -> bool:
        return not self

    @property
    def db_files(self,) -> Iterator[Path]:
        """An iterator pointing to the path of all the db files.
        Sometimes shelve creats mutiple db files with same stem.

        Returns:
            _type_: _description_

        Yields:
            Iterator[Path]: A list of paths to the db with same stem as
            db_ptr.database 
        """        
        return filter(lambda f: f.stem == self.db_ptr.database, vortex_path.iterdir())
    
    def detect_change(self,) -> bool:
        '''
        Not implemented, return True for functionality
        '''
        return True

    def verify_datum(datum:DataPoint) -> bool:
        '''
        Not implemented, return True for functionality
        '''
        return True

    def save(self,) -> bool:
        """save:
        saves all the upserts and deletes to the db.

        Returns:
            bool: True if ok
        """        
        with shelve.open(str(vortex_path/self.db_ptr.database)) as db:
            for key in self._del_keys: del db[key]
            for key,value in self.values.items():db[key] = value
            self._del_keys = set()
        return True
    
    def upsert(self, data: Union[Data,DataPoint]) -> bool:
        """upsert: 
            This method is lazy, save will need to be called
            to write changes to DB. If no key is provided then 
            data must be a dict.
        Args:
            data (Union[Data,DataPoint]): The data that will be upserted

        Raises:
            TypeError: This happens db ptr has no key, and a non dict gets
                passed as data. The reason is no key implies a full db update,
                which needs a dict.
            Exception: If the db ptr is  maleformed, you'll see this.

        Returns:
            bool: True if everything is ok
        """        
        logger.debug(f'{self.db_ptr=}')
        match(self.db_ptr):
            case DatabasePtr(_, None):
                if not isinstance(data,dict): raise TypeError('data must be a dict')
                self().update(data)
            case DatabasePtr(_, key): self()[key] = data
            case _: raise Exception(f'{self.db_ptr=} is not a valid DatabasePtr')
        return True
    
    def get(self,) -> dict[Key,DataPoint]:
        """get:
        Retrives either the enitre db or a single key.
        If key is None then db is loaded into values.
        If key not None then only the key value is 
        loaded into values.

        Raises:
            Exception: If db_ptr is malformed.

        Returns:
            DataPoint: All the values of the interface a returned.
        """        
        if not list(self.db_files): self.save()
        with shelve.open(str(vortex_path/self.db_ptr.database)) as db:
            
            match(self.db_ptr):
                case DatabasePtr(_, None): self().update(dict(db)) 
                case DatabasePtr(_, key): self.values[key] = db[key] if key in db else {}
                case _: raise Exception(f'{self.db_ptr=} is not a valid DatabasePtr')
        return self()
            
    def all(self,) -> list[dict[str,Any]]:
        return self.get()

    def delete(self,locked:bool=True) -> bool:
        if locked: return False
        self._del_keys.union(set(self().keys()))
        self.values = {}
        return True

    def keys(self,) -> set[str]:
        return self().keys()
     
    def __contains__(self,key:str) -> bool:
        return key in self.keys()
    
    def contains(self,key:str ) -> bool:
        return key in self
                
    def is_in(self,data_point:DataPoint) -> bool:
        return any(value == data_point for value in self().values())
    
    def __call__(self,) -> DataPoint:
        return self.values
    
    def wrap(self) -> Wrapper[dict[Key,DataPoint]]:
        return Wrapper(self())