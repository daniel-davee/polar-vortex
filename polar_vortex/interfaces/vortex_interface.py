from os import remove
from pathlib import Path
from typing import List, Any, Dict, Iterator, Set
from polar_vortex.interfaces.log_interface import logger
from polar_vortex.protocols.database_protocols import (
                                            DatabasePtr, 
                                            DataOperationPtr, 
                                            DataPoint, Data,
                                            Fact)
import shelve

db_path = Path(__file__).parent / 'vortex_databases'
# db_path = Path().cwd() / 'database/vortex_databases'
if not db_path.exists(): db_path.mkdir()


class VortexInterface():
    
    '''
    VortexInterface provides a uniform interface to a shelve database.
    '''
    
    def __init__(self, db_ptr:DatabasePtr,data_point:DataPoint=None) -> None:
        self.values:Dict[Fact or str, DatabasePtr] = {}
        self._del_keys:Set[Fact or str] = set()
        self.db_ptr = db_ptr 
        match(data_point):
            case None:
                self.values = self.get()
            case _:
                self.upsert(data_point)
        # Shelve can create mutiple files with dame stem
        if not list(self.db_files):self.save()

    def __bool__(self,) -> bool:
        return bool(self.values)
    
    def is_empty(self,) -> bool:
        return not self

    @property
    def db_files(self,) -> Iterator[Path]:
        return filter(lambda f: f.stem == self.db_ptr.database, db_path.iterdir())
    
    def detect_change(self,) -> bool:
        return True

    def verify_datum(datum:DataPoint) -> bool:
        '''
        Not implemented, return True for functionality
        '''
        return True

    def save(self,) -> bool:
        with shelve.open(str(db_path/self.db_ptr.database)) as db:
            for key in self._del_keys: del db[key]
            for key,value in self.values.items():db[key] = value
            self._del_keys = set()
    
    def upsert(self, data: Data or  DataPoint) -> bool:
        logger.debug(f'{self.db_ptr=}')
        match(self.db_ptr):
            case DatabasePtr(_, None):
                if not isinstance(data,dict): raise TypeError('data must be a dict')
                self.values.update(data)
            case DatabasePtr(_, key): self.values[key] = data
            case _: raise Exception(f'{self.db_ptr=} is not a valid DatabasePtr')
        return True
    
    def get(self,) -> DataPoint:
        if not list(self.db_files): self.save()
        with shelve.open(str(db_path/self.db_ptr.database)) as db:
            match(self.db_ptr):
                case DatabasePtr(_, None): self.values = dict(db).update(self.values) or {} 
                case DatabasePtr(_, key) if self.is_empty(): self.values = {}
                case DatabasePtr(_, key): self.values[key] = self.values[key] or db[key]
        return self.values
            
    def all(self,) -> List[Dict[str,Any]]:
        return self.get()

    def delete(self,locked:bool=True) -> bool:
        if locked: return False
        self._del_keys.union(set(self.values.keys()))
        self.values = {}
        return True

    def keys(self,) -> Set[str]:
        return self.values.keys()
     
    def __contains__(self,key:str) -> bool:
        return key in self.keys()
    
    def contains(self,key:str ) -> bool:
        return key in self
                
    def is_in(self,data_point:DataPoint) -> bool:
        return any(value == data_point for value in self.values.values())