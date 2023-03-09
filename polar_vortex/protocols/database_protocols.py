from typing import (
                    Protocol,
                    Any,Set,
                    Optional,
                    NamedTuple,
                    List, Dict,
                    TypeAlias
                    )
from queue import PriorityQueue

class DatabasePtr(NamedTuple):
    '''
    DatabaseConnections provides a uniform method to pass input to all the functions of the protocol. 
    The design pattern is Protocol defines actions, the Interface provides a uniform function interface.
    '''
    database:Optional[str] = None
    key: Optional[str] = None

class DataOperationPtr(NamedTuple):
    indexs:Optional[PriorityQueue[int] or int] = None
    columns:Optional[Set[str] or NamedTuple or Dict] = None

Datum: TypeAlias = NamedTuple
Data: TypeAlias = List
class DatabaseProtocol(Protocol):

    '''
    DatabaseProtocol defines the actions that can be performed on a database.
    '''
    
    db_ptr: DatabasePtr
    values: Data[Datum]
    
    def detect_change(self,) -> bool:
        """
        detect_change in underlying database. Useful for reducing reads.
        Returns:
            bool: if the underlying database changed,
                    return True
        """        
        ...
        
    def save(self,)-> Any:
        '''
        Commits a database to disk. This could a bulk save.
        '''
        ...
    
    def verify_datum(datum:Datum) -> bool:
        """
        verifies data before adding
        Args:
            datum (Datum): the piece of data. Just checks column names match ATM.

        Returns:
            bool: if the columns match
        """ 
        ...    

    def upsert(self,
            data: Data or Datum,
            op_ptrs:Optional[DataOperationPtr]
            ) -> bool:
        '''
        set a value in the database with a key and possible index
        '''
        ...

    def get(self,
            op_ptrs:Optional[DataOperationPtr]
            ) -> Any:
        '''
        get a value from the database with a key and possible index
        '''
        ...

    def delete(self, 
               op_ptrs:Optional[DataOperationPtr],
               locked:bool=True,
               ) -> bool:
        '''
        delete a value from the database with a key and possible index
        '''
        ...
        
    def all(self, ) -> Any:
        '''
        Get all values from the database with a key
        '''
        ...
   
    def contains(self,
                op_ptrs:Optional[DataOperationPtr],
                 )-> bool:
        '''
        Checks if databases exist or if a key is a database or if column is in key
        '''
        ... 
       
        
    def is_in(self,
              data:Data or Datum,
                 )-> bool:
        '''
        checks if a value is in db object
        '''
        ...
        