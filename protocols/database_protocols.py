from typing import (
                    Protocol,
                    Any,
                    Optional,
                    )
from dataclasses import dataclass, astuple
from pathlib import Path

db_path = Path(__file__).parent

@dataclass
class DatabaseConnection():
    '''
    DatabaseConnections provides a uniform method to pass input to all the functions of the protocol. 
    The design pattern is Protocol defines actions, the Interface provides a uniform function interface.
    '''
    database:Optional[str] = None
    key: Optional[str] = None
    value: Optional[str] = None
    index:Optional[int] = None
    
    def __iter__(self):
        return iter(astuple(self))

class DatabaseProtocol(Protocol):

    '''
    DatabaseProtocol defines the actions that can be performed on a database.
    '''    
    
    def get(self,
            connection:Optional[DatabaseConnection]
            ) -> Any:
        '''
        get a value from the database with a key and possible index
        '''
        ...

    def upsert(self, 
            connection:Optional[DatabaseConnection]
            ) -> bool:
        '''
        set a value in the database with a key and possible index
        '''
        ...

    def delete(self, 
               connection:Optional[DatabaseConnection],
               locked:bool=True,
               ) -> bool:
        '''
        delete a value from the database with a key and possible index
        '''
        ...
        
    def all(self, 
            connection:Optional[DatabaseConnection],
            ) -> Any:
        '''
        Get all values from the database with a key
        '''
        ...
   
    def contains(self,
                connection:Optional[DatabaseConnection],
                 )-> bool:
        '''
        Checks if a key is a database
        '''
        ... 
       
        
    def is_in(self,
              connection:Optional[DatabaseConnection],
                 )-> bool:
        '''
        checks if a value is in db object
        '''
        ...
        
        
    def save(self,
             connection:Optional[DatabaseConnection],
               )-> bool:
        '''
        Commits a database to disk. This could a bulk save.
        '''
        ...