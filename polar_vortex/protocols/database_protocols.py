from typing import (
                    Protocol,
                    Any,
                    Optional,
                    NamedTuple,
                    )
from pathlib import Path
from polar_vortex.interfaces.log_interface import logger

db_path = Path(__file__).parent

class DatabasePtr(NamedTuple):
    '''
    DatabaseConnections provides a uniform method to pass input to all the functions of the protocol. 
    The design pattern is Protocol defines actions, the Interface provides a uniform function interface.
    '''
    database:Optional[str] = None
    key: Optional[str] = None
    index:Optional[int] = None

class DatabaseProtocol(Protocol):

    '''
    DatabaseProtocol defines the actions that can be performed on a database.
    '''    
    
    def get(self,
            connection:Optional[DatabasePtr]
            ) -> Any:
        '''
        get a value from the database with a key and possible index
        '''
        ...

    def upsert(self, 
            connection:Optional[DatabasePtr]
            ) -> bool:
        '''
        set a value in the database with a key and possible index
        '''
        ...

    def delete(self, 
               connection:Optional[DatabasePtr],
               locked:bool=True,
               ) -> bool:
        '''
        delete a value from the database with a key and possible index
        '''
        ...
        
    def all(self, 
            connection:Optional[DatabasePtr],
            ) -> Any:
        '''
        Get all values from the database with a key
        '''
        ...
   
    def contains(self,
                connection:Optional[DatabasePtr],
                 )-> bool:
        '''
        Checks if a key is a database
        '''
        ... 
       
        
    def is_in(self,
              connection:Optional[DatabasePtr],
                 )-> bool:
        '''
        checks if a value is in db object
        '''
        ...
        
        
    def save(self,
             connection:Optional[DatabasePtr],
               )-> bool:
        '''
        Commits a database to disk. This could a bulk save.
        '''
        ...