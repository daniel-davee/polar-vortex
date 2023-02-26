from .log_interface import logger
from ..protocols.database_protocols import  DatabasePtr
from pathlib import Path
from shutil import rmtree
from os import remove
from typing import List, Tuple, Any, Dict
from polars import (DataFrame, 
                    LazyFrame,
                    scan_parquet, 
                    col, 
                    Expr,
                    )

# db_path = Path('interfaces/polars_databases')
db_path = Path(__file__).parent / 'polars_databases'

class PolarsInterface():
    '''
    This is a lazy PolarsInterface to a database built off of parquet files.
    It is an implementation of the DatabaseProtocol.
    '''
    
    key_file:Path
    lazyframe:LazyFrame
    
    def __init__(self,ptr:DatabasePtr=None,
                 file_type:str = 'parquet')->LazyFrame:
        ptr = ptr or DatabasePtr()
        database, key, _  = ptr
        if not (db_file:=(db_path/database)).exists():
            db_file.mkdir()
        if not (key_file := (db_file/f'{key}.{file_type}')).exists():
            DataFrame({'default':[]}).write_parquet(key_file)
        self.key_file:Path = key_file
        self.lazyframe:LazyFrame =scan_parquet(key_file)
    
    def first(self)->LazyFrame:
        return self.lazyframe.first()
    
    def last(self)->LazyFrame:
        return self.lazyframe.last()
    
    def filter(self,expr:Expr)->LazyFrame:
        return self.lazyframe.filter(expr)
    
    def select(self,columns:List[str] or Expr)->LazyFrame:
       return self.lazyframe.select(columns)
   
    @property 
    def dataframe(self)->DataFrame:
        return self.lazyframe.collect()
    
    def is_empty(self)->bool:
        return self.lazyframe.collect().is_empty() 
    
    def upsert(self, values: List[Dict[str,Any]]) -> bool:
        
        """
            Upsert a value into the database lazily.
        Returns:
            bool: True if successful
        """        
        
        if not isinstance(values, list):values = [values]
        logger.debug(f'{values=}, {self.key_file}')
        if self.is_empty():
            DataFrame(values).write_parquet(self.key_file)
            self.lazyframe = scan_parquet(self.key_file)
            return True
        self.lazyframe = self.lazyframe.collect()\
                             .vstack(DataFrame(values)).lazy()
        return True
    
    def save(self)->LazyFrame:
        
        """
            Save the database to disk.
        Returns:
            bool: True if successful
        """        
        
        self.dataframe.write_parquet(self.key_file)
        self.lazyframe = scan_parquet(self.key_file)
        return self.lazyframe
    
    def get(self, ptr:DatabasePtr,indexed:bool=False) -> LazyFrame:
        
        """
            Get values from the database. If a database and 
            key are provided, the entire table is returned. If a database, key,
            and index are provided, only the value at that index is returned.
        Returns:
            returns the lazyframe of the query
        """        
        
        get = self.all().with_row_count('index') if indexed else self.all()
        match(ptr):
            case DatabasePtr(_, _, None): return get
            case DatabasePtr( _,_, index):
                get = get.with_row_count('row')\
                         .filter(col('row') == index)\
                         .drop('row')
                return get
    
    def delete(self,ptr:DatabasePtr,locked:bool=True,) -> bool:
        
        """
            Delete a value from the database. If only a database is provided,
            then whole folder is deleted. If a database and key are provided,
            then the entire table is deleted. If a database, key, and index
            then the value at that index is deleted.
        Returns:
            bool: True if successful
        """        
        
        if locked:
            logger.info('delete is locked by default')
            return False
        match(ptr):
            case DatabasePtr(database, None, None): rmtree(db_path/database)
            case DatabasePtr(database, key, None): remove(db_path/database/key)
            case DatabasePtr( database,key, index) :
                lf = lf.filter(col('row') != index)
                self.lazyframe = lf.drop('row')        
        return True
    
    def all(self,) -> LazyFrame :
        return self.lazyframe

    def contains(self, key: str) -> bool:
        return key in self.lazyframe.columns()
    
    def is_in(self, value:Tuple[Any],) -> bool:
        return value in set(self.all().collect().to_dict().values())
