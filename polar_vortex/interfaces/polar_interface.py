from .log_interface import logger
from ..protocols.database_protocols import DatabaseConnection
from pathlib import Path
from typing import List, Tuple, Any, Dict
from polars import (DataFrame, 
                    LazyFrame,
                    concat, 
                    scan_parquet, 
                    col, 
                    Expr,
                    )

db_path = Path('databases/polars_databases')
# db_path = Path(__file__).parent / 'polars_databases'

class PolarsInterface():
    '''
    This is a lazy PolarsInterface to a database built off of parquet files.
    It is an implementation of the DatabaseProtocol.
    '''
    
    key_file:Path
    lazyframe:LazyFrame
    
    def __init__(self,connection:DatabaseConnection)->LazyFrame:
        database, key = connection.database, connection.key
        if not (db_file:=(db_path/database)).exists():
            db_file.mkdir()
        if not (key_file := (db_file/f'{key}.parquet')).exists():
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
        self.dataframe.write_parquet(self.key_file)
        self.lazyframe = scan_parquet(self.key_file)
        return self.lazyframe
    
    def get(self, connection:DatabaseConnection,indexed:bool=False) -> LazyFrame:
        get = self.all().with_row_count('index') if indexed else self.all()
        match(connection):
            case DatabaseConnection(_, _, None, None): return get
            case DatabaseConnection(_, _, value, None):
                filter_map = map(lambda key: (col(key),value[key]) , value)
                for c,v in filter_map:
                    get = get.filter(c==v)
                return get
            case DatabaseConnection(_,_, _, index):
                get = get.with_row_count('row')\
                         .filter(col('row') == index)\
                         .drop('row')
                return get
    
    def delete(self,connection:DatabaseConnection,locked:bool=True,) -> bool:
        if locked:
            logger.info('delete is locked by default')
            return False
        match(connection):
            case DatabaseConnection(_, _, None, None): return False
            case DatabaseConnection(_, _, value, _):
                indexs = list(self.get(DatabaseConnection(value=value))\
                                 .with_row_count('row')\
                                 .collect()['row'])
            case DatabaseConnection(_,_, _, index):
                indexs = index if isinstance(index, list) else [index]
        lf = self.lazyframe.with_row_count('row')          
        for index in indexs:
            lf = lf.filter(col('row') != index)
        self.lazyframe = lf.drop('row')        
        return True
    
    def all(self,) -> LazyFrame :
        return self.lazyframe

    def contains(self, key: str) -> bool:
        return key in self.lazyframe.columns()
    
    def is_in(self, value:Tuple[Any],) -> bool:
        return value in set(self.all().collect().to_dict().values())
