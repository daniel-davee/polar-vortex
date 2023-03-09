from .log_interface import logger
from ..protocols.database_protocols import  (
                                                DatabasePtr,
                                                DataOperationPtr,
                                                Data,Datum
                                             )
from pathlib import Path
from shutil import rmtree
from os import remove
from typing import List, Tuple, Any 
from polars import (DataFrame, 
                    LazyFrame,
                    scan_parquet, 
                    scan_csv,
                    concat,
                    col, 
                    Expr,
                    )
from dataclasses import dataclass, field
from queue import PriorityQueue
# db_path = Path('interfaces/polars_databases')
db_path = Path(__file__).parent / 'polars_databases'
if not db_path.exists(): db_path.mkdir()
logger.set_log_level('debug')
class PolarsInterface():
    '''
    This is a lazy PolarsInterface to a database built off of parquet files.
    It is an implementation of the DatabaseProtocol.
    example usage:
    '''

    def __init__(self,db_ptr:DatabasePtr,file_type:str='csv'):
        database, key = self.db_ptr = db_ptr
        if not (db_file:= db_path/database).exists():
            db_file.mkdir()
        self.key_file:Path = db_file/f'{key}.{file_type}'
        self.lazyframe:LazyFrame = LazyFrame() if self.is_empty else self.scan
        self.values:Data[Datum] = list()

    def __call__(self,) -> LazyFrame:
        return self.lazyframe

    def __bool__(self)->bool:
        return self.key_file.exists()

    @property
    def file_type(self):
        return self.key_file.suffix.lstrip('.')

    @property
    def scan(self) -> LazyFrame:
        match(self.file_type):
            case 'parquet': return scan_parquet(self.key_file)
            case 'csv': return scan_csv(self.key_file)
            case ns: raise Exception(f'{ns} is not supported for scan')
    
    @property 
    def dataframe(self)->DataFrame:
        return self().collect()
   
    @property 
    def is_empty(self)->bool:
        return not self
   
    def detect_change(self) -> bool:
        nl = '\n'
        return self.describe_optimized_plan() == \
                f'  CSV SCAN {self.key_file.name}{nl}  PROJECT */{len(self.columns)} COLUMNS{nl}'

    def save(self,has_header:bool=False)->LazyFrame:
        
        """
            Save the database to disk.
        Returns:
            LazyFrame: returns a new lazyframe pointing to the updated file.
        """
        df = DataFrame(self.values)        
        match(self.file_type):
            case 'parquet':
                self.dataframe.vstack(df).write_parquet(self.key_file)
            case 'csv' if self.is_empty or not self.detect_change:
                with open(self.key_file,mode='ab') as f:
                    df.write_csv(f,has_header=has_header)
            case 'csv':self.dataframe.vstack(df).write_csv(self.key_file)
            case ns: raise Exception(f'{ns} is not supported for scan')
        self.lazyframe = self.scan
        return self()

    def verify_datum(self,datum:Datum) -> bool:
        """
        verifies data before adding
        Args:
            datum (Datum): the piece of data. Just checks column names match ATM.

        Returns:
            bool: if the columns match
        """
        logger.debug(f'{self().columns=}')
        return datum._fields == tuple(self().columns)
    
    
    def upsert(self, 
               values:Data or Datum, 
               op_ptrs:DataOperationPtr = None) -> bool:
        """
        Upserts values into op_ptr location 
        Args:
            op_ptr (DataOperationPtrs): where the data is being upserted
            values (Data): the data being uploaded.

        Raises:
            Exception: _description_

        Returns:
            bool: _description_
        """        
        if not isinstance(values, Data):values = [values]
        datum: Datum = values[0]
        logger.debug(f'{values=}, {self.key_file}')
        match(op_ptrs):
            case _ if self.is_empty:
                self.values += values
                self.save(has_header=True)
            case _ if not self.verify_datum(datum):
                raise TypeError(f'{datum._fields} mismatch{self().columns}')                            
            case None: self.values += values
            case DataOperationPtr(indexs,None):
                lf:LazyFrame = self()
                indexs = indexs.queue
                dindexs = [di_1 - di_0 for di_0, di_1 in zip(indexs[:-1],indexs[1:])]
                lazy_ops:List[Tuple[LazyFrame]] = [(lf.slice(0,indexs[0]), DataFrame(values[0]))]
                lazy_ops += [(lf.slice(i+1,di),DataFrame(v).lazy()) 
                            for di,i,v in zip(dindexs,indexs,values)]
                
                cat_lf: List[LazyFrame] = [lf.slice(indexs[-1])]
                for op in lazy_ops:
                    cat_lf += list(op)
                cat_lf = cat_lf[1:] + cat_lf[0]
                self.lazyframe = concat(cat_lf)
                
            case DataOperationPtr(indexs, columns):
                logger.warning('''Column base operations have not been implemented. 
                                  Please dont define columns''')
                return False
        return True
            
    def get(self, op_ptr:DataOperationPtr,indexed:bool=False) -> LazyFrame:
        
        """
            Get values from the database. If a database and 
            key are provided, the entire table is returned. If a database, key,
            and index are provided, only the value at that index is returned.
        Returns:
            returns the lazyframe of the query
        """        
        
        get = self.all().with_row_count('index') if indexed else self.all()
        match(op_ptr):
            case DataOperationPtr(_, _, None): return get
            case DataOperationPtr( _,_, index):
                get = get.with_row_count('row')\
                         .filter(col('row') == index)\
                         .drop('row')
                return get
    
    def delete(self,ptr:DataOperationPtr,locked:bool=True,) -> bool:
        
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
            case DataOperationPtr(database, None, None): rmtree(db_path/database)
            case DataOperationPtr(database, key, None): remove(db_path/database/key)
            case DataOperationPtr( database,key, index) :
                lf = lf.filter(col('row') != index)
                self.lazyframe = lf.drop('row')        
        return True
    
    def all(self,ptr:DataOperationPtr=None) -> LazyFrame :
        return self.lazyframe

    def contains(self, column:str='', ptr:DataOperationPtr=None) -> bool:
        match(column,ptr):
            case column, None: return column in self.lazyframe.columns()
            case DataOperationPtr(database,None,None), '':
                return (db_path/database).exists()
            case DataOperationPtr(database,key,None), '':
                return (db_path/database).exists()
    
    def is_in(self, value:Tuple[Any],) -> bool:
        return value in set(self.all().collect().to_dict().values())
