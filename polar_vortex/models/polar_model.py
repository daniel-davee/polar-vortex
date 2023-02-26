from typing import NamedTuple, Any, Tuple, Dict, Set, List, Iterable
from dataclasses import dataclass, field, fields, astuple, asdict
from pathlib import Path
from interfaces.log_interface import logger
from protocols.database_protocols import DatabaseProtocol, DatabasePtr
from interfaces.polar_interface import (PolarsInterface, 
                                        LazyFrame,
                                        col,
                                        )

# root = Path(__file__).parent.parent
root = Path()
databases = root / 'databases'

class PolarModel():
    '''
    This is the base model for all models. 
    Models allow easy mapping of data to the underlying storage(ORM).
    These models are mapped to polars dataframes,stored parquet files. 
    The PolarsInterface is a lazy interface to the parquet files,
    which controls the data view.
    '''
    connection:DatabasePtr = DatabasePtr(database='default', key='default')
    _dbi = PolarsInterface
    dbi:DatabaseProtocol = _dbi(connection)

    @classmethod    
    def connect(cls, connection:DatabasePtr=None) -> bool:
        connection = cls.connection = connection or cls.connection
        cls.dbi = cls._dbi(connection)
        return True
    
    def __iter__(self):
        return iter(astuple(self))
    
    @property
    def as_tuple(self)->Tuple:
        return astuple(self)
    
    @property
    def as_dict(self)->Dict:
        return asdict(self)
    
    @property
    def as_dataframe(self)->LazyFrame:
        return self.all() 
     
    @property
    def fields(self)->Tuple:
        return map(lambda x: Field(self.connection.database,
                                   self.connection.key, 
                                   x.name, str(x.type)), 
                    fields(self))
    
    def upsert(self) -> bool:
        return self.dbi.upsert(value = self.as_dict)

    def register_model(self) -> bool:
        for field in self.fields: field.upsert()
        Field.save()
   
    def delete(self,) -> bool:
        return self.dbi.delete(DatabasePtr(value=self.as_dict))

    @classmethod
    def get(cls, db_connection:DatabasePtr,
                limit:int=2,
                indexed:bool=False,
                page_number:int=0,
                )-> Iterable[Any] or Tuple[int,Any]:
        page = cls.dbi.get(db_connection,indexed=indexed)\
                        .with_row_count()\
                        .filter(col('row_nr') >= (page_number * limit))\
                        .limit(limit)\
                        .drop('row_nr')\
                        .collect()
        while not page.is_empty():
            for row in page.iterrows():
                yield (row[0], cls(*row[1:])) if indexed else cls(*row)
            page_number += 1
            page = cls.dbi.get(db_connection,indexed=indexed)\
                            .with_row_count()\
                                .filter(col('row_nr') > (page_number * limit))\
                                .limit(limit)\
                                .drop('row_nr')\
                                .collect()
    
    @classmethod
    def all(cls,
            limit:int=2,
            indexed:bool=False,
            page_number:int=0,
            ) -> List[Tuple]:
        database, key, _, _ = cls.connection
        return cls.get(DatabasePtr(database,key,None,None),limit,indexed,page_number)

    @classmethod
    def contains(cls, key:str='')->bool:
        key = key or cls.key
        return cls.dbi.contains(key) 
    
    @classmethod
    def is_in(cls, value:Tuple[Any], key:str='')->bool:
        key = key or cls.key
        return cls.dbi.is_in(cls.database,value,key) 
    
    @classmethod
    def save(cls) -> bool:
        return cls.dbi.save()

@dataclass(frozen=True,slots=True)
class Field(PolarModel):
    database:str
    key:str
    name:str
    Type: str
    
    class Meta(PolarModel.Meta):
        connection = DatabasePtr('model','fields')
        
Field.Meta.connect()