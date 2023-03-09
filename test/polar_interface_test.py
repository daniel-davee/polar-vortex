from typing import TypeAlias
from polar_vortex.interfaces.polar_interface import PolarsInterface, db_path
from polar_vortex.protocols.database_protocols import (
                                                        DatabasePtr,
                                                        DataOperationPtr,
                                                        Data, Datum
                                                        )
from polar_vortex.interfaces.log_interface import logger
from os import remove
from pytest import fixture
from pathlib import Path

logger.set_log_level('debug')

'''
    Polar interface tests
    pi = PolarInterface = (ptr)
'''

class FooBar(Datum):
    foo:int
    bar:str

FooBars: TypeAlias = Data[FooBar]

@fixture
def db_ptr() -> DatabasePtr:
    return DatabasePtr('test', 'foo')

@fixture
def key_file(db_ptr) -> Path:
    database, key = db_ptr
    return db_path/f'{database}/{key}.parquet'

@fixture
def pi(db_ptr) -> PolarsInterface:
    return PolarsInterface(db_ptr)

@fixture
def hitch_hiker_datum() -> FooBar:
    return FooBar(42,'The ultimate answer')

@fixture
def nice_datum() -> FooBar:
    return FooBar(96,'Nice?')

@fixture
def weed_datum() -> FooBar:
    return FooBar(420,'High world')


def test_keyfile_dne(key_file):
    if key_file.exists(): remove(key_file)
    assert not key_file.exists()
    
    
def test_create_interface(pi):
    
    '''
    After pi =  PolarsInterface(ptr) key_file should exist and pi.is_empty()
    '''
    
    assert pi.is_empty
    
    
def test_upsert(pi,hitch_hiker_datum):
    '''
    After pi.upsert([{bar:42},{bar:69}]) pi.is_empty() should be false
    '''
    pi.upsert(hitch_hiker_datum)
    assert not pi.is_empty