from typing import TypeAlias
from polar_vortex.interfaces.vortex_interface import VortexInterface, db_path
from polar_vortex.protocols.database_protocols import (
                                                        DatabasePtr,
                                                        DataOperationPtr,
                                                        Data, Datum, Fact
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

class DoubleKey(Fact):
    key1:str
    key2:str

class FooBar(Datum):
    foo:int
    bar:str

FooBars: TypeAlias = Data[FooBar]

@fixture
def double_key() -> DoubleKey:
    return DoubleKey('foo','bar')

@fixture
def db_ptr() -> DatabasePtr:
    return DatabasePtr('test', 'foobar')

@fixture
def hitch_hiker_datum() -> FooBar:
    return FooBar(42,'The ultimate answer')

@fixture
def nice_datum() -> FooBar:
    return FooBar(96,'Nice?')

@fixture
def weed_datum() -> FooBar:
    return FooBar(420,'High world')

@fixture
def test_keyfile_dne(db_ptr:DatabasePtr)->DatabasePtr:
    database  = [f for f in db_path.iterdir() if f.name == db_ptr.database]
    map(remove, database)
    assert all(not f.exists() for f in database)
    return db_ptr
    
@fixture
def test_create_interface(test_keyfile_dne:DatabasePtr)->VortexInterface:
    
    '''
    After pi =  PolarsInterface(ptr) key_file should exist and pi.is_empty()
    '''
    assert (vi:=VortexInterface(test_keyfile_dne)).is_empty
    return vi
    
def test_upsert(test_create_interface:VortexInterface,hitch_hiker_datum):
    '''
    After pi.upsert([{bar:42},{bar:69}]) pi.is_empty() should be false
    '''
    vi = test_create_interface
    vi.upsert(hitch_hiker_datum)
    assert not vi.is_empty