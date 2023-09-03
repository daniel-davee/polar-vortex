from typing import TypeAlias, Tuple
from polar_vortex.interfaces.vortex_interface import VortexInterface, vortex_path
from polar_vortex.protocols.database_protocols import (
                                                        DatabasePtr,
                                                        DatabasePtr,
                                                        Data, Datum, Key
                                                        )
from polar_vortex.interfaces.log_interface import logger
from os import remove
from pytest import mark

logger.set_log_level('debug')

'''
    Polar interface tests
    vi = PolarInterface = (ptr)
'''

class FooBar(Datum):
    foo:int
    bar:str

FooBars: TypeAlias = list[FooBar]

my_key: str = 'my_key'

db_ptrs: tuple[DatabasePtr, DatabasePtr] = (DatabasePtr('test', my_key),
                                            DatabasePtr('foobar', None))

hitch_hiker_datum: FooBar = FooBar({'foo':42,
                                    'bar':'The ultimate answer'})

@mark.order(1)
def test_db_ptr():
    list(map(remove,(database:=filter(lambda f:f.stem in {k.database for k in db_ptrs}, 
                                       vortex_path.iterdir()))))
    assert not any(f.exists() for f in database)
    return db_ptrs
    
@mark.order(2)
def test_create_interfaces():
    
    '''
    After vi =  PolarsInterface(ptr) key_file should exist and vi.is_empty()
    '''
    k_vis, db_vis = vis = tuple(map(VortexInterface, db_ptrs))
    logger.debug(f'{k_vis.values=}')
    logger.debug(f'{db_vis.values=}')
    assert all(vi.is_empty() for vi in vis)

@mark.order(3)
def test_upsert():
    '''
    After vi.upsert([{bar:42},{bar:69}]) vi.is_empty() should be false
    '''
    
    k_vis, db_vis = vis = tuple(map(VortexInterface, db_ptrs))
    list(map(lambda vi: vi.upsert(hitch_hiker_datum), vis))
    assert not k_vis.is_empty()
    assert k_vis.is_in(hitch_hiker_datum)
    assert k_vis.contains(my_key)
    assert k_vis.save()
    assert db_vis.contains('foo')
    assert db_vis.contains('bar')
    assert db_vis.is_in(42)
    assert db_vis.is_in('The ultimate answer')
    assert db_vis.save()
    logger.debug(f'{db_vis.values=}')
    logger.debug(f'{k_vis.values=}')
    
@mark.order(4)
def test_get():
    '''
    After vi.get() vi.values should be [{bar:42},{bar:69}]
    '''
    k_ptr, db_ptr = db_ptrs
    k_get, db_get = VortexInterface(k_ptr).get(), VortexInterface(db_ptr).get()
    logger.debug(f'{k_get=}')
    logger.debug(f'{db_get=}')
    assert db_get == hitch_hiker_datum
    assert k_get == {my_key: hitch_hiker_datum}