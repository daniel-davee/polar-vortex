from behave import given, when, then
from polar_vortex.interfaces.log_interface import logger
from polar_vortex.interfaces.polar_interface import PolarsInterface
from polar_vortex.protocols.database_protocols import DatabasePtr
from candy.candy_wrapper import Wrapper
from candy.candy_wrapper import Wrapper
from functools import partial
from polars import LazyFrame
from typing import Any

def parse_value(context,value:str) -> Any:
    match(value):
        case 'None': return None
        case v if ':' in v: return dict(map(lambda x: x.split(':'), v.split(',')))
        case v if ',' in v: return list(map(lambda _:_.strip(),v.split(',')))
        case 'True': return True
        case 'False': return False
        case v if v.isnumeric():return float(v) if '.' in v else int(v)
        case v if v in context.variables: return context.variables[v]()
        case v:return v

@given(u'the log level is {level}')
def step_impl(context, level):
    logger.info(f'{"#"*20} log {level=}')
    logger.set_minimum_level(logger.logLevels[level])

@given(u'database and key_file should not exist')
def step_impl(context):
    database, key = context.variables.database(), context.variables.key()
    pi = PolarsInterface(DatabasePtr(database,key))
    pi.key_file.unlink()
    pi.key_file.parent.rmdir()

@given(u'the variable {name} is {value}')
def step_impl(context, name, value):
    name, value = name.strip(), value.strip()
    logger.debug(f'{name=},{value=}')
    if not hasattr(context,'variables'): context.variables = Wrapper('variables') 
    context.variables[name] = parse_value(context,value)
    logger.debug(f'{context.variables[name]()=}') 

@when(u'run {verb}')
def step_impl(context, verb):
    variables = context.variables
    database, key = variables.database(), variables.key()
    index, value = variables.index(), variables.value()
    ptr = DatabasePtr(database,key,index)
    pi = PolarsInterface(ptr)
    match(verb):
        case 'create': context.result = Wrapper(pi)
        case 'save': context.result = Wrapper(pi.save)
        case 'upsert': context.result = Wrapper(partial(pi.upsert,[value]))
        case 'get': context.result = Wrapper(partial(pi.get,ptr,indexed=True))
        case 'delete':context.result = Wrapper(partial(pi.delete,ptr,variables.locked))
        case 'all': context.result = Wrapper(pi.all)
        case 'empty': context.result = Wrapper(pi.is_empty)
        case 'key_file': context.result = Wrapper(pi.key_file.exists)
        case _: raise NotImplementedError(f'{verb} is not implemented')

@then(u'all values should be the result')
def step_impl(context):
    result = context.result()
    database, key = context.variables.connection()
    df = PolarsInterface(DatabasePtr(database,key))\
                            .lazyframe\
                            .collect()
    result = result.drop('index').collect()
    logger.debug(f'{result=},{df=},{df.frame_equal(result)=}')
    assert result.frame_equal(df)


@then(u'the value at {index} should be returned')
def step_impl(context, index):
    result = context.result()
    if isinstance(result, LazyFrame): result = result.collect()
    index , result = int(index), result.collect()
    logger.debug(f'{result=},{index=}') 
    assert result['index'] == index

@then(u'{value} should be returned')
def step_impl(context, value):
    result = context.result()
    if isinstance(result, LazyFrame): result = result.drop('index').collect()
    logger.debug(f'{result=},{(pv:=parse_value(context,value))=}') 
    assert result == pv

