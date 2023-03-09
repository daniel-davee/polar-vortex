from typing import Callable
from inspect import getframeinfo, stack
from pathlib import Path
from pysimplelog import Logger
from pprint import PrettyPrinter

log_dir = Path(__file__).parent / 'logs'

def debuginfo(log_fn:Callable)->Callable:
    def wrapper(msg,*args,**kwargs):
        frameinfo = getframeinfo(stack()[1][0])
        msg = '\n'.join(map(lambda _: _.strip(),
                            filter(bool ,f'''
                                            File: {frameinfo.filename} Line: {frameinfo.lineno} Function: {frameinfo.function}
                                            {msg}
                                            {'#'*80} 
                                        '''.split('\n')
                            )))
        return log_fn(msg,*args,**kwargs)
    return wrapper    
    

logger = Logger('polar_vortex')

logger.debug = debuginfo(logger.debug)

pp = PrettyPrinter(indent=4)


logger.set_log_file_basename((log_dir/'main_logs').as_posix())

def set_log_level(level:str) -> str:
    logger.set_minimum_level(logger.logLevels[level])
    logger.debug(f'Set log level to {level}')
    
logger.set_log_level = set_log_level