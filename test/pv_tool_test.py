from polar_vortex.interfaces.log_interface import logger
from polar_vortex.pv_tool import PolarVortex
from run_cmd.run_cmd import Script
from pathlib import Path

cmd_path:Path = Path(__file__).parent.parent / 'polar_vortex/pv_tool.py'

logger.set_log_level('debug')

def test_help():
    '''
    Testing help. 
    '''
    sub_cmds:list[str] = PolarVortex.commands
    docs:list[str] = [getattr(PolarVortex,cmd).__doc__.strip() for cmd in sub_cmds]
    help_cmds:Script = Script()
    help_cmds.cmds = '\n'.join([f'python {str(cmd_path.absolute())} {cmd} -h' for cmd in sub_cmds])
    help_cmds = list(help_cmds())
    for cmd,doc in zip(help_cmds,docs):
        assert doc in cmd
    