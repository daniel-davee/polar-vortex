from polar_vortex.interfaces.log_interface import logger
from polar_vortex.pv_tool import PolarVortex
from run_cmd.run_cmd import run_cmd
from pathlib import Path

cmd_path:Path = Path(__file__).parent.parent / 'polar_vortex/pv_tool.py'

logger.set_log_level('debug')

def test_help():
    '''
    After vi.get() vi.values should be [{bar:42},{bar:69}]
    '''
    cmd = run_cmd(f'python {str(cmd_path.absolute())} -h')
    assert PolarVortex.list.__doc__.strip() in cmd
    