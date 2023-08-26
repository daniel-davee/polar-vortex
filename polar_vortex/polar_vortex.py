from plac import Interpreter
from polar_vortex.interfaces.vortex_interface import vortex_path, VortexInterface
from polar_vortex.protocols.database_protocols import DatabasePtr
import shelve

db_help = '''database name which keys you want to list, if not specified all databases will be listed.'''

class PolarVortex(object):
    
    '''
    Polar Vortex is the cli for either the polars or vortex databases.
    '''
    
    commands = 'list', 'get_path'
    
    dbs = {db.stem for db in vortex_path.iterdir()}
    
    def get_path(self, 
                 db_type:('Either vortex or polar','positional' ,)='vortex',
                 ):
        '''
        Get the path to the database.
        '''    
        match(db_type):
            case 'vortex':print(vortex_path.absolute())
            case _: raise Exception(f'No database type {db_type}.')
    
    def list(self, 
             database:(db_help,'positional' ,)='',
             ):
        '''
        List the databases or keys in a database.
        '''
        match(database):
            case '':
                for db in self.dbs:print(db)
            case str() if database in self.dbs: 
                print(VortexInterface(DatabasePtr(database)).keys())
            case _: raise Exception(f'No database named {database} found.')

def main():
    Interpreter.call(PolarVortex)
        
if __name__ == '__main__':
    main()