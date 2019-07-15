from orm.PSQL import Psql

def psql(options):
    return Psql(options)

class ORM:
    def __init__(self, options, dialect):
        dialects = {
            'psql': psql
        }
        self.obj = dialects[dialect](options)
    
    def app(self):
        return self.obj
        