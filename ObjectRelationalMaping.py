def getTable(Parent, table_name, props):
    class Tabel(Parent):

        def __init__(self, props):
            self.con = props.con
            self.curs = props.curs
            self.table_name = table_name

        def insert(self, row):
            return super().insert(self.table_name, row)
        
        def findAll(self, options={}):
            return super().findAll(self.table_name, options)

    return Tabel(props)

def getSchema(Parent, schema, props):
    class Schema(Parent):

        def __init__(self, props):
            self.con = props.con
            self.curs = props.curs
            self.schema = schema

        def getTablename(self, table):
            return self.schema+"."+table

        def define(self, table_name, columns):
            return super().define(self.getTablename(table_name), columns)
        
        def insert(self, table, row):
            super().insert(self.getTablename(table), row)
        
        def findAll(self, table, options):
            return super().findAll(self.getTablename(table), options)

    return Schema(props)


class ObjectRelationalMaping:
    def __init__(self, options):
        self.DBName = options['db_name']
        self.host = options['host']
        self.port = options['port']
        self.userName = options['user_name']
        self.password = options['password']

    def define(self, table_name, columns):
        pass

    def newSchema(self, schema):
        pass
    
    def insert(self, row):
        values = ""
        columns = ""
        if type(row) == type([]):
            for value in row:
                if type(value) == type(''):
                    values += "'{}',".format(value)
                else:
                    values += "{},".format(value)
        elif type(row) == type({}):
            columns = "("
            for column in row:
                columns += "{},".format(column)
                value = row[column]
                if type(value) == type(''):
                    values += "'{}',".format(value)
                else:
                    values += "{},".format(value)
            columns = columns[:-1] + ") "
        else:
            raise Exception('row is not in excepted type. Onyl [] or {} allowed')
        return [values[:-1], columns]

    def findAll(self, options):
        pass