import psycopg2 as pg
import logging
from orm.ObjectRelationalMaping import ObjectRelationalMaping, getTable, getSchema
class Psql(ObjectRelationalMaping):

    def __init__(self, options):
        super(Psql, self).__init__(options)
        self.con = pg.connect(database=self.DBName, user=self.userName, password=self.password, host=self.host, port=self.port)
        self.curs = self.con.cursor()

    def define(self, table_name, columns):
        super().define(table_name, columns)
        command = "CREATE TABLE {table}(".format(table=table_name)
        for column in columns:
            col = columns[column]
            print(col)
            data_type = col['type']
            unique = ' UNIQUE' if col.get('unique', False) else ''
            primary = ' PRIMARY KEY' if col.get('isPrimaryKey', False) else ''
            null = ' NOT NULL' if col.get('notNull', False) else ''
            command += "{column} {data_type}{primary}{unique}{not_null},".format(column=column, data_type=data_type, primary=primary, unique=unique, not_null=null)
        command = command[:-1]+');'
        print(command)
        try:
            self.curs.execute(command)
            self.con.commit()
        except Exception as e:
            logging.warning(e)
            self.con.rollback()
        return getTable(Psql, table_name, self)
    
    def newSchema(self, schema):
        super().newSchema(schema)
        command = "CREATE SCHEMA {}".format(schema)
        try:
            self.curs.execute(command)
            self.con.commit()
        except Exception as e:
            logging.warning(e)
            self.con.rollback()
        return getSchema(Psql, schema, self)
    
    def insert(self, tabele_name, row):
        row, columns = super().insert(row)
        command = "INSERT INTO {} {}values({})".format(tabele_name, columns, row)
        try:
            self.curs.execute(command)
            self.con.commit()
            return 0
        except Exception as e:
            logging.warning(e)
            self.con.rollback()
            if "duplicate key"in str(e):
                return -2
            else:
                return -1
        print(command)

    def parse(self, col, val):
        if(type(val) == type('') or type(val) == type(1)):
            return col+"="+(str(val) if type(val) == type(1) else "'"+ val +"'")
        else:
            condition = ""
            for rules in val:
                if rules == "OR":
                    condition = "("
                    for rule in val[rules]:
                        print(rule)
                        if rule == "eq":
                            condition += (col + " = " + str(val[rules][rule]))+" OR "
                        elif rule == "lt":
                            condition += (col + " < " + str(val[rules][rule]))+" OR "
                        elif rule == "gt":
                            condition += (col + " > " + str(val[rules][rule]))+" OR "
                        elif rule == "lte":
                            condition += (col + " <= " + str(val[rules][rule]))+" OR "
                        elif rule == "gte":
                            condition += (col + " >= " + str(val[rules][rule]))+" OR "
                        elif rule == "lk":
                            condition += (col + " LIKE '" + str(val[rules][rule]))+"' OR "
                    condition = condition[:-4]+")"
                elif rules == "AND":
                    condition = "("            
                    for rule in val[rules]:
                        print(rule)
                        if rule == "eq":
                            condition += (col + " = " + str(val[rules][rule]))+" AND "
                        elif rule == "lt":
                            condition += (col + " < " + str(val[rules][rule]))+" AND "
                        elif rule == "gt":
                            condition += (col + " > " + str(val[rules][rule]))+" AND "
                        elif rule == "lte":
                            condition += (col + " <= " + str(val[rules][rule]))+" AND "
                        elif rule == "gte":
                            condition += (col + " >= " + str(val[rules][rule]))+" AND "
                        elif rule == "lk":
                            condition += (col + " LIKE '" + str(val[rules][rule]))+"' AND "
                    condition = condition[:-4]+")"
                else:
                    if rules == "eq":
                        condition += (col + " = " + (str(val[rules]) if type(val[rules]) == type(1) else "'"+ val[rules] +"'"))
                    elif rules == "lt":
                        condition += (col + " < " + (str(val[rules]) if type(val[rules]) == type(1) else "'"+ val[rules] +"'"))
                    elif rules == "gt":
                        condition += (col + " > " + (str(val[rules]) if type(val[rules]) == type(1) else "'"+ val[rules] +"'"))
                    elif rules == "lte":
                        condition += (col + " <= " + (str(val[rules]) if type(val[rules]) == type(1) else "'"+ val[rules] +"'"))
                    elif rules == "gte":
                        condition += (col + " >= " + (str(val[rules]) if type(val[rules]) == type(1) else "'"+ val[rules] +"'"))
                    elif rules == "lk":
                        condition += (col + " LIKE '" + str(val[rules])+"'")
            return condition

    
    def findAll(self, table, options):
        super().findAll(options)
        columns = "*"
        where = ""
        if 'attributes' in options:
            columns = ""
            attributes = options['attributes']
            for col in attributes:
                if type(col) == type([]):
                    columns += (col[0] +" AS "+col[1]+",")
                else:
                    columns += col+","
            columns = columns[:-1]
        if 'where' in options:
            queries = options['where']
            for query in queries:
                if query == "OR":
                    cols = queries[query]
                    for col in cols:
                        temp = cols[col]
                        where += self.parse(col, temp) + " OR "
                    where = where[:-3] + "AND "
                elif query == "AND":
                    cols = queries[query]
                    for col in cols:
                        temp = cols[col]
                        where += self.parse(col, temp) + " AND "
                else:
                    where += self.parse(query, queries[query]) + " AND "
            where = where[:-4] if "OR" in where[-4:] or "AND" in where[-4:] else where
            where = " WHERE " + where
        command = "SELECT {columns} from {table}{where}".format(columns=columns, table=table, where=where)
        print(command)
        try:
            self.curs.execute(command)
            self.con.commit()
            return self.curs.fetchall()
        except Exception as e:
            logging.warning(e)
            self.con.rollback()



'''
columns = {
    col_name:{
        type: '',
        unique: bool,
        isPrimaryKey:bool,
        notNull: bool,
        reference:{
            table:string:table,
            key:string:column,
        }
    }
}

findAll:
    options={
        where:{
            column:value
        },
        attributes:[...columns]
    }

'''