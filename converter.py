"""
Author      : Lloyd Moore
Version     : 0.1
Title       : SQL_to_JSON_Converter
Description : |
    Given an sql schema returns the same data formatted as sets
    of json strings.

    Given a table with the following schema and data.

        Groups
            Schema  id int, label
            Data    1, canine
                    2, feline

        Animals
            Schema  id int, group_id int, label varchar(10)
            Data    1, canine, dog 
                    2, feline, cat
                    3, feline, tiger

        Would Return the following json strings as a set.
            {"id":1, "group":"canine", "label":"dog"},
            {"id":2, "group":"feline", "label":"cat"},
            {"id":3, "group":"feline", "label":"tiger"}

How it Works

    The two tables, Animals and Groups are related by the columns
    Animals.group_id and groups.id. Although there are no foreign keys defined in 
    our simple example it is common (bad) practice to not define them and 
    any software tool that is useful should cater for the most common cases.

    The algorithm to infer the connection is as follows:

    Given a table, inspect it's column names and if there is one that ends in _id,
    take the first part of its name as the name of the table it relates to.

    For each data entry in the table under the column name found, take the id and 
    return the data entry in the foreign table with the same id and expand it inline
    with the current data being inspected.
    
    So, given our example schema and data, lets see what would happen whilst running
    our application. Suppose the code inspects the Groups table first. None of its column
    names end in _id and so the application would ignore.

    The following table, Animals has a column named group_id. We take the 'group' part
    of the name and run a matching algorithm which first checks for a table called group, 
    Group, GROUP and if no match is found, checks for it's plural forms, Groups and GROUPS.

    In this case it would find Groups and merge the data in every entry
    in the Groups table with its corresponding row in the Animals table.
  
"""
import MySQLdb

class SqlToJson(object):
    def __init__(self, db): 
        self.con = MySQLdb.connect('localhost', 'root', 'root', db)
        self.cur = self.con.cursor()

    def process_tables(self):
        tables   = [] 
        table_names = self.get_table_names()
        for table in table_names:
            data = [dict(zip(self.get_columns(table), x)) for x in self.get_data(table)]
            table_data = {'data':data,'table':table}
            tables.append(table_data)
            self.denormalize(table_data)
        print tables
        return tables

    def get_data(self, table):
        return [x for x in self.q("select * from %s" % (table,))]

    def get_table_names(self):         
        return (x[0] for x in self.q("show tables"))

    def get_columns(self, table):
        return [x[0] for x in self.q("show columns from %s" % (table))]

    def q(self, sql):
        self.cur.execute(sql)
        return (x for x in self.cur.fetchall())

    def jsonq(self, sql):
        cols = self.get_cols_from_sql_cmd(sql)
        data = self.q(sql)
        return [dict(zip(cols, d)) for d in data]

    def get_cols_from_sql_cmd(self, sql):
        out = sql.lower().replace('select ','').split('from')
        out = out[0].split(',')
        out = map(lambda x: x.split(' ')[-1], out)
        return filter(lambda x: x != '', out)

    def get_foreign_tables(self, table):
        columns = filter(lambda x: x[-3:] == '_id', self.get_columns(table))
        return {
            'table':table, 
            'foreign_tables':["%ss" % x.split('_')[0] for x in columns]}

    def denormalize(self, table_data):
        table = table_data['table']
        tables = self.get_foreign_tables(table)
        foreigners = [x for x in tables.items()]
        print foreigners

if __name__=="__main__":
    db_name  = "doit"
    jsql = SqlToJson(db_name)
    sql = "select id myid, title, notes, created_at from task"
    #sql = "select * from task"
    print jsql.jsonq(sql)
