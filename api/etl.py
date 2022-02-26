import mysql.connector

class ETL(object):
    def __init__(self,host, user, password, database):
        self.host_args = {
						'host' : host,
						'user' : user,
						'password' : password,
						'database' : database
						}
    
    def mysql_connect(self):
        try:
            mydb = mysql.connector.connect(**self.host_args)
            return mydb
        except mysql.connector.Error as e:
            print("      >> Error code:", e.errno)        # error number
            print("      >> SQLSTATE value:", e.sqlstate) # SQLSTATE value
            print("      >> Error message:", e.msg)       # error message
            print("      >> Error:", e)                   # errno, sqlstate, msg values
            s = str(e)
            print("      >> Error:", s)
            exit()  
    
    def exec_script(self,script_dir):
        mydb=ETL.mysql_connect(self)
        mycur=mydb.cursor(dictionary=True)
        with open(script_dir, 'r') as sql_file:
            result_iterator = mycur.execute(sql_file.read(), multi=True)
            for res in result_iterator:
                print("Running query: ", res)  # Will print out a short representation of the query
                print(f"Affected {res.rowcount} rows" )
        mydb.commit()
        mycur.close() 
    
    def insert_bulk(self, table, data, truncate='n'):
        mydb=ETL.mysql_connect(self)
        mycur=mydb.cursor()
        if truncate.lower() == 'y':
            truncSQL = 'truncate table {};'.format(table)
            print('mysql stmt:', truncSQL)
            mycur.execute(truncSQL)
        insertSQLState="Insert into air_pollution_api Values(%s,%s,%s,%s,%s,%s);"
        print('mysql stmt:', insertSQLState)
        result_iterator = mycur.executemany(insertSQLState, data)
        mydb.commit()
        mycur.close()