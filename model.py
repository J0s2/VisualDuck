import duckdb
DB = "__main__.duckdb"
CON = duckdb.connect(database = DB, check_same_thread=False)

def query(q, con = CON):

    return (con.execute(q).fetchall())



def checkDataBaseExists(path):

    q = """
      select count(database_name)
      from databases
      where path = '{path}'
    """.format(path = path)

    return query(q)

def updateMetaDB(name, path='./'):
    q = """"
      INSERT INTO databases VALUES ('{name}', '{path}')
    """.format(name = name, path = path)

    print(q)

def updateMetaTable(name, db):
    """

    """
    q = """
        INSERT INTO tables VALUES ('{name}', '{db}')
    """.format(name = name, db = db)

    print(q)

def showDataBases(con = CON):

    q = """SELECT database_name, path from databases"""

    return (con.execute(q).fetchall())

def createDB(name, path):
    """
        Creates a database on the specified path
    """
    con = duckdb.connect(path + name + '.duckdb')
    con.close()
    updateMetaDB(name, path)

def closeConnections():

    for connector in connections:
        connections[connector].close()

    