import duckdb
import dearpygui.dearpygui as dpg
from timeit import default_timer as timer
import pandas as pd

DB = "__main__.duckdb"
CON = duckdb.connect(database = DB, check_same_thread=False)


connections = {
    'BASE': duckdb.connect(database = DB, check_same_thread=False)
}

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

def connectDB(sender, app_data, user_data):

    
    print(f"sender is: {sender}")
    print(f"app_data is: {app_data}")
    print(f"user_data is: {user_data}")

    print(dpg.get_item_label(sender)) # Devuelve el nombre de la base de datos

    with dpg.tab(label = dpg.get_item_label(sender), parent = 'tabbar'):

        text = dpg.add_input_text(width = 1720, height = 400, multiline = True)
        con = duckdb.connect(database=user_data, check_same_thread=False)
        
        with dpg.window(label="Results", width = 1720, height = 550, pos=(200,480)):

                result = dpg.add_text(user_data='')

        with dpg.window(label="Status", width = 1720, height = 60, pos=(200,1030), no_close = True):

            status = dpg.add_text(default_value = dpg.get_item_label(sender) + ' connected', user_data='')

        dpg.add_button(label = 'Play', callback=runQuery, user_data=[con, text, result, status])
   
    print(connections)
    
def createDB(name, path):
    """
        Creates a database on the specified path
    """
    con = duckdb.connect(path + name + '.duckdb')
    con.close()
    updateMetaDB(name, path)


def runQuery(sender, app_data, user_data):


    con, q , result, status = user_data[0], dpg.get_value(user_data[1]),  user_data[2], user_data[3]
    

    query_start = timer()
    output = con.execute(q).fetchall()
    query_end = timer()

    dpg.set_value(result, output)
    

    query_time = (query_end - query_start)
    dpg.set_value(status, str(round(query_time, 3)) + 'ms')
       
def addDatabaseUI():

    for n, database in enumerate(showDataBases()):
            # Show all Databases. database[0] = name, database[1] = path
            dpg.add_button(label = database[0], callback = connectDB, parent = "Databases", user_data=database[1])
            
            
def closeConnections():

    for connector in connections:
        connections[connector].close()


if __name__ == "__main__":

    dpg.create_context()
    dpg.create_viewport(title='VisualDuckDB', width=1920, height=1080)

    # Database List
    with dpg.window(label="Databases", width = 200, height = 1080):

        addDatabaseUI() # Add databases name



    # Editor query    
    with dpg.window(label="Console" ,width = 1720, height = 480, pos=(200,0), no_scrollbar = True):

        with dpg.tab_bar(reorderable=True, tag = 'tabbar'):

            pass

        
    dpg.setup_dearpygui()
    dpg.show_viewport()
    dpg.start_dearpygui()

   ##DEBUG LINES 

    # while dpg.is_dearpygui_running():
    #     jobs = dpg.get_callback_queue() # retrieves and clears queue
    #     dpg.run_callbacks(jobs)
    #     dpg.render_dearpygui_frame()

    dpg.destroy_context()

    
    
    ##DEBUG LINES
    ##showDataBases()
    ##updateMetaDB('test','./')
    print('Conexión cerrada')
    closeConnections()