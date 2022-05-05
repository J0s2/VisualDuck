import duckdb
import dearpygui.dearpygui as dpg
import pandas as pd

DB = "__main__.duckdb"
CON = duckdb.connect(database = DB, check_same_thread=False)

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


def createDB(name, path):
    """
        Creates a database on the specified path
    """
    con = duckdb.connect(path + name + '.duckdb')
    con.close()
    updateMetaDB(name, path)


def runQuery(sender, app_data, user_data):


    result = user_data[1].execute(dpg.get_value(user_data[0])).df()
    dpg.set_value(user_data[2], result)
    
    

if __name__ == "__main__":

    dpg.create_context()

    dpg.create_viewport(title='VisualDuckDB', width=1920, height=1080)

    # Database List
    with dpg.window(label="Databases", width = 200, height = 1080):

        for n, database in enumerate(showDataBases()):
            # Show all Databases
            nameDatabase = database[0]
            tag =  'db' + str(n)
            dpg.add_button(tag = tag, label = nameDatabase, callback = connectDB, user_data=database)

        
    with dpg.window(label="Editor query", width = 1720, height = 480, pos=(200,0)):

        with dpg.window(label="Status", width = 1720, height = 600, pos=(200,480)):

            #dpg.add_text(CON.description)
            status = dpg.add_text(label = 'Results', default_value = CON.description, user_data='')

        query = dpg.add_input_text(label = "Hello, world", width = 1720, height = 400, multiline = True)
        dpg.add_button(label = 'Execute', callback=runQuery, user_data=[query, CON, status])


        
        
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
    CON.close()