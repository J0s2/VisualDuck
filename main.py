from email.policy import default
from tkinter import Scrollbar
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
        
        result = dpg.add_window(label="Results", width = 1720, height = 500, pos=(200,480))

                #result = dpg.add_text(user_data='')
                
                #result =  dpg.add_table(header_row=False, row_background=True,
                #            borders_innerH=True, borders_outerH=True, borders_innerV=True,
                #            borders_outerV=True, delay_search=True)

                    

        with dpg.window(label="Status", width = 1720, height = 110, pos=(200,980), no_close = True, no_scrollbar = False):

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

def showResults(result, output):

    print('pintando tabla en....', result)
    print(output.values)

    if dpg.does_item_exist('tableResult'):
        dpg.delete_item('tableResult')

    with dpg.table(parent = result, header_row=True, row_background=True,
                            borders_innerH=True, borders_outerH=True, borders_innerV=True,
                            borders_outerV=True, delay_search=True, tag = 'tableResult'):        


        for column in output:
        
            dpg.add_table_column(label=column)
            

        for i in range(len(output.values)):
            with dpg.table_row():
                for j in range(len(output.columns)):
                    dpg.add_text(output.values[i,j])


    
    print('fin tabla')


def runQuery(sender, app_data, user_data):


    con, q , result, status = user_data[0], dpg.get_value(user_data[1]),  user_data[2], user_data[3]
    

    query_start = timer()
    output = con.execute(q).fetch_df_chunk()
    query_end = timer()

    print('mostrar resultados en', result)
    showResults(result, output)
    #dpg.set_value(result, output)
    

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
        dpg.add_spacer()
        dpg.add_separator()
       



    # Editor query    
    with dpg.window(label="Console" ,width = 1720, height = 480, pos=(200,0), no_scrollbar = True, no_close = True):

        tab_bar_query =  dpg.add_tab_bar(reorderable=True, tag = 'tabbar')

            

    dpg.show_documentation()
    dpg.show_style_editor()
    dpg.show_debug()
    dpg.show_about()
    dpg.show_metrics()
    dpg.show_font_manager()
    dpg.show_item_registry()
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