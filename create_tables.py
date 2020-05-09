'''
    Manage creation of the db and tables
    This file depend on sql_queries where all static infos are defined (DSN, sql queries...)
    >> open a console and do a %run create_tables.py
    execute_sql_statment() can be verbose
'''
import psycopg2
from sql_queries import create_table_queries, drop_table_queries, DSN_STUDENT, DSN_SPARKIFY, DB_SCRIPT
  
def execute_sql_statment( dsn , sql_queries , sql_exception_msg = 'psycopg2 error message' , log_verbose = False):
    '''
        Manage connection to the database and process sql_querie 
        Udacity note : It replace originals create_database(), drop_tables(), create_tables() methods by a unique one
        
        Parameters
        ----------
            dsn : str - data source name for psycopg2.connect()
            sql_queries : list of str - SQL query(ies) to process
            sql_exception_msg : str - a short description in case of error
            log_verbose : bool - log connection & queries
            
        Returns
        -------
            bool : True if everything has done right, False
        
        Raise
        -----
            TypeError if sql_queries is not a list
    '''

    #assume by default that something is going wrong :-)
    transaction_status = False
    #query currently processing...
    current_query = ''
    
    if( not isinstance(sql_queries, list) ):
        raise TypeError('statments should  be a list/array')
        
    conn = None        
    try: 
        # connect to default database and get 
        conn = psycopg2.connect( dsn )
        conn.set_session(autocommit=True)
        cur = conn.cursor()
        if( log_verbose ): print( conn )
        
        for query in sql_queries:
            current_query = query #copy to get it out of "for" scope
            if( log_verbose ): print( "query :{}".format(current_query) )
                
            cur.execute(current_query)
            conn.commit()
            
        transaction_status = True #well done :-)
        
    except psycopg2.Error as e: 
        
        if( conn ):conn.rollback()
            
        #print a friendly message
        print( sql_exception_msg.format(current_query) )
        if( current_query != '' ):print("current query : {}".format( current_query ) )
        print (e)
        
    finally:
        # always close connection and never let the system in a bad state
        if( conn ):conn.close()
            
        return transaction_status

def main():
    '''
        Launch all SQL process to prepare the database for
    '''    
    print( '*********** Creating database tables.....')
    if( not execute_sql_statment( DSN_STUDENT , DB_SCRIPT , "Error when creating sparkify database" , False ) ): return False
    
    print( '*********** Dropping tables.....')
    if( not execute_sql_statment( DSN_SPARKIFY , drop_table_queries , "Error when dropping table" , False ) ): return False
    
    print( '*********** Creating tables   .....')
    if( not execute_sql_statment( DSN_SPARKIFY , create_table_queries , "Error when creating table" , False ) ): return False
    
    return True
    
if __name__ == "__main__":
    main()