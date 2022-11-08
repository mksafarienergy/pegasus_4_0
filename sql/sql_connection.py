import time

import pyodbc 

# Some other example server values are
# server = 'localhost\sqlexpress' # for a named instance
# server = 'myserver,port' # to specify an alternate port

SERVER = 'safari-pegasus.database.windows.net'
DATABASE = 'pegasus'
USERNAME = 'pegasus-master'
PASSWORD = 'SolarEnergy2021'
DRIVER = '{ODBC Driver 17 for SQL Server}'

# ENCRYPT defaults to yes starting in ODBC Driver 18. It's good to always specify ENCRYPT=yes on the client side to avoid MITM attacks.
connection_string = f'DRIVER={DRIVER};SERVER={SERVER};DATABASE={DATABASE};ENCRYPT=yes;UID={USERNAME};PWD={PASSWORD}'
cnxn = pyodbc.connect(connection_string)
cursor = cnxn.cursor()
cursor.fast_executemany = True

#Sample select query
cursor.execute("SELECT @@version;") 
row = cursor.fetchone() 
while row: 
    print(row[0])
    row = cursor.fetchone()


def get_query_string_by_table(table_name) -> str:
    """Return query string based on the table name"""

    if table_name == 'temp_locus_sitelevel_daily':
        return 'INSERT INTO dbo.temp_locus_sitelevel_daily (site_id, monitoring_platform, backup_site_id, measured_energy, proforma) VALUES (?,?,?,?,?)'
    
    return ''



def insert_into_table(table_name) -> None:
    """
    Inserting a single row into a table. retrieves
    """

    start = time.time()
    query = get_query_string_by_table(table_name)
    count = cursor.executemany(query, [['Test1', 'TestLocus', 'Test Backup', '234', 324]]*5000)
    cnxn.commit()
    end = time.time()
    print(f'Rows inserted: {str(count)} | {end - start}')


if __name__ == "__main__":
    insert_into_table('temp_locus_sitelevel_daily')
