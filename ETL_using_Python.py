#DATA ENGINEER - WRITING DAGs

#Objectives
#In this assignment you will write a python program that will:
#- Connect to IBM DB2 data warehouse and identify the last row on it.
#- Connect to MySQL staging data warehouse and find all rows later than the last row on the datawarehouse.
#- Insert the new data in the MySQL staging data warehouse into the IBM DB2 production data warehouse.



# Import libraries required for connecting to mysql
import mysql.connector
# Import libraries required for connecting to DB2
import ibm_db

# Connect to MySQL
connection = mysql.connector.connect(user='root', password='MMMMNTktaXJlZZZZ',host='127.0.0.1',database='sales')
# create cursor
cursor = connection.cursor()

# Connect to DB2
dsn_hostname = "dashdb-txn-sbox-yp-dal09-04.services.dal.bluemix.net"
dsn_uid = "abc12345"
dsn_pwd = "7dBZ3wWt9XN6$o0J"
dsn_port = "30120"
dsn_database = "bludb"
dsn_driver = "{IBM DB2 ODBC DRIVER}"
dsn_protocol = "TCPIP"
dsn_security = "SSL"

#Create the dsn connection string
dsn = (
    "DRIVER={0};"
    "DATABASE={1};"
    "HOSTNAME={2};"
    "PORT={3};"
    "PROTOCOL={4};"
    "UID={5};"
    "PWD={6};"
    "SECURITY={7};").format(dsn_driver, dsn_database, dsn_hostname, dsn_port, dsn_protocol, dsn_uid, dsn_pwd, dsn_security)

# create connection
conn = ibm_db.connect(dsn, "", "")
print ("Connected to database: ", dsn_database, "as user: ", dsn_uid, "on host: ", dsn_hostname)

# Find out the last rowid from DB2 data warehouse
# The function get_last_rowid must return the last rowid of the table sales_data on the IBM DB2 database.

def get_last_rowid():
    SQL="select rowid from sales_data order by timestamp desc limit 1"
    stmt = ibm_db.exec_immediate(conn, SQL)
    while (ibm_db.fetch_row(stmt) is True):
        id = ibm_db.result(stmt, 0)
        return id

last_row_id = get_last_rowid()
print("Last row id on production datawarehouse = ", last_row_id)

# List out all records in MySQL database with rowid greater than the one on the Data warehouse
# The function get_latest_records must return a list of all records that have a rowid greater than the last_row_id in the sales_data table in the sales database on the MySQL staging data warehouse.

def get_latest_records(rowid):
    SQL = "select * from sales_data"
    cursor.execute(SQL)
    latest_rows = []
    for row in cursor.fetchall():
        if row[0] > rowid:
            latest_rows.append(row)
    return latest_rows

new_records = get_latest_records(last_row_id)

print("New rows on staging datawarehouse = ", len(new_records))

# Insert the additional records from MySQL into DB2 data warehouse.
# The function insert_records must insert all the records passed to it into the sales_data table in IBM DB2 database.

def insert_records(records):
    SQL = "insert into sales_data (rowid,product_id,customer_id,quantity)  VALUES(?,?,?,?);"
    stmt = ibm_db.prepare(conn, SQL)
    for row in records:
        ibm_db.execute(stmt, row)

insert_records(new_records)
print("New rows inserted into production datawarehouse = ", len(new_records))

# disconnect from mysql warehouse
connection.close()
# disconnect from DB2 data warehouse
ibm_db.close(conn)
# End of program