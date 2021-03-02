#
# Assignment2 Interface
#

import psycopg2
import os
import sys
import threading

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):

    conn = openconnection
    cur = conn.cursor()
    cur.execute("SELECT MAX(" + SortingColumnName + "), MIN(" + SortingColumnName + ") FROM " + InputTable + ";")
    sortingcolumn_maximumvalue, sortingcolumn_minimumvalue = cur.fetchone()
    no_of_threads = 5
    rangeinterval = float(sortingcolumn_maximumvalue - sortingcolumn_minimumvalue) / no_of_threads
    threads = []

    def ascendingorder(InputTable, SortingColumnName, min_range_forsorttabel, max_range_forsorttabel, partitionnumber, openconnection):
        conn = openconnection
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS ascending_ordered_table" + str(partitionnumber) + ";")
        cur.execute("CREATE TABLE ascending_ordered_table" + str(partitionnumber) + " ( LIKE " + InputTable + " INCLUDING ALL);")
        if partitionnumber == 0:
            cur.execute("INSERT INTO ascending_ordered_table" + str(partitionnumber) + " SELECT * FROM " + InputTable + " WHERE " + SortingColumnName + " >= " + str(min_range_forsorttabel) + " AND " + SortingColumnName + " <= " + str(max_range_forsorttabel) + " ORDER BY " + SortingColumnName + " ASC;")
        else:
            cur.execute("INSERT INTO ascending_ordered_table" + str(partitionnumber) + " SELECT * FROM " + InputTable + " WHERE " + SortingColumnName + " > " + str(min_range_forsorttabel) + " AND " + SortingColumnName + " <= " + str(max_range_forsorttabel) + " ORDER BY " + SortingColumnName + " ASC;")
        cur.close()
        conn.commit()

    i = 0
    while(i < no_of_threads):
        min_range_forsorttabel =sortingcolumn_minimumvalue +i * rangeinterval
        max_range_forsorttabel = min_range_forsorttabel + rangeinterval
        threads.append(threading.Thread(target=ascendingorder, args=(InputTable, SortingColumnName, min_range_forsorttabel, max_range_forsorttabel, i, openconnection)))
        threads[i].start()
        i += 1
    cur.execute("DROP TABLE IF EXISTS " + OutputTable + ";")
    cur.execute("CREATE TABLE " + OutputTable + " ( LIKE " + InputTable + " INCLUDING ALL );")

    j = 0
    while(j < no_of_threads):
        threads[j].join()
        cur.execute("INSERT INTO " + OutputTable + " SELECT * FROM ascending_ordered_table" + str(j) + ";")
        cur.execute("DROP TABLE IF EXISTS ascending_ordered_table" + str(j) + ";")
        j += 1

    cur.close()
    conn.commit()




def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    conn = openconnection
    cur = conn.cursor()
    no_of_threads = 5
    cur.execute("SELECT MAX(" + Table1JoinColumn + "), MIN(" + Table1JoinColumn + ") FROM " + InputTable1 + ";")
    table1_maximumvalue, table1_minimumvalue = cur.fetchone()
    cur.execute("SELECT MAX(" + Table2JoinColumn + "), MIN(" + Table2JoinColumn + ") FROM " + InputTable2 + ";")
    table2_maximumvalue, table2_minimumvalue = cur.fetchone()
    join_maximumvalue = max(table1_maximumvalue, table2_maximumvalue)
    join_minimumvalue = min(table1_minimumvalue, table2_minimumvalue)
    rangeinterval = float(join_maximumvalue - join_minimumvalue) / no_of_threads
    threads = []
    cur.execute( "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + InputTable1 + "';")
    InputTable1_schema = cur.fetchall()
    cur.execute("SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + InputTable2 + "';")
    InputTable2_schema = cur.fetchall()

    i = 0
    while (i < no_of_threads):
        min_range_join = join_minimumvalue + i * rangeinterval
        max_range_join = min_range_join + rangeinterval
        threads.append(threading.Thread(target=Joinfunction, args=(InputTable1, InputTable2, InputTable2_schema, Table1JoinColumn, Table2JoinColumn, min_range_join, max_range_join, i, openconnection)))
        threads[i].start()
        i += 1
    cur.execute("DROP TABLE IF EXISTS " + OutputTable + ";")
    cur.execute("CREATE TABLE " + OutputTable + " ( LIKE " + InputTable1 + " INCLUDING ALL );")

    for k in range(len(InputTable2_schema)):
        cur.execute("ALTER TABLE " + OutputTable + " ADD COLUMN " + InputTable2_schema[k][0] + " " + InputTable2_schema[k][1] + ";")

    j = 0
    while (j < no_of_threads):
        threads[j].join()
        cur.execute("INSERT INTO " + OutputTable + " SELECT * FROM join_table" + str(j) + ";")
        cur.execute("DROP TABLE IF EXISTS join_table" + str(j) + ";")
        cur.execute("DROP TABLE IF EXISTS input1_temp" + str(j) + ";")
        cur.execute("DROP TABLE IF EXISTS input2_temp" + str(j) + ";")
        j += 1

    cur.close()
    conn.commit()


def Joinfunction(InputTable1, InputTable2, InputTable2_schema, Table1JoinColumn, Table2JoinColumn, min_range_join, max_range_join, partitionnumber, openconnection):
    # Function to joins partition based on the join key's Table1JoinColumn and Table2JoinColumn.
    conn = openconnection
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS join_table" + str(partitionnumber) + " ;")
    cur.execute("DROP TABLE IF EXISTS input1_temp" + str(partitionnumber) + ";")
    cur.execute("DROP TABLE IF EXISTS input2_temp" + str(partitionnumber) + ";")
    cur.execute("CREATE TABLE join_table" + str(partitionnumber) + "(LIKE " + InputTable1 + " INCLUDING ALL);")
    cur.execute("CREATE TABLE input1_temp" + str(partitionnumber) + " (LIKE " + InputTable1 + " INCLUDING ALL);")
    cur.execute("CREATE TABLE input2_temp" + str(partitionnumber) + " (LIKE " + InputTable2 + " INCLUDING ALL);")


    for k in range(len(InputTable2_schema)):
        cur.execute("ALTER TABLE join_table" + str(partitionnumber) + " ADD COLUMN " + InputTable2_schema[k][0] + " " + InputTable2_schema[k][1] + ";")

    if partitionnumber == 0:
        cur.execute("INSERT INTO input1_temp" + str(partitionnumber) + " SELECT * FROM " + InputTable1 + " WHERE " + Table1JoinColumn + " >= " + str(min_range_join) + " AND " + Table1JoinColumn + " <= " + str(max_range_join)+";")
        cur.execute("INSERT INTO input2_temp" + str(partitionnumber) + " SELECT * FROM " + InputTable2 + " WHERE " + Table2JoinColumn + " >= " + str(min_range_join) + " AND " + Table2JoinColumn + " <= " + str(max_range_join) + ";")
    else:
        cur.execute("INSERT INTO input1_temp" + str(partitionnumber) + " SELECT * FROM " + InputTable1 + " WHERE " + Table1JoinColumn + " > " + str(min_range_join) + " AND " + Table1JoinColumn + " <= " + str(max_range_join) + ";")
        cur.execute("INSERT INTO input2_temp" + str(partitionnumber) + " SELECT * FROM " + InputTable2 + " WHERE " + Table2JoinColumn + " > " + str(min_range_join) + " AND " + Table2JoinColumn + " <= " + str(max_range_join) + ";")

    cur.execute("INSERT INTO join_table" + str(partitionnumber) + " SELECT * FROM input1_temp" + str(partitionnumber) + " INNER JOIN input2_temp" + str(partitionnumber) + " ON input1_temp" + str(partitionnumber) + "." + Table1JoinColumn + " = input2_temp" + str(partitionnumber) + "." + Table2JoinColumn + ";")
    cur.close()
    conn.commit()


################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='dds_assignment2'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='dds_assignment2'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.commit()
    con.close()

# Donot change this function
def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()


