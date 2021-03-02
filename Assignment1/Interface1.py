import psycopg2
import os
import sys
import math

RANGE_TABLE_PREFIX = 'range_ratings_part'
RROBIN_TABLE_PREFIX = 'round_robin_ratings_part'

def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    try:
        #Function loads data into the table "ratings" from the file path "ratingsfilepath".
        conn = openconnection
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS " + ratingstablename+";")
        cur.execute("CREATE TABLE " + ratingstablename + "(userid INT, unwanted1 CHAR, movieid INT, unwanted2 CHAR, rating FLOAT, unwanted3 CHAR, timedate BIGINT);")
        cur.copy_from(open(ratingsfilepath), ratingstablename, sep=':')
        cur.execute("ALTER TABLE " + ratingstablename + " DROP COLUMN unwanted1, DROP COLUMN unwanted2, DROP COLUMN unwanted3, DROP COLUMN timedate;")
        #Creating MetaData table for storing partition details i.e (partition_type, No of partitions).
        cur.execute("DROP TABLE IF EXISTS partition_details;")
        cur.execute("CREATE TABLE partition_details( partition_type VARCHAR(40), no_of_partitions INT);")
        cur.close()
    except Exception as detail:
        traceback.print_exc()


def rangePartition(ratingstablename, numberofpartitions, openconnection):
    try:
        conn = openconnection
        cur = conn.cursor()
        increment = 5.0/numberofpartitions
        partitionnumber = 0
        y = 0
        while(partitionnumber< numberofpartitions):
            cur.execute("DROP TABLE IF EXISTS "+ RANGE_TABLE_PREFIX+ str(partitionnumber)+";")
            cur.execute("CREATE TABLE "+ RANGE_TABLE_PREFIX+ str(partitionnumber)+"(userid INT, movieid INT, rating FLOAT);")
            if partitionnumber == 0:
                cur.execute("INSERT INTO "+ RANGE_TABLE_PREFIX+ str(partitionnumber)+" (userid , movieid, rating) SELECT userid, movieid, rating FROM " + ratingstablename + " WHERE rating = 0 ;")
            cur.execute("INSERT INTO "+ RANGE_TABLE_PREFIX+ str(partitionnumber) + " (userid , movieid, rating) SELECT userid, movieid, rating FROM " + ratingstablename + " WHERE rating > " + str(partitionnumber) + " AND rating <= " + str(partitionnumber + increment) + ";")
            partitionnumber+=1
            y = y+increment
        cur.execute("INSERT INTO partition_details (partition_type, no_of_partitions) VALUES (\'range_ratings\', "+str(numberofpartitions)+");")
        cur.close()
        cur.close()
        conn.commit()
    except Exception as detail:
        traceback.print_exc()



def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    try:
        conn = openconnection
        cur = conn.cursor()
        i = 0
        while (i < numberofpartitions):
            cur.execute("DROP TABLE IF EXISTS "+ RROBIN_TABLE_PREFIX + str(i)+";")
            cur.execute("CREATE TABLE "+ RROBIN_TABLE_PREFIX + str(i) + "(userid INT, movieid INT, rating FLOAT);")
            #Using ROW_NUMBER to get rownumbers of data and store it in temporary table.
            cur.execute("INSERT INTO "+ RROBIN_TABLE_PREFIX + str(i) + " SELECT userid, movieid, rating FROM (SELECT userid, movieid, rating, ROW_NUMBER() OVER() as required_row FROM " + ratingstablename + ") as temporary WHERE ((temporary.required_row-1)% 5) = " + str(i) + ";")
            i += 1
        cur.execute("INSERT INTO partition_details (partition_type, no_of_partitions) VALUES (\'robin_ratings\', " + str(numberofpartitions) + ");")
        cur.close()
        conn.commit()
    except Exception as detail:
        traceback.print_exc()


def roundRobinInsert(ratingstablename, userid, itemid, rating, openconnection):
    try:
        conn = openconnection
        cur = conn.cursor()
        cur.execute("INSERT INTO " + ratingstablename + " VALUES (" + str(userid) + "," + str(itemid) + "," + str(rating) + ");")
        cur.execute("SELECT COUNT(*) FROM " + ratingstablename + ";")
        count = int(cur.fetchone()[0])
        cur.execute("SELECT no_of_partitions FROM partition_details WHERE partition_type = \'robin_ratings\' ")
        numberofrobinpartition = int(cur.fetchone()[0])
        cur.execute("INSERT INTO "+RROBIN_TABLE_PREFIX + str((count-1)%numberofrobinpartition) + "(userid, movieid, rating) VALUES (" + str(userid)+","+str(itemid)+","+str(rating)+");")
        cur.close()
        conn.commit()
    except Exception as detail:
        traceback.print_exc()


def rangeInsert(ratingstablename, userid, itemid, rating, openconnection):
    try:
        conn = openconnection
        cur = conn.cursor()
        cur.execute("SELECT no_of_partitions FROM partition_details WHERE partition_type = \'range_ratings\' ")
        numberofrangepartition = int(cur.fetchone()[0])
        partitiontablerange = 5.0/numberofrangepartition
        partitionnumber = math.floor(rating/partitiontablerange)-1
        cur.execute("INSERT INTO " + ratingstablename + " VALUES (" + str(userid) + "," + str(itemid) + "," + str(rating) + ");")
        if (rating != 0):
            cur.execute("INSERT INTO "+RANGE_TABLE_PREFIX + str(partitionnumber) + "(userid, movieid, rating) VALUES (" + str(userid)+","+str(itemid)+","+str(rating)+");")
        else:
            cur.execute("INSERT INTO "+ RANGE_TABLE_PREFIX + str(0)+"(userid, movieid, rating) VALUES (" + str(userid) + "," + str(itemid) + "," + str(rating) + ");")
        cur.close()
        conn.commit()
    except Exception as detail:
        traceback.print_exc()

def rangeQuery(ratingMinValue, ratingMaxValue, openconnection, outputPath):
    try:
        conn = openconnection
        cur = conn.cursor()
        cur.execute("SELECT no_of_partitions FROM partition_details WHERE partition_type = \'range_ratings\' ")
        numberofrangepartition = int(cur.fetchone()[0])
        cur.execute("SELECT no_of_partitions FROM partition_details WHERE partition_type = \'robin_ratings\' ")
        numberofrobinpartition = int(cur.fetchone()[0])

        outputfile = open(outputPath, "w+")

        j = 0
        while (j < numberofrobinpartition):
            cur.execute("SELECT userid, movieid, rating FROM "+RROBIN_TABLE_PREFIX + str(j) + " WHERE rating>= " + str(ratingMinValue) + " AND " + "rating <= " + str(ratingMaxValue) + ";")
            robinrecords = cur.fetchall()
            if len(robinrecords) != 0:
                for l in range(len(robinrecords)):
                    outputfile.write(RROBIN_TABLE_PREFIX + str(j) + "," + str(robinrecords[l][0]) + "," + str(robinrecords[l][1]) + "," + str(robinrecords[l][2]) + "\n")
            j += 1


        i = 0
        while ( i < numberofrangepartition):
            cur.execute("SELECT userid, movieid, rating FROM "+RANGE_TABLE_PREFIX+str(i)+" WHERE rating>= "+str(ratingMinValue)+" AND "+"rating <= "+str(ratingMaxValue)+";")
            rangerecords = cur.fetchall()
            if len(rangerecords) !=0:
                for k in range(len(rangerecords)):
                    outputfile.write(RANGE_TABLE_PREFIX+str(i)+","+str(rangerecords[k][0])+","+str(rangerecords[k][1])+","+str(rangerecords[k][2])+"\n")
            i+=1
        outputfile.close()
        cur.close()
        conn.commit()
    except Exception as detail:
        traceback.print_exc()

def pointQuery(ratingValue, openconnection, outputPath):

    try:
        conn = openconnection
        cur = conn.cursor()
        cur.execute("SELECT no_of_partitions FROM partition_details WHERE partition_type = \'range_ratings\' ")
        numberofrangepartition = int(cur.fetchone()[0])
        cur.execute("SELECT no_of_partitions FROM partition_details WHERE partition_type = \'robin_ratings\' ")
        numberofrobinpartition = int(cur.fetchone()[0])

        outputfile = open(outputPath, "w+")

        j = 0
        while (j < numberofrobinpartition):
            cur.execute("SELECT userid, movieid, rating FROM "+RROBIN_TABLE_PREFIX+ str(j) + " WHERE rating = " + str(ratingValue) + ";")
            robinrecords = cur.fetchall()
            if len(robinrecords) != 0:
                for l in range(len(robinrecords)):
                    outputfile.write(RROBIN_TABLE_PREFIX + str(j) + "," + str(robinrecords[l][0]) + "," + str(robinrecords[l][1]) + "," + str(robinrecords[l][2]) + "\n")
            j += 1

        i = 0

        while (i < numberofrangepartition):
            cur.execute("SELECT userid, movieid, rating FROM "+RANGE_TABLE_PREFIX + str(i) + " WHERE rating = " + str(ratingValue) +";")
            rangerecords = cur.fetchall()
            if len(rangerecords) != 0:
                for k in range(len(rangerecords)):
                    outputfile.write(RANGE_TABLE_PREFIX+ str(i) + "," + str(rangerecords[k][0]) + "," + str(rangerecords[k][1]) + "," + str(rangerecords[k][2]) + "\n")
            i += 1

        outputfile.close()
        cur.close()
        conn.commit()
    except Exception as detail:
        traceback.print_exc()


def createDB(dbname='dds_assignment1'):
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
    con.close()

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
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    finally:
        if cursor:
            cursor.close()
