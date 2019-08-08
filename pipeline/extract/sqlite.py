import sqlite3

from settings.base_conf import sqlite_conf

sqlite_conn = sqlite_conf.SQLiteConfig[sqlite_conf.SQLiteENV]

DB_NAME = sqlite_conn['DB_NAME']

def create_connection(db_file):
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        logger.error(e)
 
    return None

def create_table(conn):
    cursor = conn.cursor()
    try:
        query = ("CREATE TABLE IF NOT EXISTS "
            + DB_NAME
            + "(id INTEGER PRIMARY KEY, kobo_id TEXT, "
            + "cb_id TEXT, rev_id TEXT)")

        cursor.execute(query)
        
        conn.commit()
    except Exception as e:
        # Roll back any change if something goes wrong
        conn.rollback()
        raise e

def insert_data(conn, kobo_id):
    cursor = conn.cursor()
    try:
        query = ("INSERT INTO " 
            + DB_NAME 
            + "(kobo_id, cb_id, rev_id) "
            + "VALUES(?,?,?)")

        values = (kobo_id, "", "")
        cursor.execute(query, values)

        logger.info("User inserted")
        conn.commit()
    except sqlite3.IntegrityError:
        logger.info('Record already exists')

def get_one_data(conn, kobo_id):
    cursor = conn.cursor()
    query = ("SELECT kobo_id, cb_id, rev_id FROM "
        + DB_NAME
        + " WHERE kobo_id=?")

    values = (kobo_id,)

    cursor.execute(query, values)
    user = cursor.fetchone()
    
    if(user != None):
        logger.info("User found!")

    return user

def get_many_data(conn):
    cursor = conn.cursor()
    query = ("SELECT cb_id, kobo_id FROM " + DB_NAME)
    cursor.execute(query)
    all_rows = cursor.fetchall()

    return all_rows

def update_data(conn, sqlite_data, kobo_id):

    (cb_id, rev_id) = (sqlite_data['id'], sqlite_data['rev'])

    cursor = conn.cursor()
    query = ("UPDATE "
        + DB_NAME
        + " SET cb_id=?, rev_id=? WHERE kobo_id=?")
    values = (cb_id, rev_id, kobo_id)
    cursor.execute(query,values)

    conn.commit()

def close_db(conn):
    conn.close()