import sqlite3
from datetime import datetime
import json

timeFrame = '2006-06'
sql_transaction = []

connection = sqlite3.connect('{}.db'.format(timeFrame))
c = connection.cursor()


def create_table():
    c.execute('''CREATE TABLE IF NOT EXISTS parent_reply
            (parent_id TEXT PRIMARY KEY, comment_id TEXT UNIQUE, 
            parent TEXT, comment TEXT, subreddit TEXT, unix INT, score INT)''')


def format_data(data):
    data = data.replace("\n", "newlinechar" ).replace("\r", "newlinechar" ).replace('"', "'" )
    return data


def transaction_bldr(sql):
    global sql_transaction
    sql_transaction.append(sql)
    if len(sql_transaction) > 1000:
        c.execute('BEGIN TRANSACTION')
        for s in sql_transaction:
            try:
                c.execute(s)
            except Exception as e:
                pass
        connection.commit()
        sql_transaction = []


def find_parent(pid):
    try:
        sql = "SELECT comment FROM parent_reply WHERE comment_id = '{}' LIMIT 1".format(pid)
        c.execute(sql)
        if (result := c.fetchone()) != None:
            return result[0]
        else:
            return False
    except Exception as e:
        return False


def find_existing_score(pid):
    try:
        sql = "SELECT score FROM parent_reply WHERE parent_id = '{}' LIMIT 1".format(pid)
        c.execute(sql)
        if (result := c.fetchone()) != None:
            return result[0]
        else:
            return False
    except Exception as e:
        return False


def acceptable(data):
    if len(data.split(' ')) > 50 or len(data) < 1:
        return False
    elif len(data) > 1000:
        return False
    elif data == ['deleted'] or data == ['removed']:
        return False
    else:
        return True


def sql_insert_replace_comment(parent_id, comment_id, comment, parent, subreddit, time, score):
    try:
        sql = """ UPDATE parent_reply SET parent_id = ?, comment_id = ?, comment=?, parent=?, subreddit=?, time=?, score=? 
                WHERE parent_id = ?;""".format(parent_id)
        transaction_bldr(sql)

    except Exception as e:
        print("S-UPDATE",e)


def sql_insert_parent(parent_id, comment_id, comment, parent, subreddit, time, score):
    try:
        sql = """ INSERT INTO parent_reply (parent_id, comment_id, comment, parent, subreddit, time, score)"""
        transaction_bldr(sql)

    except Exception as e:
        print("S-parent - insertion", e)


def sql_insert_no_parent(parent_id, comment_id, comment, subreddit, time, score):
    try:
        sql = """ INSERT INTO parent_reply (parent_id, comment_id, comment, subreddit, time, score)"""
        transaction_bldr(sql)

    except Exception as e:
        print("S-NO Parent - insertion", e)


if __name__ == "__main__":
    create_table()
    row_counter = 0
    paired_rows = 0

    with open("/Users/kalpanamaram/PycharmProjects/chatbot/data/RC_{}".format(timeFrame), buffering=1000) as f:
        for row in f:
            row_counter += 1
            row = json.loads(row)
            parent_id = row['parent_id']
            comment_id = 0
            body = format_data(row['body'])
            created_utc = row['created_utc']
            score = row['score']
            subreddit = row['subreddit']
            parent_data = find_parent(parent_id)

            if score > 2:
                if acceptable(body):
                    existing_comment_score = find_existing_score(parent_id)
                    if existing_comment_score:
                        if score > existing_comment_score:
                            sql_insert_replace_comment(parent_id, comment_id, parent_data, body, subreddit, created_utc, score)
                            paired_rows += 1
                    else:
                        if parent_data:
                            sql_insert_parent(parent_id, comment_id, parent_data, body, subreddit, created_utc, score)
                        else:
                            sql_insert_no_parent(comment_id, parent_id, body, subreddit, created_utc, score)

            if row_counter % 10000 == 0:
                print("Total rows: {}, Paired rows: {}, Time: {}".format(row_counter, paired_rows, str(datetime.now())))


