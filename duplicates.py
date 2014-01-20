#!/usr/bin/env python
# Author: semmara

import sqlite3
import sys
import os
import hashlib
import mimetypes
import stat
import threading
import Queue
import subprocess
import traceback
import argparse


# TODO:
# * continue at last position - any idea???
# * show duplicates only, if files still exist???
# * ignore files with defined mime type
# * identify mount points
# * delete duplicated files


args = {}


# id ???
class DB_Manager(object):
    db_file = ":memory:"
    table_created = {"lock": threading.Lock(), "state": False}

    def __init__(self):
        self.conn = sqlite3.connect(DB_Manager.db_file, isolation_level="IMMEDIATE")
        self.name = "indication"
        l = DB_Manager.table_created["lock"]
        l.acquire(True)
        if not DB_Manager.table_created["state"]:
            self._create_table()
            DB_Manager.table_created["state"] = True
        l.release()

    def __del__(self):
        self.conn.close()

    def __dump(self):
        print "-"*80
        print ">>> DUMP:"
        for line in self.conn.iterdump():
            print line

    def set_table(self, name):
        self.name = name

    def _create_table(self):
        cur = self.conn.cursor()
        cur.execute('''CREATE TABLE IF NOT EXISTS %s
                    (i_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    path TEXT NOT NULL,
                    mtime DOUBLE NOT NULL,
                    hash VARCHAR (32),
                    mime VARCHAR (32),
                    read_size UNSIGNED BIG INT,
                    file_size UNSIGNED BIG INT)''' % self.name)
        self.conn.commit()

    def add_item(self, path_, mtime_, hash_, mime_, read_size_, file_size_):
        """!adds item to database if path_ is unknown to database
        @arg path_ path to file
        @arg mtime_ time of last file modification
        @arg hash_ hash value of file
        @arg mime_ mime type of file
        @returns True if added
        @returns False if path already exists
        """
        cur = self.conn.cursor()
        cur.execute('''SELECT i_id FROM %s WHERE path='%s' ''' % (self.name, path_))
        for i in cur:
            return False
        cur.execute('''INSERT INTO %s (i_id, path, mtime, hash, mime, read_size, file_size)
                    VALUES (NULL, '%s', %f, '%s','%s',%d,%d) ''' % (self.name, path_, mtime_, hash_, mime_, read_size_, file_size_))
        self.conn.commit()
        cur.close()
        return True

    def get_items(self, mime_=None):
        cur = self.conn.cursor()
        if mime_ is None:
            cur.execute('''SELECT * FROM %s''' % self.name)
        else:
            cur.execute('''SELECT * FROM %s WHERE mime='%s' ''' % (self.name, mime_))
        l = []
        for i in cur:
            l.append(i)
        return l

    def get_item_by_path(self, path_):
        cur = self.conn.cursor()
        cur.execute('''SELECT * FROM %s WHERE path='%s' ''' % (self.name, path_))
        l = []
        for i in cur:
            l.append(i)
        return l

    def get_item_by_hash(self, hash_):
        cur = self.conn.cursor()
        cur.execute('''SELECT * FROM %s WHERE hash='%s' ''' % (self.name, hash_))

    def get_duplicates(self):
        cur = self.conn.cursor()
        e = '''select o.i_id, o.path, o.hash, o.mtime
                from %s o
                inner join (
                    SELECT hash, COUNT(*) AS dupeCount
                    FROM %s
                    GROUP BY hash
                    HAVING COUNT(*) > 1
                ) oc on o.hash = oc.hash
                ORDER BY o.hash,o.mtime DESC''' % (self.name, self.name)
        cur.execute(e)
        l = []
        for row in cur:
            l.append(row)
        return l

    def delete_item_by_id(self, id_):
        pass

    def set_details_by_id(self, id_, hash_=None, mime_=None):
        pass

    def update_item_by_id(self, id_, mtime_, hash_, mime_):
        #print id_, mtime_, hash_, mime_
        cur = self.conn.cursor()
        e = '''UPDATE %s SET mtime=%f,hash='%s',mime='%s' WHERE i_id=%s''' % (self.name, mtime_, hash_, mime_, str(id_))
        #print e
        cur.execute(e)
        self.conn.commit()

    def update_item_size_by_id(self, read_size_, file_size_):
        cur = self.conn.cursor()
        e = '''UPDATE %s SET read_size=%d,file_size=%d WHERE i_id=%s''' % (self.name, read_size_, file_size_, str(id_))
        cur.execute(e)
        self.conn.commit()


    #def get_items(self):
    #    cur = self.conn.cursor()
    #    cur.execute('''SELECT hash ('%s', '%s', '%s')''' % (path_, hash_, mime_))
    #    self.conn.commit()


def indicate():
    DB_Manager.db_file = args["database"]
    dbm = DB_Manager()

    folder_ = os.path.abspath(args["indicate"])

    def walk_onerror(error):
        print "="*80
        print error
        print "="*80

    # walk through given folder
    itm = IndicateThread_Manager(args["threads"])
    for (root, dirs, files) in os.walk(folder_, onerror=walk_onerror):  # by default, does not follow links
        if not args["hidden"]:
            files = [f for f in files if not f[0] == '.']
            dirs[:] = [d for d in dirs if not d[0] == '.']
        for f in files:
            if f is args["database"]:
                continue
            fp = os.path.abspath(os.path.join(root, f))
            itm.put_into_queue(fp)
    itm.wait_till_finished()

    print "-----------------"
    print "found duplicates:"
    print ""
    for item in dbm.get_duplicates():
        print item


# delete items
# multiple selection
# confirmation message
# show list of files
# select files to delete
# check if duplicate is available/found
def delete_items():
    pass


def list_of_all_files(folder):
    pass


def read_mime_of_file(file_):
    return subprocess.Popen(["file", "--mime-type", "-b", file_], stdout=subprocess.PIPE).communicate()[0]


class IndicateThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        dbm = DB_Manager()
        while True:
            self.file_ = IndicateThread_Manager.queue.get()
            try:
                # get size of file
                s = os.stat(self.file_).st_size  # size of file in bytes
                READ_LIMIT = args["max_size"]
                #if s > READ_LIMIT and True:  # TODO: replace True with sys.argv[?]
                #    s = READ_LIMIT

                # get hash value
                h = hashlib.md5()
                with open(self.file_, "r") as f:
                    i = 0
                    b = 128
                    while True:
                        i += b
                        READ_BYTES = b
                        if i > READ_LIMIT:
                            READ_BYTES = i-READ_LIMIT
                        b128 = f.read(READ_BYTES)
                        if not b128:
                            break
                        h.update(b128)
                        if i >= READ_LIMIT:
                            break
                    print "="*5, "i:", i, "="*5

                h_hex = unicode(h.hexdigest())

                # get mtime value
                t = os.stat(self.file_).st_mtime

                # get mime value
                m = read_mime_of_file(self.file_)

                if dbm.add_item(self.file_, t, h.hexdigest(), m, READ_LIMIT, s):
                    print "add item"
                    pass
                else:
                    print "add item failed"
                    items = dbm.get_item_by_path(self.file_)
                    print "items:", items
                    for id_, path_, mtime_, hash_, mime_, rsize_, fsize_ in items:
                        if (t > mtime_) or (h_hex is not hash_):
                            print "update item"
                            dbm.update_item_by_id(id_, t, h_hex, m)
                        if (READ_LIMIT > rsize_) or (s is not fsize_):
                            dbm.update_item_size_by_id(id_, READ_LIMIT, s)
            except:
                print "error occured while working on file", self.file_
                traceback.print_exc()
            finally:
                IndicateThread_Manager.queue.task_done()


class IndicateThread_Manager():
    queue = Queue.Queue()

    def __init__(self, numb_of_threads=2):
        self.indi_threads = [IndicateThread() for i in range(numb_of_threads)]
        for t in self.indi_threads:
            t.setDaemon(True)
            t.start()

    def put_into_queue(self, fpath):
        IndicateThread_Manager.queue.put(fpath)

    def wait_till_finished(self):
        IndicateThread_Manager.queue.join()


def main():
    global args
    parser = argparse.ArgumentParser(description='Find duplicate files.')
    parser.add_argument('-o', '--output', default='-', help='output to file')
    parser.add_argument('-d', '--database', default=os.path.join(os.getenv("HOME"), '.dup.db'), help='set location of database')
    parser.add_argument('-i', '--indicate', default=os.getenv("HOME"), help='set folder to indicate')
    parser.add_argument('-t', '--threads', type=int, default=2, help='number of threads used to indicate (default: 2)')
    parser.add_argument('-f', '--filter', type=list, help='Ignore files with given mime type. This will speed up indication.')
    parser.add_argument('--max_size', type=int, default=1024**3, help='max size to read (in byte)')
    parser.add_argument('--hidden', action='store_true', help='also indicate hidden files')
    parser.add_argument('--dump', action='store_true', help='dump indication')
    parser.add_argument('mode', default='INDICATE', help='INDICATE, LIST, DUMP')
    args = vars(parser.parse_args())
    print args

    if 'delete' in args['mode'].lower():
        pass  # ids?
    else:
        if 'indicate' in args['mode'].lower():
            indicate()
        if 'list' in args['mode'].lower():
            pass
        if 'dump' in args['mode'].lower():
            pass

if __name__ == '__main__':
    main()
