#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pyodbc
import sqlite3
from collections import namedtuple
import re
import sys
import os

usage = '''
Usage: mdb2sqlite.py <input.mdb> <output.sqlite>
'''

fileIn = os.path.abspath(sys.argv[-2])
fileOut = sys.argv[-1]

cnxn = pyodbc.connect('DRIVER={{Microsoft Access Driver (*.mdb)}};DBQ={};'.format(fileIn))

cursor = cnxn.cursor()

conn = sqlite3.connect(fileOut)
c = conn.cursor()

Table = namedtuple('Table', ['cat', 'schem', 'name', 'type'])

# get a list of tables
tables = []
for row in cursor.tables():
    if row.table_type == 'TABLE':
        t = Table(row.table_cat, row.table_schem, row.table_name, row.table_type)
        tables.append(t)

for t in tables:
    try:
        print(t.name)
        # SQLite tables must being with a character or _
        t_name = t.name
        if not re.match('[a-zA-Z]', t.name):
            t_name = '_' + t_name
        if t_name == 'Order':
            t_name = '_' + t_name

        # get table definition
        columns = []
        for row in cursor.columns(table=t.name):
            # for row in cursor.columns(t.name.decode('utf-16-le')):
            print('    {} [{}({})]'.format(row.column_name, row.type_name, row.column_size))
            col_name = re.sub('[^a-zA-Z0-9]', '_', row.column_name)
            # Forcing columns being with a character or _
            if not re.match('[a-zA-Z]', col_name):
                col_name = '_' + col_name
            if col_name == 'Order':
                col_name = '_' + col_name
            
            columns.append('{} {}({})'.format(col_name, row.type_name, row.column_size))
        cols = ', '.join(columns)
        print(cols)
    except:
        e = sys.exc_info()[0]
        print()
        print('Error getting table definitions on {} : {}'.format(t_name, cols))
        print(e)
        continue
        
    try:
        # create the table in SQLite
        c.execute('DROP TABLE IF EXISTS "{}"'.format(t_name))
        c.execute('CREATE TABLE "{}" ({})'.format(t_name, cols))
    except:
        e = sys.exc_info()[0]
        print()
        print('Error creating SQLite table {} : {}'.format(t_name, cols))
        print(e)
        continue
        
    try:
        # copy the data from MDB to SQLite
        cursor.execute('SELECT * FROM "{}"'.format(t.name))
        for row in cursor:
            values = []
            for value in row:
                if value is None:
                    values.append(None)
                else:
                    if isinstance(value, bytearray):
                        value = sqlite3.Binary(value)
                    else:
                        value = u'{}'.format(value)
                    values.append(value)
            v = ', '.join(['?']*len(values))
            sql = 'INSERT INTO "{}" VALUES(' + v + ')'
            c.execute(sql.format(t_name), values)
            conn.commit()
        conn.commit()
    except:
        e = sys.exc_info()[0]
        print()
        print('Error copying data from MDB to SQLite at {} : {}'.format(t_name, values))
        print(e)
        continue
        
conn.commit()
conn.close()
