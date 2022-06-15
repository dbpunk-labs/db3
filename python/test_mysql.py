#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#

import pymysql.cursors
from datetime import datetime
import random

connection = pymysql.connect(host='127.0.0.1',
                             user='root',
                             password="xxx",
                             database='db1',
                             port=9292,
                             cursorclass=pymysql.cursors.DictCursor)
with connection:
    with connection.cursor() as cursor:
        #cursor.execute("create table device_signal(ts timestamp, device_id varchar(256), signal int);")
        # Create a new record
        ts = datetime.now().timestamp()
        for i in range(10000 * 100):
            dt = datetime.fromtimestamp(ts + i)
            sql = "INSERT INTO device_signal VALUES ('%s', 'd_%d', %d);"%(dt.strftime("%Y-%m-%d %H:%M:%S"), random.randrange(1,100000), random.randrange(1, 100))
            print(sql)
            cursor.execute(sql)
            break
