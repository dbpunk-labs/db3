#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#

from sqlalchemy import create_engine
engine = create_engine("mysql+pymysql://root:pass@127.0.0.1:9292/db2")
with engine.connect() as conn:
    #cursor = conn.cursor()
    for r in conn.execute("select * from device_signal limit 10"):
        print(r)
