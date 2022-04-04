import os
from sqlalchemy import *
from sqlalchemy.pool import NullPool

tmpl_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')


DB_USER = "jy3174"
DB_PASSWORD = "JaceYJH"

DB_SERVER = "w4111.cisxo09blonu.us-east-1.rds.amazonaws.com"

#DATABASEURI = "postgresql://"+DB_USER+":"+DB_PASSWORD+"@"+DB_SERVER+"/w4111"
DATABASEURI = "postgresql://"+DB_USER+":"+DB_PASSWORD+"@"+DB_SERVER+"/proj1part2"
engine = create_engine(DATABASEURI)

engine.execute("""DROP TABLE IF EXISTS posts;""")
engine.execute("""CREATE TABLE posts (
    id SERIAL PRIMARY KEY,
    created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    title TEXT NOT NULL,
    content TEXT NOT NULL
);""")

# 插入两条文章
engine.execute("INSERT INTO posts (title, content) VALUES (%s, %s)",
            ('学习Flask1', '跟麦叔学习flask第一部分')
            )

engine.execute("INSERT INTO posts (title, content) VALUES (%s, %s)",
            ('学习Flask2', '跟麦叔学习flask第二部分')
            )
