import os
from sqlalchemy import *
from sqlalchemy.pool import NullPool
from flask import Flask, request, render_template, g, redirect, Response, url_for, flash

app = Flask(__name__)
app.config['SECRET_KEY'] = 'maishu is fat again, 555'

DB_USER = "jy3174"
DB_PASSWORD = "JaceYJH"

DB_SERVER = "w4111.cisxo09blonu.us-east-1.rds.amazonaws.com"

#DATABASEURI = "postgresql://"+DB_USER+":"+DB_PASSWORD+"@"+DB_SERVER+"/w4111"
DATABASEURI = "postgresql://"+DB_USER+":"+DB_PASSWORD+"@"+DB_SERVER+"/proj1part2"
DATABASEURI = "postgresql://jy3174:JaceYJH@w4111.cisxo09blonu.us-east-1.rds.amazonaws.com/proj1part2"
engine = create_engine(DATABASEURI)


@app.before_request
def before_request():
  """
  This function is run at the beginning of every web request 
  (every time you enter an address in the web browser).
  We use it to setup a database connection that can be used throughout the request

  The variable g is globally accessible
  """
  try:
    g.conn = engine.connect()
  except:
    print("uh oh, problem connecting to database")
    import traceback; traceback.print_exc()
    g.conn = None

@app.teardown_request
def teardown_request(exception):
  """
  At the end of the web request, this makes sure to close the database connection.
  If you don't the database could run out of memory!
  """
  try:
    g.conn.close()
  except Exception as e:
    pass

# 创建一个函数用来获取数据库链接
def get_db_connection():
    # 创建数据库链接到database.db文件
    # conn = sqlite3.connect('database.db')
    # # 设置数据的解析方法，有了这个设置，就可以像字典一样访问每一列数据
    # conn.row_factory = sqlite3.Row
    return g.conn

# 根据post_id从数据库中获取post
def get_post(post_id):
    conn = get_db_connection()
    post = conn.execute('SELECT * FROM posts WHERE id = %s',
                        (post_id,)).fetchone()
    #conn.close()
    return post


# @app.route('/') 
# def index():
#     # 调用上面的函数，获取链接
#     conn = get_db_connection()
#     # 查询所有数据，放到变量posts中
#     posts = conn.execute('SELECT * FROM posts').fetchall()

#     # names = []
#     # for result in posts:
#     #     names.append(result['name'])  # can also be accessed using result[0]
#     # posts.close()
#     # print(names)

#     #conn.close()
#     #把查询出来的posts传给网页
#     return render_template('index.html', posts=posts)  


@app.route('/') 
def index():
    # 调用上面的函数，获取链接
    conn = get_db_connection()
    # 查询所有数据，放到变量posts中
    bizs = conn.execute('SELECT * FROM business LIMIT 10').fetchall()
    return render_template('index2.html', bizs=bizs)  




@app.route('/posts/<int:post_id>')
def post(post_id):
    post = get_post(post_id)
    return render_template('post.html', post=post)


@app.route('/posts/new', methods=('GET', 'POST'))
def new():
    if request.method == 'POST':
        title = request.form['title']
        content = request.form['content']

        if not title:
            flash('标题不能为空!')
        elif not content:
            flash('内容不能为空')
        else:
            conn = get_db_connection()
            conn.execute('INSERT INTO posts (title, content) VALUES (%s, %s)',
                         (title, content))
            #conn.commit()
            #conn.close()
            return redirect(url_for('index'))

    return render_template('new.html')


@app.route('/posts/<int:id>/edit', methods=('GET', 'POST'))
def edit(id):
    post = get_post(id)

    if request.method == 'POST':
        title = request.form['title']
        content = request.form['content']

        if not title:
            flash('Title is required!')
        else:
            conn = get_db_connection()
            conn.execute('UPDATE posts SET title = %s, content = %s'
                         ' WHERE id = %s',
                         (title, content, id))
            #conn.commit()
            #conn.close()
            return redirect(url_for('index'))

    return render_template('edit.html', post=post)


@app.route('/posts/<int:id>/delete', methods=('POST',))
def delete(id):
    post = get_post(id)
    conn = get_db_connection()
    conn.execute('DELETE FROM posts WHERE id = %s', (id,))
    #conn.commit()
    #conn.close()
    flash('"{}" 删除成功!'.format(post['title']))
    return redirect(url_for('index'))

@app.route('/about')
def about():
    return render_template('about.html')





if __name__ == "__main__":
  import click

  @click.command()
  @click.option('--debug', is_flag=True)
  @click.option('--threaded', is_flag=True)
  @click.argument('HOST', default='0.0.0.0')
  @click.argument('PORT', default=8111, type=int)
  def run(debug, threaded, host, port):
    """
    This function handles command line parameters.
    Run the server using

        python server.py

    Show the help text using

        python server.py --help

    """

    HOST, PORT = host, port
    print("running on %s:%d" % (HOST, PORT))
    app.run(host=HOST, port=PORT, debug=debug, threaded=threaded)
  run()
