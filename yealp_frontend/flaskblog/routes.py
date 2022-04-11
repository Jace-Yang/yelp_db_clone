import os
import secrets
from PIL import Image
from flask import render_template, url_for, flash, redirect, request, abort, g
from flaskblog import app, db, bcrypt, mail, engine
from flaskblog.forms import (RegistrationForm, LoginForm, UpdateAccountForm,
                             PostForm, RequestResetForm, ResetPasswordForm)
from flaskblog.models import User, Post
from flask_login import login_user, current_user, logout_user, login_required
from flask_mail import Message


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


@app.route("/")
@app.route("/home")
def home():
    page = request.args.get('page', 1, type=int)
    posts = Post.query.order_by(Post.date_posted.desc()).paginate(page=page, per_page=5)
    return render_template('home.html', posts=posts)


@app.route("/about")
def about():
    return render_template('about.html', title='About')


@app.route("/register", methods=['GET', 'POST'])
def register():
    if current_user.is_authenticated:
        return redirect(url_for('home'))
    form = RegistrationForm()
    if form.validate_on_submit():
        hashed_password = bcrypt.generate_password_hash(form.password.data).decode('utf-8')
        user = User(username=form.username.data, email=form.email.data, password=hashed_password)
        db.session.add(user)
        db.session.commit()
        flash('Your account has been created! You are now able to log in', 'success')
        return redirect(url_for('login'))
    return render_template('register.html', title='Register', form=form)


@app.route("/login", methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('home'))
    form = LoginForm()
    if form.validate_on_submit():
        user = User.query.filter_by(email=form.email.data).first()
        if user and bcrypt.check_password_hash(user.password, form.password.data):
            login_user(user, remember=form.remember.data)
            next_page = request.args.get('next')
            return redirect(next_page) if next_page else redirect(url_for('home'))
        else:
            flash('Login Unsuccessful. Please check email and password', 'danger')
    return render_template('login.html', title='Login', form=form)


@app.route("/logout")
def logout():
    logout_user()
    return redirect(url_for('home'))


def save_picture(form_picture):
    random_hex = secrets.token_hex(8)
    _, f_ext = os.path.splitext(form_picture.filename)
    picture_fn = random_hex + f_ext
    picture_path = os.path.join(app.root_path, 'static/profile_pics', picture_fn)

    output_size = (125, 125)
    i = Image.open(form_picture)
    i.thumbnail(output_size)
    i.save(picture_path)
    return picture_fn


@app.route("/account", methods=['GET', 'POST'])
@login_required
def account():
    form = UpdateAccountForm()
    if form.validate_on_submit():
        if form.picture.data:
            picture_file = save_picture(form.picture.data)
            current_user.image_file = picture_file
        current_user.username = form.username.data
        current_user.email = form.email.data
        db.session.commit()
        flash('Your account has been updated!', 'success')
        return redirect(url_for('account'))
    elif request.method == 'GET':
        form.username.data = current_user.username
        form.email.data = current_user.email
    image_file = url_for('static', filename='profile_pics/' + current_user.image_file)
    return render_template('account.html', title='Account',
                           image_file=image_file, form=form)


@app.route("/post/new", methods=['GET', 'POST'])
@login_required
def new_post():
    form = PostForm()
    if form.validate_on_submit():
        post = Post(title=form.title.data, content=form.content.data, author=current_user)
        db.session.add(post)
        db.session.commit()
        flash('Your post has been created!', 'success')
        return redirect(url_for('home'))
    return render_template('create_post.html', title='New Post',
                           form=form, legend='New Post')

@app.route("/post/<int:post_id>")
def post(post_id):
    post = Post.query.get_or_404(post_id)
    return render_template('post.html', title=post.title, post=post)


@app.route("/post/<int:post_id>/update", methods=['GET', 'POST'])
@login_required
def update_post(post_id):
    post = Post.query.get_or_404(post_id)
    if post.author != current_user:
        abort(403)
    form = PostForm()
    if form.validate_on_submit():
        post.title = form.title.data
        post.content = form.content.data
        db.session.commit()
        flash('Your post has been updated!', 'success')
        return redirect(url_for('post', post_id=post.id))
    elif request.method == 'GET':
        form.title.data = post.title
        form.content.data = post.content
    return render_template('create_post.html', title='Update Post',
                           form=form, legend='Update Post')


@app.route("/post/<int:post_id>/delete", methods=['POST'])
@login_required
def delete_post(post_id):
    post = Post.query.get_or_404(post_id)
    if post.author != current_user:
        abort(403)
    db.session.delete(post)
    db.session.commit()
    flash('Your post has been deleted!', 'success')
    return redirect(url_for('home'))


@app.route("/bloguser/<string:username>")
def user_posts(username):
    page = request.args.get('page', 1, type=int)
    user = User.query.filter_by(username=username).first_or_404()
    posts = Post.query.filter_by(author=user)\
        .order_by(Post.date_posted.desc())\
        .paginate(page=page, per_page=5)
    return render_template('user_posts.html', posts=posts, user=user)


def send_reset_email(user):
    token = user.get_reset_token()
    msg = Message('Password Reset Request',
                  sender='noreply@demo.com',
                  recipients=[user.email])
    msg.body = f'''To reset your password, visit the following link:
{url_for('reset_token', token=token, _external=True)}

If you did not make this request then simply ignore this email and no changes will be made.
'''
    mail.send(msg)


@app.route("/reset_password", methods=['GET', 'POST'])
def reset_request():
    if current_user.is_authenticated:
        return redirect(url_for('home'))
    form = RequestResetForm()
    if form.validate_on_submit():
        user = User.query.filter_by(email=form.email.data).first()
        send_reset_email(user)
        flash('An email has been sent with instructions to reset your password.', 'info')
        return redirect(url_for('login'))
    return render_template('reset_request.html', title='Reset Password', form=form)


@app.route("/reset_password/<token>", methods=['GET', 'POST'])
def reset_token(token):
    if current_user.is_authenticated:
        return redirect(url_for('home'))
    user = User.verify_reset_token(token)
    if user is None:
        flash('That is an invalid or expired token', 'warning')
        return redirect(url_for('reset_request'))
    form = ResetPasswordForm()
    if form.validate_on_submit():
        hashed_password = bcrypt.generate_password_hash(form.password.data).decode('utf-8')
        user.password = hashed_password
        db.session.commit()
        flash('Your password has been updated! You are now able to log in', 'success')
        return redirect(url_for('login'))
    return render_template('reset_token.html', title='Reset Password', form=form)


@app.route('/restaurants') 
def restaurants():
    bizs = g.conn.execute('''
    SELECT * FROM business LIMIT 50
    ''').fetchall()
    return render_template('restaurants_main.html', bizs=bizs)  


@app.route('/restaurants/<string:business_id>')
def restaurant(business_id):
    restaurant = g.conn.execute('SELECT * FROM business WHERE business_id = %s', (business_id, )).fetchone()
    # Load the review
    reviews = g.conn.execute('''
        WITH one_restaurant AS (
            SELECT *
            FROM Review_of_Business
            WHERE business_id = %s AND detailed_review IS NOT NULL
            LIMIT 50)
        SELECT Users.name as username, short_tip, stars, user_id,
               detailed_review, review_date,
               useful, funny, cool	
        FROM one_restaurant
            LEFT JOIN Users_write_Review USING(review_id)
            LEFT JOIN Users USING(user_id)
        ORDER BY review_date DESC''', (business_id, )).fetchall()

    #form = LoginForm()
    if form.validate_on_submit():
        user = User.query.filter_by(email=form.email.data).first()
        if user and bcrypt.check_password_hash(user.password, form.password.data):
            login_user(user, remember=form.remember.data)
            
    return render_template('restaurant.html', restaurant=restaurant, reviews=reviews)


@app.route("/user/<string:user_id>")
@app.route("/user/<string:user_id>/")
def user_reviews(user_id):
    #user_id = 'UCl94KiDnyHOSHYgGHjLJA'
    print(10)
    user = g.conn.execute('SELECT * FROM USERS WHERE user_id = %s', (user_id, )).fetchone()
    # Load the review
    reviews = g.conn.execute('''
        WITH one_user AS(
            SELECT user_id
            FROM Users
            WHERE user_id = %s),

        User_review AS(
            SELECT review_id
            FROM one_user JOIN Users_write_Review USING(user_id)
        )

        SELECT * 
        FROM User_review JOIN Review_of_Business USING(review_id)''', (user_id, )).fetchall()

    collections = g.conn.execute('''
        WITH one_user AS(
            SELECT user_id AS collection_owner_id, collection_id
            FROM Collection_of_User
            WHERE user_id = %s)

        SELECT collection_owner_id, collection_id, 
        CONCAT('Collection NO.', collection_id, ' with ', COUNT(*), ' restaurants') AS collection_name
        FROM one_user 
            JOIN Collection_contain_Business USING(collection_owner_id, collection_id)
        GROUP BY collection_owner_id, collection_id
        ORDER BY collection_id''', (user_id, )).fetchall()
    return render_template('user_reviews.html', user=user, reviews=reviews, collections=collections)


@app.route("/user/<string:user_id>/collection/<int:collection_id>")
def user_collection(user_id, collection_id):
    # http://127.0.0.1:5000/user/9Xmw_WcUCShPD0qGO1UD7w/collection/1
    user = g.conn.execute('SELECT * FROM USERS WHERE user_id = %s', (user_id, )).fetchone()
    bizs_in_collection = g.conn.execute('''
        WITH bizs_in_collections AS(
            SELECT *
            FROM Collection_contain_Business
            WHERE collection_owner_id = %s AND collection_id = %s)
        SELECT *
        FROM bizs_in_collections JOIN Business USING(business_id)''', 
        (user_id, collection_id)).fetchall()
    collection_name = g.conn.execute('''
        WITH one_collection AS(
            SELECT user_id AS collection_owner_id, collection_id
            FROM Collection_of_User
            WHERE user_id = %s AND collection_id = %s)

        SELECT CONCAT('Collection NO.', collection_id, ' with ', COUNT(*), ' restaurants') AS collection_name
        FROM one_collection 
            JOIN Collection_contain_Business USING(collection_owner_id, collection_id)
        GROUP BY collection_owner_id, collection_id''', 
        (user_id, collection_id)).fetchone()['collection_name']
    print(bizs_in_collection)
    return render_template('user/user_collection.html', user=user,  bizs=bizs_in_collection, collection_name=collection_name)