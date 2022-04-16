import os
import secrets
from PIL import Image
from flask import render_template, url_for, flash, redirect, request, abort, g, session
from yealp_app import app, mail, engine
from yealp_app.forms import (RegistrationForm, LoginForm, UpdateAccountForm,
                             RequestResetForm, ResetPasswordForm,SearchForm,
                             ReviewForm, TipForm, AddBusinessPictureForm)
from yealp_app.models import Yealper
from yealp_app.database_utils import get_user
from flask_login import login_user, current_user, logout_user, login_required
from flask_mail import Message

import secrets
from datetime import date

def get_current_user():
    if not current_user.is_authenticated:
        return redirect(url_for('login'))
    else:
        return get_user(current_user.user_id)


@app.before_request
def before_request():
    """
    This function is run at the beginning of every web request 
    (every time you enter an address in the web browser).
    We use it to setup a database connection that can be used throughout the request

    The variable g is globally accessible
    """
    try:
        print("Try connecting...")
        g.conn = engine.connect()
        print("Connection is good!")
        pass
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

@app.route("/register", methods=['GET', 'POST'])
def register():
    def add_user_to_db(email, name, password, user_id=None, yealping_since=None):
        if not user_id:
            user_id = secrets.token_hex(11)
        if not yealping_since:
            yealping_since = date.today().strftime("%Y-%m-%d")
        g.conn.execute('''
            INSERT INTO Users (user_id, email, name, password, yealping_since) 
            VALUES (%s, %s, %s, %s, %s)''',
        (user_id, email, name, password, yealping_since))
        
    if current_user.is_authenticated:
        return redirect(url_for('home'))
    form = RegistrationForm()
    if form.validate_on_submit():
        #hashed_password = bcrypt.generate_password_hash(form.password.data).decode('utf-8')
        #user = User(username=form.username.data, email=form.email.data, password=hashed_password)
        #db.session.add(user)
        #db.session.commit()
        add_user_to_db(email=form.email.data, name=form.username.data, password=form.password.data)
        flash('Your account has been created! You are now able to log in', 'success')
        return redirect(url_for('login'))
    return render_template('register.html', title='Register', form=form)


@app.route("/login", methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('home'))
    form = LoginForm()
    if form.validate_on_submit():
        #user = User.query.filter_by(email=form.email.data).first()
        print('OK')
        user = engine.execute('''
            SELECT * 
            FROM Users
            WHERE email = %s
        ''', (form.email.data, )).fetchone()
        print(user)
        #if user and bcrypt.check_password_hash(user.password, form.password.data):
        if user and user['password'] == form.password.data:
            next_page = request.args.get('next')
            login_user(Yealper(user))
            return redirect(next_page) if next_page else redirect(url_for('search'))
        else:
            flash('Login Unsuccessful. Please check email and password', 'danger')
    return render_template('user/login.html', title='Login', form=form)


@app.route("/logout")
def logout():
    logout_user()
    return redirect(url_for('search'))


def save_picture(form_picture, path = 'static/profile_pics'):
    random_hex = secrets.token_hex(8)
    _, f_ext = os.path.splitext(form_picture.filename)
    picture_fn = random_hex + f_ext
    picture_path = os.path.join(app.root_path, path, picture_fn)

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
        picture_file = save_picture(form.picture.data) if form.picture.data else current_user.account_img_file
        
        g.conn.execute('''
            UPDATE Users 
            SET name = %s, email = %s, account_img_file = %s
            WHERE user_id = %s
            ''', (form.username.data, form.email.data, picture_file, current_user.user_id))
        print(123123)
        flash('Your account has been updated!', 'success')
        return redirect(url_for('account'))
    elif request.method == 'GET':
        form.username.data = current_user.name
        form.email.data = current_user.email
    image_file = url_for('static', filename='profile_pics/' + current_user.account_img_file)
    return render_template('user/account.html', title='Account',
                           image_file=image_file, form=form)


@app.route("/post/<int:post_id>")
def post(post_id):
    post = Post.query.get_or_404(post_id)
    return render_template('post.html', title=post.title, post=post)


# @app.route("/post/<int:post_id>/update", methods=['GET', 'POST'])
# @login_required
# def update_post(post_id):
#     post = Post.query.get_or_404(post_id)
#     if post.author != current_user:
#         abort(403)
#     form = PostForm()
#     if form.validate_on_submit():
#         post.title = form.title.data
#         post.content = form.content.data
#         db.session.commit()
#         flash('Your post has been updated!', 'success')
#         return redirect(url_for('post', post_id=post.id))
#     elif request.method == 'GET':
#         form.title.data = post.title
#         form.content.data = post.content
#     return render_template('create_post.html', title='Update Post',
#                            form=form, legend='Update Post')


def send_reset_email(user):
    token = user.get_reset_token()
    msg = Message('Password Reset Request',
                  sender='noreply@demo.com',
                  recipients=[user.email])
                  
    msg.body = f'''Hi {user.name}! This is the password reset instructions from Yealp platform. To reset your password, visit the following link:
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
        user = engine.execute('''
            SELECT * 
            FROM Users
            WHERE email = %s
        ''', (form.email.data, )).fetchone()
        send_reset_email(Yealper(user))
        flash('An email has been sent with instructions to reset your password.', 'info')
        return redirect(url_for('login'))
    return render_template('reset_request.html', title='Reset Password', form=form)


@app.route("/reset_password/<token>", methods=['GET', 'POST'])
def reset_token(token):
    if current_user.is_authenticated:
        return redirect(url_for('home'))
    user = Yealper.verify_reset_token(token)
    if user is None:
        flash('That is an invalid or expired token', 'warning')
        return redirect(url_for('reset_request'))
    form = ResetPasswordForm()
    if form.validate_on_submit():
        #hashed_password = bcrypt.generate_password_hash(form.password.data).decode('utf-8')
        #user.password = hashed_password
        #db.session.commit()
        g.conn.execute('''
            UPDATE Users 
            SET password = %s
            WHERE user_id = %s
            ''', (form.password.data, user.user_id))
        flash('Your password has been updated! You are now able to log in', 'success')
        return redirect(url_for('login'))
    return render_template('reset_token.html', title='Reset Password', form=form)

@app.route("/",methods=['GET', 'POST'])
@app.route('/home',methods=['GET', 'POST'])
def search():
    form = SearchForm()
    print(form)
    if form.validate_on_submit():
        state = form.state.data
        is_takeout_rule = form.is_takeout.data
        is_open_rule = form.is_open.data
        order_rule = form.order_rule.data
        return redirect(url_for('restaurants_searched', state = state, is_takeout_rule = is_takeout_rule, is_open_rule = is_open_rule, order_rule = order_rule))
    return render_template('search.html', form=form)

@app.route('/restaurants', methods=['GET', 'POST'])
def restaurants():
    form = SearchForm()
    if request.method == "POST":
        state = form.state.data
        is_takeout_rule = form.is_takeout.data
        is_open_rule = form.is_open.data
        order_rule = form.order_rule.data
        return redirect(url_for('restaurants_searched', state = state, is_takeout_rule = is_takeout_rule, is_open_rule = is_open_rule, order_rule = order_rule))
    else:
        bizs = g.conn.execute('''
            SELECT * FROM business_wide LIMIT 50
        ''').fetchall()
        return render_template('restaurants_main.html',  bizs = bizs, form = form)

@app.route('/restaurants/<string:state>/is_takeout:<string:is_takeout_rule>/is_open:<string:is_open_rule>/order_by:<string:order_rule>', methods=['GET', 'POST'])
def restaurants_searched(state, is_takeout_rule, is_open_rule, order_rule):
    form = SearchForm()
    print(form)
    print(form.state.data)
    print(form.is_open.data)

    if request.method == "POST":
        state = form.state.data
        is_takeout_rule = form.is_takeout.data
        is_open_rule = form.is_open.data
        order_rule = form.order_rule.data
        return redirect(url_for('restaurants_searched', state = state, is_takeout_rule = is_takeout_rule, 
                                is_open_rule = is_open_rule, order_rule = order_rule))
    else:
        form = SearchForm()
        form.order_rule.data = order_rule
        form.is_open.data = True if is_open_rule == 'True' else False
        form.is_takeout.data = True if is_takeout_rule == 'True' else False
        form.state.data = state
        is_takeout_SQL = "AND is_takeout = 'True'" if is_takeout_rule == 'True' else ""
        is_open_SQL = "AND is_open = 'True'" if is_open_rule == 'True' else ""
        order_rule_SQL = order_rule + " DESC" if order_rule != "name" else order_rule
        bizs = g.conn.execute(f'''
            SELECT *
            FROM business_wide
            where state = %s {is_takeout_SQL} {is_open_SQL}
            ORDER BY case when average_stars is null then 1 else 0 end, {order_rule_SQL}
            ''',(state, )).fetchall()
        if bizs is None:
            flash('No results! Randomly pick 50 restaurants:', 'fail')
            bizs = g.conn.execute('''
                SELECT * FROM business_wide LIMIT 50
            ''').fetchall()
        return render_template('restaurants_main.html',  bizs = bizs, form = form)

@app.route('/restaurant/<string:business_id>', methods=['GET', 'POST'])
def restaurant(business_id, show = 'review'):
    def CurrentUserIsFan():
        if not current_user.is_authenticated:
            return False
        is_fan = g.conn.execute('''
            SELECT 1 FROM 
            Users_favorite_Business 
            WHERE user_id = %s AND business_id = %s
            ''', (current_user.user_id, business_id)).fetchone()
        return True if is_fan else False
        
    restaurant = g.conn.execute('''
        SELECT * FROM business_wide WHERE business_id = %s
    ''', business_id).fetchone()
    # Load the review
    reader_uid = current_user.user_id if current_user.is_authenticated else ' '
    reviews =  g.conn.execute('''
        WITH current_user_upvote_status AS(
            SELECT review_id, 
                SUM(CASE WHEN upvote_type  = 'useful' THEN 1 ELSE 0 END) AS is_useful,
                SUM(CASE WHEN upvote_type  = 'cool' THEN 1 ELSE 0 END) AS is_cool,
                SUM(CASE WHEN upvote_type  = 'funny' THEN 1 ELSE 0 END) AS is_funny
            FROM Users_upvote_Review 
            WHERE user_id = %s
            GROUP BY review_id
        )
        SELECT * 
        FROM reviews_wide LEFT JOIN current_user_upvote_status USING(review_id)
        WHERE business_id = %s''', (reader_uid, business_id)).fetchall()
    tips = g.conn.execute('''
        WITH current_user_upvote_status AS(
            SELECT review_id, 
                SUM(CASE WHEN upvote_type  = 'like' THEN 1 ELSE 0 END) AS is_like
            FROM Users_upvote_Review 
            WHERE user_id = %s
            GROUP BY review_id
        )
        SELECT * 
        FROM tips_wide LEFT JOIN current_user_upvote_status USING(review_id)
        WHERE business_id = %s''', (reader_uid, business_id, )).fetchall()

    favorite = CurrentUserIsFan()
    collections = None

    if current_user.is_authenticated:
    # Load whether the user already like the restaurant
        collections = g.conn.execute('''
            WITH one_user_collections AS(
                SELECT user_id AS collection_owner_id, collection_id
                FROM Collection_of_User
                WHERE user_id = %s)            
            SELECT collection_id, COUNT(CASE WHEN business_id = %s THEN 1 ELSE NULL END) AS biz_in
            FROM one_user_collections 
            LEFT JOIN Collection_contain_Business USING(collection_owner_id, collection_id)
            GROUP BY collection_id
            ORDER BY collection_id''', (current_user.user_id, business_id)).fetchall()
        
        if request.method == "POST" and request.form.get('favorite_action') == 'Favorite the restaurant':
            print(request.form)
            g.conn.execute('''
                INSERT INTO Users_favorite_Business(user_id, business_id) 
                VALUES (%s, %s)''',
                (current_user.user_id, business_id))
            favorite = CurrentUserIsFan()
            #return render_template("restaurant.html", restaurant=restaurant, reviews=reviews, favorite=favorite, collections=collections)
            
        if request.method == "POST" and request.form.get('favorite_action') == 'Unfavorite the restaurant':
            print(request.form)
            g.conn.execute('''
                DELETE FROM Users_favorite_Business 
                WHERE user_id = %s AND business_id = %s''',
                (current_user.user_id, business_id))
            favorite = CurrentUserIsFan()
            #return render_template("restaurant.html", restaurant=restaurant, reviews=reviews, favorite=favorite, collections=collections)

        if request.method == "POST"  and 'collections_update' in request.form:
            current_collections = request.form.getlist('collections_update')
            # Delete all original collections
            g.conn.execute('''
                DELETE FROM Collection_contain_Business 
                WHERE collection_owner_id = %s AND business_id = %s''',
                (current_user.user_id, business_id))
            for collection in current_collections:
                g.conn.execute('''
                    INSERT INTO Collection_contain_Business(collection_owner_id, collection_id, business_id) VALUES(%s, %s, %s)''',
                    (current_user.user_id, int(collection), business_id))

            collections = g.conn.execute('''
                WITH one_user_collections AS(
                    SELECT user_id AS collection_owner_id, collection_id
                    FROM Collection_of_User
                    WHERE user_id = %s)
                SELECT collection_id, COUNT(CASE WHEN business_id = %s THEN 1 ELSE NULL END) AS biz_in
                FROM one_user_collections 
                LEFT JOIN Collection_contain_Business USING(collection_owner_id, collection_id)
                GROUP BY collection_id
                ORDER BY collection_id''', (current_user.user_id, business_id)).fetchall()
            #return render_template("restaurant.html", restaurant=restaurant, reviews=reviews, favorite=favorite, collections=collections)

    return render_template('restaurant.html', biz=restaurant, reviews=reviews, tips = tips, favorite=favorite, collections=collections, show = show)

@app.route('/restaurant/<string:business_id>/<string:show>/new_collection', methods=['GET', 'POST'])
@login_required
def create_collection_in_restaurant(business_id, show):

    created_date = date.today().strftime("%Y-%m-%d")
    current_collections = g.conn.execute('''
        SELECT * FROM Collection_of_User WHERE user_id = %s''',
        (current_user.user_id, )).fetchall()
    if current_collections:
        new_id = max([collection['collection_id'] for collection in current_collections]) + 1
    else:
        new_id = 1
    g.conn.execute('''
        INSERT INTO Collection_of_User(user_id, collection_id, created_date) 
        VALUES (%s, %s, %s)''',
        (current_user.user_id, new_id, created_date))    
    return redirect(url_for('restaurant', business_id = business_id, show = show))



@app.route('/favorites') 
@login_required
def user_favorites():
    bizs = g.conn.execute('''
        WITH user_favorite AS(
            SELECT business_id
            FROM Users_favorite_Business
            WHERE user_id = %s
        )
        SELECT * 
        FROM business JOIN user_favorite USING(business_id) 
        ORDER BY name
        ''', (current_user.user_id, )).fetchall()
    collections = g.conn.execute('''
        WITH one_fan_collection AS(
            SELECT followee_user_id AS collection_owner_id, collection_id
            FROM Users_follow_Collection
            WHERE fan_user_id = %s),
            
        collection AS(
            SELECT collection_owner_id AS user_id, collection_id, 
                CONCAT('Collection NO.', collection_id, ' with ', COUNT(*), ' restaurants') AS collection_name
            FROM one_fan_collection JOIN Collection_contain_Business USING(collection_owner_id, collection_id)
            GROUP BY collection_owner_id, collection_id
            ORDER BY COUNT(*) DESC)
            
        SELECT user_id AS owner_uid, collection_id, name as collection_owner_name, collection_name
        FROM collection JOIN Users USING(user_id)''', (current_user.user_id, )).fetchall()

    
    return render_template('user/favorites.html', bizs=bizs, collections=collections)  

@app.route("/user/<string:user_id>", methods=['GET', 'POST'])
def user_main(user_id):

    def CurrentUserIsFan():
        if not current_user.is_authenticated:
            return False
        is_fan = g.conn.execute('''
            SELECT * 
            FROM Users_follow_Users 
            WHERE follwee_user_id = %s AND fan_user_id = %s
            ''', (user_id, current_user.user_id)).fetchone()
        return True if is_fan else False
    
    def get_n_fans(user_id):
        n_fans = g.conn.execute('''
            SELECT COUNT(DISTINCT fan_user_id) AS n
            FROM Users_follow_Users
            WHERE follwee_user_id = %s''', (user_id, )).fetchone()['n']
        return n_fans

    user = get_user(user_id)
    # Load all the review sented by the mainpage user
    reviews = g.conn.execute('''
        WITH one_user AS(
            SELECT user_id, name AS username
            FROM Users
            WHERE user_id = %s),

        User_review AS(
            SELECT review_id, username
            FROM one_user JOIN Users_write_Review USING(user_id)
        )

        SELECT * 
        FROM User_review 
            JOIN Review_of_Business USING(review_id)
            JOIN Business USING(business_id)''', (user_id, )).fetchall()
    # Load all the collections created by the mainpage user
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

    # Load total amount of followers of the mainpage user
    n_fans = get_n_fans(user_id)

    is_fan = CurrentUserIsFan()
    
    # Follow button interaction
    if request.method == "POST":
        if not current_user.is_authenticated:
            return redirect(url_for('login'))

        # Execute the follow request
        if request.form.get('follow_action') == 'Follow this user':
            follow_since = date.today().strftime("%Y-%m-%d")
            g.conn.execute('''
                INSERT INTO Users_follow_Users(follwee_user_id, fan_user_id, follow_since) 
                VALUES (%s, %s, %s)''',
                (user_id, current_user.user_id, follow_since))
            is_fan = True
            n_fans = get_n_fans(user_id)
            return render_template('user/main.html', user=user, reviews=reviews, collections=collections, is_fan=is_fan, n_fans = n_fans)

        # Execute the Unfollow request
        if request.form.get('follow_action') == 'Unfollow this user':
            print((user_id, current_user.user_id))
            g.conn.execute('''
                DELETE FROM Users_follow_Users 
                WHERE follwee_user_id = %s AND fan_user_id = %s''',
                (user_id, current_user.user_id))
            is_fan = False
            n_fans = get_n_fans(user_id)
            return render_template('user/main.html', user=user, reviews=reviews, collections=collections, is_fan=is_fan, n_fans = n_fans)
    return render_template('user/main.html', user=user, reviews=reviews, collections=collections, is_fan=is_fan, n_fans = n_fans)

@app.route("/user/followees", methods=['GET', 'POST'])
def user_followees():
    cur_user = get_current_user()
    print(cur_user)
    followees = g.conn.execute('''
        WITH followees AS(
            SELECT follwee_user_id AS user_id
            FROM Users_follow_Users
            WHERE fan_user_id = %s)
        SELECT *
        FROM followees JOIN Users USING(user_id)
        ''',
        (cur_user['user_id'], )).fetchall()
    print(followees)
    return render_template('user/followees.html', followees=followees)

@app.route("/user/<string:user_id>/collections", methods=['GET', 'POST'])
def user_collections(user_id):
    def get_collections_by_user(user_id):
    # http://127.0.0.1:5000/user/9Xmw_WcUCShPD0qGO1UD7w/collection/1
        collections = g.conn.execute('''
            WITH collections AS(
                SELECT user_id AS collection_owner_id, collection_id
                FROM Collection_of_User
                WHERE user_id = %s)
            SELECT collection_owner_id, collection_id,
                CONCAT('Collection NO.', collection_id, ' with ', COUNT(business_id), ' restaurants') AS collection_name
            FROM collections LEFT JOIN Collection_contain_Business USING(collection_owner_id, collection_id)
            GROUP BY collection_owner_id, collection_id
            ORDER BY collection_id
            ''', 
            (user_id, )).fetchall()
        return collections
    user = get_user(user_id)
    collections = get_collections_by_user(user_id)
    
    # Create a new collection
    if request.method == "POST" and 'create_new_collection' in request.form:
        created_date = date.today().strftime("%Y-%m-%d")
        current_collections = g.conn.execute('''
            SELECT * FROM Collection_of_User WHERE user_id = %s''',
            (user_id, )).fetchall()
        if current_collections:
            new_id = max([collection['collection_id'] for collection in current_collections]) + 1
        else:
            new_id = 1
        g.conn.execute('''
            INSERT INTO Collection_of_User(user_id, collection_id, created_date) 
            VALUES (%s, %s, %s)''',
            (user_id, new_id, created_date))
        collections = get_collections_by_user(user_id)
        return render_template("user/collections.html", title='collections', user=user,  collections = collections)

    return render_template('user/collections.html', title='collections', user=user,  collections = collections)

@app.route("/user/<string:user_id>/collection/<int:collection_id>", methods=['GET', 'POST'])
def user_collection(user_id, collection_id):
    # http://127.0.0.1:5000/user/9Xmw_WcUCShPD0qGO1UD7w/collection/1

    def CurrentUserIsFan():
        if not current_user.is_authenticated:
            return False
        is_fan = g.conn.execute('''
            SELECT 1 FROM 
            Users_follow_Collection 
            WHERE fan_user_id = %s AND followee_user_id = %s AND collection_id = %s
            ''', (current_user.user_id, user_id, collection_id)).fetchone()
        return True if is_fan else False
    def get_collection():
        collection = g.conn.execute('''
        WITH one_collection AS(
            SELECT user_id AS collection_owner_id, collection_id, created_date
            FROM Collection_of_User
            WHERE user_id = %s AND collection_id = %s),
            
        one_collection_n_fan AS(
            SELECT a.collection_owner_id AS collection_owner_id, 
                a.collection_id AS collection_id, created_date,
                COUNT(DISTINCT fan_user_id) AS n_fans
            FROM one_collection a LEFT JOIN Users_follow_Collection b ON 
                a.collection_owner_id = b.followee_user_id AND
                a.collection_id = b.collection_id
            GROUP BY a.collection_owner_id, a.collection_id, created_date
        )

        SELECT CONCAT('Collection NO.', collection_id, ' with ', COUNT(business_id), ' restaurants') AS collection_name, created_date, n_fans
        FROM one_collection_n_fan 
            LEFT JOIN Collection_contain_Business USING(collection_owner_id, collection_id)
        GROUP BY collection_owner_id, collection_id, created_date, n_fans''', 
        (user_id, collection_id)).fetchone()
        return collection
    is_fan = CurrentUserIsFan()
    user = g.conn.execute('SELECT * FROM USERS WHERE user_id = %s', (user_id, )).fetchone()
    bizs_in_collection = g.conn.execute('''
        WITH bizs_in_collections AS(
            SELECT *
            FROM Collection_contain_Business
            WHERE collection_owner_id = %s AND collection_id = %s)
        SELECT *
        FROM bizs_in_collections JOIN Business USING(business_id)''', 
        (user_id, collection_id)).fetchall()
    collection = get_collection()

    # Follow button interaction
    if request.method == "POST":
        if not current_user.is_authenticated:
            return redirect(url_for('login'))

        # Execute the follow request
        if request.form.get('follow_button') == 'Follow the collection':
            g.conn.execute('''
                INSERT INTO Users_follow_Collection(fan_user_id, followee_user_id, collection_id) 
                VALUES (%s, %s, %s)''',
                (current_user.user_id, user_id, collection_id))
            is_fan = True
            collection = get_collection()
            return render_template('user/collection.html', title='collection', user=user, bizs=bizs_in_collection, collection=collection, is_fan=is_fan)

        # Execute the Unfollow request
        if request.form.get('follow_button') == 'Unfollow the collection':
            g.conn.execute('''
                DELETE FROM Users_follow_Collection 
                WHERE fan_user_id = %s AND followee_user_id = %s AND collection_id = %s''',
                (current_user.user_id, user_id, collection_id))
            is_fan = False
            collection = get_collection()
            return render_template('user/collection.html', title='collection', user=user, bizs=bizs_in_collection, collection=collection, is_fan=is_fan)

    return render_template('user/collection.html', title='collection', user=user, bizs=bizs_in_collection, collection=collection, is_fan=is_fan)


@app.route("/restaurant/<string:business_id>/review/new", methods=['GET', 'POST'])
@login_required
def create_review(business_id):
    form = ReviewForm()
    biz = g.conn.execute('SELECT * FROM Business WHERE business_id = %s', (business_id, )).fetchone()
    if form.validate_on_submit():
        max_review_id =  g.conn.execute('''
            SELECT MAX(review_id) as max_id FROM Review_of_Business''').fetchone()
        
        review_id = max_review_id['max_id'] + 1
        review_date = date.today().strftime("%Y-%m-%d")
        detailed_review = form.content.data
        n_star = form.star.data
        useful = funny = cool = 0
        # Input the reivew into the business
        g.conn.execute('''
            INSERT INTO Review_of_Business(review_id, review_date, business_id, detailed_review, stars, useful, funny, cool)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)''',
            (review_id, review_date, business_id, detailed_review, n_star, useful, funny, cool))
        # Log the users that creates it
        g.conn.execute('''
            INSERT INTO Users_write_Review(user_id, review_id)
            VALUES (%s, %s)''',
            (current_user.user_id, review_id))
        flash('Your review has been created!', 'success')
        return redirect(url_for('restaurant', business_id = business_id, show = 'review'))
    return render_template('review/create_detail_review.html', biz = biz, form = form, title='New Review', legend='Submit new review')


@app.route("/restaurant/<string:business_id>/tip/new", methods=['GET', 'POST'])
@login_required
def create_tip(business_id):
    form = TipForm()
    biz = g.conn.execute('SELECT * FROM Business WHERE business_id = %s', (business_id, )).fetchone()
    if form.validate_on_submit():
        max_review_id =  g.conn.execute('''
            SELECT MAX(review_id) as max_id FROM Review_of_Business''').fetchone()
        review_id = max_review_id['max_id'] + 1
        review_date = date.today().strftime("%Y-%m-%d")
        short_tip = form.content.data
        likes = 0
        # Input the reivew into the business
        g.conn.execute('''
            INSERT INTO Review_of_Business(review_id, review_date, business_id, short_tip, likes)
            VALUES (%s, %s, %s, %s, %s)''',
            (review_id, review_date, business_id, short_tip, likes))
        # Log the users that creates it
        g.conn.execute('''
            INSERT INTO Users_write_Review(user_id, review_id)
            VALUES (%s, %s)''',
            (current_user.user_id, review_id))
        flash('Your tip has been created!', 'success')
        return redirect(url_for('restaurant', business_id = business_id, show = 'tip'))
    return render_template('review/create_short_tip.html', form = form, biz = biz, title='New Tip', legend='Submit new short tip')

@app.route("/restaurant/<string:business_id>/create_biz_photo", methods=['GET', 'POST'])
@login_required
def create_biz_photo(business_id):
    form = AddBusinessPictureForm()

    biz = g.conn.execute('SELECT * FROM Business WHERE business_id = %s', (business_id, )).fetchone()
    if form.validate_on_submit():
        if form.picture.data:
            picture_file = save_picture(form.picture.data, path = 'static/business_photos')
            
            g.conn.execute('''
                INSERT INTO Photo_contained_Business(photo_id, business_id) 
                VALUES(%s, %s)
                ''', (picture_file.replace(".jpg", ""), business_id))
            print(123123)
            flash('Your pictures has been uploaded!', 'success')
            return redirect(url_for('restaurant', business_id = business_id))
        else:
            flash('Please upload a picture!', 'danger')
            return render_template('restaurant/add_picture.html', title='Add Picture', biz=biz, form=form)
    return render_template('restaurant/add_picture.html', title='Add Picture', biz=biz, form=form)
    


@app.route("/restaurant/<string:business_id>/review/<string:review_id>/delete", methods=('POST',))
@login_required
def delete_review(business_id, review_id):
    print(business_id, review_id)
    g.conn.execute('''
        DELETE FROM Review_of_Business
        WHERE review_id = %s
        ''', (review_id, ))
    flash('Your review has been deleted!', 'danger')
    return redirect(url_for('restaurant', business_id=business_id))

@app.route("/restaurant/<string:business_id>/review/<string:review_id>/upvote/<string:upvote_type>", methods=('POST', 'GET'))
@login_required
def upvote_review(business_id, review_id, upvote_type):
    
    def is_upvote():
        return g.conn.execute('''
            SELECT user_id, review_id, upvote_type 
            FROM Users_upvote_Review 
            WHERE review_id = %s AND upvote_type = %s AND user_id = %s
            ''', (review_id, upvote_type if upvote_type != 'likes' else 'like', current_user.user_id)).fetchall()
    if not is_upvote():
        
        g.conn.execute('''
            INSERT INTO Users_upvote_Review(user_id, review_id, upvote_type) VALUES(%s, %s, %s)
        ''', (current_user.user_id, review_id, upvote_type if upvote_type != 'likes' else 'like'))
        g.conn.execute(f'''
            UPDATE Review_of_Business 
            SET {upvote_type} = {upvote_type} + 1 
            WHERE review_id = %s
            ''', (review_id, ))
    else:
        g.conn.execute('''
            DELETE FROM Users_upvote_Review
            WHERE user_id = %s AND review_id = %s AND upvote_type = %s
        ''', (current_user.user_id, review_id, upvote_type if upvote_type != 'likes' else 'like'))
        g.conn.execute(f'''
            UPDATE Review_of_Business 
            SET {upvote_type} = {upvote_type} - 1 
            WHERE review_id = %s
            ''', (review_id, ))
    return redirect(url_for('restaurant', business_id=business_id, show = 'review'))