from datetime import datetime
from itsdangerous import TimedJSONWebSignatureSerializer as Serializer
from yealp_app import db, login_manager, app, engine
from flask_login import UserMixin
from flask import g


@login_manager.user_loader
def load_user(user_id):
    conn = engine.connect()
    user = conn.execute('''
        SELECT *
        FROM Users
        WHERE user_id = %s''', (user_id, )).fetchone()
    if user:
        return Yealper(user)
    else:
        return None

class Yealper(object):
    """Wraps User object for Flask-Login"""
    def __init__(self, user):
        '''
        user: a row of instance from Users table
        '''
        self.user_id = user['user_id']
        self.name = user['name']
        self.email = user['email']
        self.yealping_since = user['yealping_since']
        self.account_img_file = user['account_img_file']

    def get_id(self):
        return self.user_id

    def is_active(self):
        return self._user.enabled

    def is_anonymous(self):
        return False

    def is_authenticated(self):
        return True

    def get_reset_token(self, expires_sec=1800):
        s = Serializer(app.config['SECRET_KEY'], expires_sec)
        return s.dumps({'user_id': self.user_id}).decode('utf-8')

    @staticmethod
    def verify_reset_token(token):
        s = Serializer(app.config['SECRET_KEY'])
        try:
            user_id = s.loads(token)['user_id']
        except:
            return None
        return g.conn.execute('SELECT * FROM USERS WHERE user_id = %s', (user_id, )).fetchone()

class User(db.Model, UserMixin):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(20), unique=True, nullable=False)
    email = db.Column(db.String(120), unique=True, nullable=False)
    image_file = db.Column(db.String(20), nullable=False, default='default.jpg')
    password = db.Column(db.String(60), nullable=False)
    posts = db.relationship('Post', backref='author', lazy=True)

    def get_reset_token(self, expires_sec=1800):
        s = Serializer(app.config['SECRET_KEY'], expires_sec)
        return s.dumps({'user_id': self.id}).decode('utf-8')

    @staticmethod
    def verify_reset_token(token):
        s = Serializer(app.config['SECRET_KEY'])
        try:
            user_id = s.loads(token)['user_id']
        except:
            return None
        return User.query.get(user_id)

    def __repr__(self):
        return f"User('{self.username}', '{self.email}', '{self.image_file}')"


class Post(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(100), nullable=False)
    date_posted = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    content = db.Column(db.Text, nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)

    def __repr__(self):
        return f"Post('{self.title}', '{self.date_posted}')"
