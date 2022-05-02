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
