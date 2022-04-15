import os
from pickle import TRUE
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_bcrypt import Bcrypt
from flask_login import LoginManager
from flask_mail import Mail
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool

from dotenv import load_dotenv
load_dotenv()

app = Flask(__name__)
app.config['SECRET_KEY'] = '5791628bb0b13ce0c676dfde280ba245'
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///site.db'
db = SQLAlchemy(app)
bcrypt = Bcrypt(app)
login_manager = LoginManager(app)
login_manager.login_view = 'login'
login_manager.login_message_category = 'info'
app.config['MAIL_SERVER'] = 'smtp.googlemail.com'
app.config['MAIL_PORT'] = 587
app.config['MAIL_USE_TLS'] = True
app.config['MAIL_USERNAME'] = os.environ.get('EMAIL_USER')
app.config['MAIL_PASSWORD'] = os.environ.get('EMAIL_PASS')
mail = Mail(app)

TESTING = True
if TESTING:
    DB_USER = os.environ.get('DB_USER_1')
    DB_PASSWORD = os.environ.get('DB_PASSWORD_1')
else:
    DB_USER = os.environ.get('DB_USER_2')
    DB_PASSWORD = os.environ.get('DB_PASSWORD_2')
DB_SERVER = os.environ.get('DB_SERVER')
#DB_SERVER = "w4111project1part2db.cisxo09blonu.us-east-1.rds.amazonaws.com"
#DATABASEURI = "postgresql://"+DB_USER+":"+DB_PASSWORD+"@"+DB_SERVER+"/w4111"
DATABASEURI = "postgresql://"+DB_USER+":"+DB_PASSWORD+"@"+DB_SERVER+"/proj1part2"
print(DATABASEURI)

engine = create_engine("postgresql://jy3174:JaceYJH@w4111-4-14.cisxo09blonu.us-east-1.rds.amazonaws.com/proj1part2")

from flaskblog import routes