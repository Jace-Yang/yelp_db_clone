from email.policy import default
import json
from flask_wtf import FlaskForm
from flask_wtf.file import FileField, FileAllowed
from flask_login import current_user
from wtforms import StringField, PasswordField, SubmitField, BooleanField, TextAreaField, SelectField
from wtforms.validators import DataRequired, Length, Email, EqualTo, ValidationError
from flaskblog.models import User
from flaskblog import app, db,engine
from flask import render_template, url_for, flash, redirect, request, abort, g, session

class RegistrationForm(FlaskForm):
    username = StringField('Your name',
                           validators=[DataRequired(), Length(min=2, max=20)])
    email = StringField('Email',
                        validators=[DataRequired(), Email()])
    password = PasswordField('Password', validators=[DataRequired(), Length(min=8, max=16)])
    confirm_password = PasswordField('Confirm Password',
                                     validators=[DataRequired(), EqualTo('password')])
    submit = SubmitField('Sign Up')

    def validate_email(self, email):
        user = g.conn.execute('SELECT * FROM USERS WHERE email = %s', (email.data, )).fetchone()
        if user:
            raise ValidationError('That email is taken. Please choose a different one.')


class LoginForm(FlaskForm):
    email = StringField('Email',
                        validators=[DataRequired(), Email()])
    password = PasswordField('Password', validators=[DataRequired()])
    remember = BooleanField('Remember Me')
    submit = SubmitField('Login')


class CollectionForm(FlaskForm):
    collect = BooleanField('Collect')

class Favorite(FlaskForm):
    collect = BooleanField('Favorite')


class UpdateAccountForm(FlaskForm):
    username = StringField('Username',
                           validators=[DataRequired(), Length(min=2, max=20)])
    email = StringField('Email',
                        validators=[DataRequired(), Email()])
    picture = FileField('Update Profile Picture', validators=[FileAllowed(['jpg', 'png'])])
    submit = SubmitField('Update')

    def validate_email(self, email):
        user = g.conn.execute('SELECT * FROM USERS WHERE email = %s', (email.data, )).fetchone()
        if user is None:
            raise ValidationError('There is no account with that email. You must register first.')

class AddBusinessPictureForm(FlaskForm):
    picture = FileField('Submit new picture of jpg type', validators=[FileAllowed(['jpg'])])
    submit = SubmitField('Add photo')

class ReviewForm(FlaskForm):
    STARTS_OPTIONS = [(i/2, str(i/2) + ' stars' if i != 2 else str(i/2) + ' star') for i in list(range(11))]
    star = SelectField('Stars', choices=STARTS_OPTIONS,  coerce=float)
    content = TextAreaField('Content', validators=[DataRequired(), Length(min=30)])
    submit = SubmitField('Post')

class TipForm(FlaskForm):
    content = TextAreaField('Content', validators=[DataRequired()])
    submit = SubmitField('Post')

class SearchForm(FlaskForm):

    with open("../yealp_frontend/flaskblog/static/STATES_OPTIONS.json", 'r') as f:
        STATES_OPTIONS = json.load(f)
        STATES_OPTIONS = [(state, state) for state in STATES_OPTIONS]
    
    state = SelectField('States', choices=STATES_OPTIONS, default = STATES_OPTIONS[0])
    order_rule = SelectField('Order by', choices=[('name', 'Name'), ('average_stars', 'Stars'), ('n_detailed_review', 'Number of reviews')], default='Name')
    is_takeout = BooleanField('Allow takeout')
    is_open = BooleanField('Is opening')
    submit = SubmitField('Search')


class RequestResetForm(FlaskForm):
    email = StringField('Email',
                        validators=[DataRequired(), Email()])
    submit = SubmitField('Request Password Reset')

    def validate_email(self, email):
        user = g.conn.execute('SELECT * FROM USERS WHERE email = %s', (email.data, )).fetchone()
        if user is None:
            raise ValidationError('There is no account with that email. You must register first.')


class ResetPasswordForm(FlaskForm):
    password = PasswordField('Password', validators=[DataRequired()])
    confirm_password = PasswordField('Confirm Password',
                                     validators=[DataRequired(), EqualTo('password')])
    submit = SubmitField('Reset Password')
