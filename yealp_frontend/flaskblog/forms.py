from flask_wtf import FlaskForm
from flask_wtf.file import FileField, FileAllowed
from flask_login import current_user
from wtforms import StringField, PasswordField, SubmitField, BooleanField, TextAreaField, SelectField
from wtforms.validators import DataRequired, Length, Email, EqualTo, ValidationError
from flaskblog.models import User
from flaskblog import app, db,engine
from flask import render_template, url_for, flash, redirect, request, abort, g, session

class RegistrationForm(FlaskForm):
    username = StringField('Username',
                           validators=[DataRequired(), Length(min=2, max=20)])
    email = StringField('Email',
                        validators=[DataRequired(), Email()])
    password = PasswordField('Password', validators=[DataRequired(), Length(min=8, max=16)])
    confirm_password = PasswordField('Confirm Password',
                                     validators=[DataRequired(), EqualTo('password')])
    submit = SubmitField('Sign Up')

    def validate_username(self, username):
        user = User.query.filter_by(username=username.data).first()
        if user:
            raise ValidationError('That username is taken. Please choose a different one.')

    def validate_email(self, email):
        user = User.query.filter_by(email=email.data).first()
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

    def validate_username(self, username):
        if username.data != current_user.username:
            user = User.query.filter_by(username=username.data).first()
            if user:
                raise ValidationError('That username is taken. Please choose a different one.')

    def validate_email(self, email):
        if email.data != current_user.email:
            user = User.query.filter_by(email=email.data).first()
            if user:
                raise ValidationError('That email is taken. Please choose a different one.')


class PostForm(FlaskForm):
    starts = StringField('Title', validators=[DataRequired()])
    content = TextAreaField('Content', validators=[DataRequired()])
    submit = SubmitField('Post')


class ReviewForm(FlaskForm):
    STARTS_OPTIONS = [(i/2, str(i/2) + ' stars' if i != 2 else str(i/2) + ' star') for i in list(range(11))]
    star = SelectField('Stars', choices=STARTS_OPTIONS,  coerce=float)
    content = TextAreaField('Content', validators=[DataRequired(), Length(min=30)])
    submit = SubmitField('Post')


class SearchForm(FlaskForm):
    conn = engine.connect()
    STATES_OPTIONS = conn.execute('''
                SELECT state
                FROM Business
                GROUP BY state
            ''').fetchall()
    STATES_OPTIONS = [state['state'] for state in STATES_OPTIONS]
    print(STATES_OPTIONS)
    state = SelectField('States', choices=STATES_OPTIONS)
    # STARS_OPTIONS = g.conn.execute('''
    #             SELECT name, address, city, round(AVG(stars), 2) AS average_stars
    #             FROM Review_of_Business JOIN Business USING(business_id)
    #             WHERE detailed_review IS NOT NULL AND is_open = True
    #             GROUP BY business_id, name, address, city
    #         ''').fetchall()
    # print(STARS_OPTIONS)
    STARS_OPTIONS = [(i/2, str(i/2) + ' stars' if i != 2 else str(i/2) + ' star') for i in list(range(11))]
    star = SelectField('Stars', choices=STARS_OPTIONS,  coerce=float)
    print(STARS_OPTIONS)
    #zipcode = TextAreaField('Content', validators=[DataRequired(), Length(min=30)])
    state = SelectField('States', choices=STATES_OPTIONS)
    submit = SubmitField('Search')


class RequestResetForm(FlaskForm):
    email = StringField('Email',
                        validators=[DataRequired(), Email()])
    submit = SubmitField('Request Password Reset')

    def validate_email(self, email):
        user = User.query.filter_by(email=email.data).first()
        if user is None:
            raise ValidationError('There is no account with that email. You must register first.')


class ResetPasswordForm(FlaskForm):
    password = PasswordField('Password', validators=[DataRequired()])
    confirm_password = PasswordField('Confirm Password',
                                     validators=[DataRequired(), EqualTo('password')])
    submit = SubmitField('Reset Password')
