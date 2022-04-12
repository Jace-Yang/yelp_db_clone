from flask              import Flask, render_template, redirect, url_for                          
from flask_bootstrap    import Bootstrap
from flask_wtf          import FlaskForm
from wtforms            import SubmitField

class PowerState(FlaskForm) :
    state = SubmitField('OFF')

app = Flask(__name__)
Bootstrap(app)

app.config['SECRET_KEY'] = 'YOUR SECRET KEY'

@app.route('/', methods=['GET', 'POST'])
def home() :
    form = PowerState()

    if form.validate_on_submit() :
        if form.state.label.text == 'OFF' :
            PowerState.state = SubmitField('ON')
        elif form.state.label.text == 'ON' :
            PowerState.state = SubmitField('OFF')

        return redirect(url_for('home'))
    return render_template('demo.html', form=form)