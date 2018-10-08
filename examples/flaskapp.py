from flask import Flask
from spinach.contrib.flask_spinach import Spinach

app = Flask(__name__)
spinach = Spinach(app)


@spinach.task(name='say_hello')
def say_hello():
    print('Hello from a task')


@app.route('/')
def home():
    spinach.schedule('say_hello')
    return 'Hello from HTTP'
