from flask import Flask
import pytest

from spinach import MemoryBroker, Tasks
from spinach.contrib.flask_spinach import Spinach


@pytest.fixture
def app():
    app = Flask('testing')
    app.config['TESTING'] = True
    app.config['SPINACH_BROKER'] = MemoryBroker()
    return app


def test_flask_extension(app):
    spinach = Spinach(app)

    @spinach.task(name='say_hello')
    def say_hello():
        print('Hello from a task')

    @app.route('/')
    def home():
        spinach.schedule('say_hello')
        return 'Hello from HTTP'

    client = app.test_client()
    response = client.get('/')
    assert response.status_code == 200
    assert response.data == b'Hello from HTTP'

    runner = app.test_cli_runner()
    result = runner.invoke(args=['spinach', '--stop-when-queue-empty'])
    assert result.output == 'Hello from a task\n'


def test_flask_extension_app_factory(app):
    spinach = Spinach()
    tasks = Tasks()

    @tasks.task(name='foo')
    def foo():
        return 'foo'

    spinach.init_app(app)
    spinach.register_tasks(app, tasks)

    with app.app_context():
        assert spinach.execute('foo') == 'foo'


def test_flask_extension_not_init(app):
    spinach = Spinach()

    # RuntimeError for being outside application context
    with pytest.raises(RuntimeError):
        spinach.spin

    # RuntimeError for having not initialized the extension
    with pytest.raises(RuntimeError):
        spinach.register_tasks(app, None)

    # RuntimeError for having not initialized the extension
    with app.app_context():
        with pytest.raises(RuntimeError):
            spinach.spin
