import cPickle as pickle
import mock
import flask

from .base import TestCase

from flask_gae.queuehandler import pushqueue


@pushqueue('testqueue')
def execute(*args, **kwargs):
    return "OK"

tq_bp = flask.Blueprint('test_blueprint', __name__)


@tq_bp.route('/queue/')
@pushqueue('testqueue')
def blueprint_task(a, b, c):
    return "OK"


class PushQueueViewTestCase(TestCase):
    def setUp(self):
        execute_patch = mock.patch.object(execute, 'func')
        self.addCleanup(execute_patch.stop)
        self.execute = execute_patch.start()

    def create_app(self):
        self.logger = mock.Mock()
        app = flask.Flask(__name__)

        self.view = execute
        app.add_url_rule(
            '/testhandler/',
            view_func=execute)

        app.register_blueprint(tq_bp, url_prefix='/bp')

        return app

    def make_request(self, *args, **kwargs):
        payload = pickle.dumps((args, kwargs))
        return self.client.post('/testhandler/', data=payload, headers={
            'X-AppEngine-QueueName': 'test',
            'X-AppEngine-TaskRetryCount': 1,
        })

    def test_no_queueheaders(self):
        resp = self.client.post('/testhandler/', data="")
        self.assert403(resp)

    def test_with_queueheaders(self):
        """
        Check that when calling the endpoint with the correct headers
        execute() gets called with the right values (the payload).
        """
        resp = self.make_request(1, 2, 3, kw='arg')
        self.assert200(resp)
        self.execute.assert_called_once_with(1, 2, 3, kw='arg')

    def test_with_future(self):
        """
        Check that when execute() returns a future we get the result before
        finishing the request
        """

        self.execute.return_value = mock.Mock(['get_result'])

        resp = self.make_request(1, 2, 3, kw='arg')
        self.assert200(resp)
        self.execute.assert_called_once_with(1, 2, 3, kw='arg')

        # Check the future result is retreived
        self.execute().get_result.assert_called_once_with()

    def test_failure(self):
        """
        When a call to execute() fails, make sure we log an exception and
        return a 500 response so the task can be retried.
        """
        self.execute.side_effect = NotImplementedError()

        resp = self.make_request(1, 2, 3, kw='arg')
        self.assert500(resp)

    @mock.patch('google.appengine.api.taskqueue.add')
    def test_queue(self, tq_add):
        payload = pickle.dumps(((1, 2, 3), {'kw': 'arg'}))
        self.view.queue(1, 2, 3, kw='arg')

        tq_add.assert_called_once_with(
            url='/testhandler/',
            queue_name='testqueue',
            payload=payload,
            transactional=None,
            eta=None,
            target=None,
            name=None,
        )

    @mock.patch('google.appengine.api.taskqueue.add')
    def test_queue_extra_args(self, tq_add):
        self.view.queue(1, 2, 3, kw='arg',
                        _eta=mock.sentinel.ETA,
                        _transactional=mock.sentinel.TRANSACTIONAL,
                        _target=mock.sentinel.TARGET,
                        _name=mock.sentinel.NAME)

        tq_add.assert_called_once_with(
            url='/testhandler/',
            queue_name='testqueue',
            payload=mock.ANY,
            transactional=mock.sentinel.TRANSACTIONAL,
            eta=mock.sentinel.ETA,
            target=mock.sentinel.TARGET,
            name=mock.sentinel.NAME,
        )

    @mock.patch('google.appengine.api.taskqueue.add')
    def test_blueprint_queue(self, tq_add):
        """
        Check that we get the correct url when registered under a blueprint
        """
        blueprint_task.queue(1, 2, 3)

        tq_add.assert_called_once_with(
            url='/bp/queue/',
            queue_name='testqueue',
            payload=mock.ANY,
            transactional=None,
            eta=None,
            target=None,
            name=None,
        )

    @mock.patch('google.appengine.api.taskqueue.add')
    def test_alternate_app(self, tq_add):
        app2 = flask.Flask('other_guy')

        @app2.route('/foo/bar/baz/')
        @pushqueue('other_queue')
        def other_app_handler():
            return "OK"

        other_app_handler.queue(_app=app2)
        tq_add.assert_called_once_with(
            url='/foo/bar/baz/',
            queue_name='other_queue',
            payload=mock.ANY,
            transactional=None,
            eta=None,
            target=None,
            name=None,
        )
