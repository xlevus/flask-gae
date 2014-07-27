import cPickle as pickle
import mock
import flask

from .base import TestCase

from flask_gae.queuehandler import pushqueue


@pushqueue('testqueue')
def execute(self, *args, **kwargs):
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
        self.view.queue(1, 2, 3, kw='arg')

        tq_add.assert_called_once_with(
            url='/testhandler/',
            queue_name='testqueue',
            payload=mock.ANY,
            transactional=None,
            eta=None,
            target=None,
            name=None,
        )

