import os
import cPickle as pickle
import mock
import flask
from google.appengine.api import taskqueue

from flask.ext import gae
from flask.ext.gae import queuehandler


@gae.pushqueue('testqueue')
def execute(*args, **kwargs):
    return "OK"

tq_bp = flask.Blueprint('test_blueprint', __name__)


@tq_bp.route('/queue/')
@gae.pushqueue('testqueue')
def blueprint_task(a, b, c):
    return "OK"


ROW_WORKER = mock.MagicMock()


@tq_bp.route('/worker')
@gae.pullqueue('pullqueue', 'module', 123, 50)
def worker(rows):
    for task, data in rows:
        ROW_WORKER(data)
        yield task


class PushQueueViewTestCase(gae.testing.TestCase):
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
        self.assertEqual(resp.status_code, 500)

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
        @gae.pushqueue('other_queue')
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


class PullWorkerTestCase(gae.testing.TestCase):
    taskqueue_stub = {'root_path': os.path.dirname(__file__)}

    def create_app(self):
        app = flask.Flask(__name__)
        app.register_blueprint(tq_bp)
        return app

    def setUp(self):
        ROW_WORKER.reset_mock()

    @mock.patch('google.appengine.api.taskqueue.Queue')
    @mock.patch('google.appengine.api.taskqueue.Task', mock.call)
    def test_push_single(self, Queue):
        worker.push(mock.sentinel.TASK1, eta=mock.sentinel.ETA)

        Queue().add.assert_called_once_with(
            [mock.call(
                payload=pickle.dumps(mock.sentinel.TASK1),
                method='PULL',
                eta=mock.sentinel.ETA)]
        )

    @mock.patch('google.appengine.api.taskqueue.Queue')
    @mock.patch('google.appengine.api.taskqueue.Task', mock.call)
    def test_push_multiple(self, Queue):
        worker.push(mock.sentinel.TASK1,
                    mock.sentinel.TASK2)

        Queue().add.assert_called_once_with(
            [mock.call(
                payload=pickle.dumps(mock.sentinel.TASK1),
                method='PULL'),
             mock.call(
                payload=pickle.dumps(mock.sentinel.TASK2),
                method='PULL')]
        )

    @mock.patch.object(taskqueue.Queue, 'delete_tasks')
    @mock.patch.object(taskqueue.Queue, 'lease_tasks',
                       wraps=taskqueue.Queue('pullqueue').lease_tasks)
    def test_pull(self, lease_tasks, delete_tasks):
        for i in xrange(100):
            worker.push(i)

        worker._pull()

        self.assertEqual(ROW_WORKER.call_args_list,
                         [mock.call(i) for i in xrange(100)])

        # We call lease_tasks 3 times because we expect there to be some
        # afterwards
        self.assertEqual(
            lease_tasks.call_args_list,
            [mock.call(123, 50), mock.call(123, 50), mock.call(123, 50)])

        # but we only expect two calls to delete_tasks
        self.assertEqual(
            delete_tasks.call_args_list,
            [mock.call([mock.ANY]*50), mock.call([mock.ANY]*50)])

    def test_pull_tags(self):
        pass

    @mock.patch.object(taskqueue.Queue, 'delete_tasks')
    @mock.patch.object(taskqueue.Queue, 'lease_tasks')
    def test_pull_locked(self, delete_tasks, lease_tasks):
        queuehandler._PullWorkerLock.acquire('pullqueue')

        worker._pull()
        self.assertFalse(lease_tasks.called)
        self.assertFalse(delete_tasks.called)
        self.assertFalse(ROW_WORKER.called)

    @mock.patch.object(queuehandler, 'start_new_background_thread')
    def test_get(self, bg_thread):
        self.client.get('/worker')
        bg_thread.assert_called_once_with(worker._pull, ())

    def test_start(self):
        pass

