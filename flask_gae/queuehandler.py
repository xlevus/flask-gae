import logging
import cPickle as pickle

from google.appengine.api import taskqueue

import flask


def task_retry_count():
    """
    Get the number of times the currently running task has been retried.
    """
    retries = flask.request.headers.get(
        'X-AppEngine-TaskRetryCount')
    if retries is not None:
        return int(retries)
    return None


class PushQueueHandler(object):
    QUEUE_ARGS = ['app', 'eta', 'name', 'target', 'transactional']

    def __init__(self, queue_name='default', logger=None):
        self.queue_name = queue_name
        self.logger = logger or logging.getLogger(__name__)
        self.func = None

    def __call__(self, func=None):
        if self.func is None:
            self.func = func
            self.__name__ = func.__name__
            self.__doc__ = func.__doc__
            self.__module__ = func.__module__

            return self
        return self._request_handler()

    @property
    def methods(self):
        return ['post']

    def _request_handler(self):
        queue_name = flask.request.headers.get('X-AppEngine-QueueName')
        if not queue_name:
            flask.abort(403, "This is a taskqueue endpoint.")

        try:
            args, kwargs = pickle.loads(flask.request.data)
            resp = self.func(*args, **kwargs)
            if hasattr(resp, 'get_result'):
                resp.get_result()
        except Exception:
            self.logger.exception(
                "Task execution failed on attempt #%s",
                task_retry_count())

            return "Task execution failed", 500
        return "View completed successfully"

    def queue(self, *args, **kwargs):
        queue_args = self._pop_tq_add_args(kwargs)
        app = queue_args.pop('app', None) or flask.current_app

        with app.app_context():
            url = self.url()

        payload = pickle.dumps((args, kwargs))

        taskqueue.add(
            url=url,
            queue_name=self.queue_name,
            payload=payload,
            **queue_args
        )

    def _pop_tq_add_args(self, kwargs):
        return {
            x: kwargs.pop('_' + x, None) for x in self.QUEUE_ARGS
        }

    def url(self):
        for endpoint, function in flask.current_app.view_functions.iteritems():
            if self is function:
                return flask.url_for(endpoint)
        raise RuntimeError("Unable to find the endpoint name")


pushqueue = PushQueueHandler

