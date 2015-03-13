import time
from datetime import timedelta
import logging
import cPickle as pickle
from functools import update_wrapper

from google.appengine.ext import ndb
from google.appengine.api import taskqueue
from google.appengine.api import app_identity
from google.appengine.api import urlfetch
from google.appengine.api.background_thread import start_new_background_thread

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
    """
    A decorator to turn a view into an AppEngine push-queue handler.

    :param queue_name: The queue name to enqueue jobs for this handler on.
    """

    QUEUE_ARGS = ['app', 'eta', 'name', 'target', 'transactional']

    def __init__(self, queue_name='default'):
        self.queue_name = queue_name
        self.func = None

    def __call__(self, func=None):
        if self.func is None:
            self.func = func
            self.__name__ = func.__name__
            self.__doc__ = func.__doc__
            self.__module__ = func.__module__
            self.logger = logging.getLogger(func.__module__)

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
        """
        Enqueue the function to be called with the given args and keyword
        arguments.

        :param _app: The optional application to use for routing.
        :param _eta: The ETA for the task
        :param _transactional: Enqueue the task in a transaction.
        :param _target: The target version/module to run the task on
        :param _name: The task name.
        """
        queue_args = self._pop_tq_add_args(kwargs)
        app = queue_args.pop('app', None) or flask.current_app

        with app.test_request_context():
            # flask.url_for uses the request context if it is present
            # as we're most likely in a request context, use a
            # test_request_context() instead.
            url = self.url()

        payload = pickle.dumps((args, kwargs))

        taskqueue.add(
            url=url,
            queue_name=self.queue_name,
            payload=payload,
            **queue_args
        )

    def _pop_tq_add_args(self, kwargs):
        """
        Extract the arguments for the taskqueue.add out of the original
        keyword arguments.
        """
        return {
            x: kwargs.pop('_' + x, None) for x in self.QUEUE_ARGS
        }

    def url(self):
        for endpoint, function in flask.current_app.view_functions.iteritems():
            if self is function:
                return flask.url_for(endpoint)
        raise RuntimeError("Unable to find the endpoint name")


pushqueue = PushQueueHandler


class _PullWorkerLock(ndb.Model):
    count = ndb.IntegerProperty(default=0)

    @classmethod
    @ndb.transactional
    def acquire(cls, id, max_workers=1):
        inst = cls.get_or_insert(id)
        if inst.count >= max_workers:
            return False

        inst.count += 1
        inst.put()
        return inst

    @ndb.transactional
    def release(self):
        self.count -= 1
        self.put()


class PullQueueHandler(object):
    """
    View class to provide a wrapper around a pull queue worker.

    Only one worker thread will run at a time, and will continue to
    lease tasks until it is leased no more. At that point, the worker
    must be started again. This can be done in one of two ways:

        1. Not yet implemented.

        2. Use a cron job to call the pull worker periodically.

    The worker (decorated) function *must* yield each completed task.
    These will be deleted from the queue. Any tasks that are not yielded
    will remain on the queue to be re-leased later.

    :param queue_name: Queue to push tasks to/pull tasks from
    :param module_name: Module to run the pull worker on.
    :param tag: Tag tasks, and only pull matching tasks off the queue.
    :param lease_seconds: Time to lease tasks for.
    :param lease_size: Number of tasks to lease per pull.

    Usage ::

        app = flask.Flask('pullworker')

        @app.route('/myworker')  # Calling this URL will start the worker.
        @PullWorker('myworker', 'mymodule', 60, 500, spawn_delay=60)
        def myworker(rows):
            tasks, datas = zip(*rows)  # Unzip tasks and rows
            for data in datas:
                do_stuff(data)
            yield tasks  # You can yield either a single or iterable of tasks

        myworker.push({'some': 'data'})
        myworker.push({'foo': 'bar'})
        myworker.push({'baz': 'qux'}, eta=sometime_in_the_future)
        myworker.push({'more': 'than'}, {'one': 'payload'})
    """

    #: Module or class that provides dumps/loads functionality. Default
    #: is cPickle.
    serializer = pickle

    def __init__(self, queue_name, module_name, tag=None, lease_seconds=600,
                 lease_size=100):
        self.func = None

        self.queue_name = queue_name
        self.module_name = module_name
        self.tag = tag
        self.lease_seconds = lease_seconds
        self.lease_size = lease_size

    @property
    def queue(self):
        return taskqueue.Queue(self.queue_name)

    def __call__(self, func=None):
        if self.func is None:
            self.func = func
            self.logger = logging.getLogger(func.__module__)
            update_wrapper(self, func)
            return self
        return self._start()

    def _start(self):
        try:
            delay = int(flask.request.args.get('delay', None))
        except (TypeError, ValueError):
            delay = None

        start_new_background_thread(self._pull, (delay,))
        return "Started"

    def _pull(self, delay=None):
        lock = _PullWorkerLock.acquire(self.queue.name)
        if lock is False:
            return "locked"

        if delay:
            # Simple sleep delay. Much easier doing this in the lock
            # than using a task queue or something.
            self.logger.debug(
                "Waiting %s before processing",
                timedelta(seconds=delay))
            time.sleep(delay)

        try:
            while True:
                if self.tag:
                    tasks = self.queue.lease_tasks_by_tag(
                        self.lease_seconds, self.lease_size, self.tag)
                else:
                    tasks = self.queue.lease_tasks(
                        self.lease_seconds, self.lease_size)

                self.logger.debug("Leased %i tasks.", len(tasks))
                if len(tasks) == 0:
                    self.logger.debug("Finishing")
                    return

                completed = []
                output, _ = self._deserialize(tasks)

                try:
                    for success in self.func(output):
                        # Iter the function, and try to extend the completed
                        # tasks
                        try:
                            completed.extend(success)
                        except TypeError:
                            # Somebody yielded a single task. Append it
                            completed.append(success)
                finally:
                    self.queue.delete_tasks(completed)

        finally:
            lock.release()

    def _deserialize(self, tasks):
        output = []
        errors = []
        for t in tasks:
            try:
                output.append((t, self.serializer.loads(t.payload)))
            except ValueError:
                errors.append(t)
        return output, errors

    def push(self, *payloads, **task_args):
        """
        Push data onto the queue. Each argument is pushed to the queue as a
        new task.
        """
        tasks = [taskqueue.Task(payload=self.serializer.dumps(p),
                                method='PULL',
                                tag=self.tag,
                                **task_args)
                 for p in payloads]
        self.queue.add(tasks)

    def start(self, module=None, app=None, delay=None):
        """
        Attempt to start processing the pull queue.

        :param module: The module to start the task on. If unspecified
            will default to the module_name as provided in the init method.

        :param app: The flask.Flask app to build the URL off. If unspecified
            will use the current app.

        :param delay: Wait x seconds before starting to pull tasks off the
            queue. Useful for preventing pulling singular tasks repeatedly.
        """
        with (app or flask.current_app).test_request_context():
            path = self.url(delay=delay)

        url = 'https://{module}-dot-{hostname}{path}'.format(
            module=module or self.module_name,
            hostname=app_identity.get_default_version_hostname(),
            path=path)

        urlfetch.fetch(url)

    def url(self, **kwargs):
        for endpoint, function in flask.current_app.view_functions.iteritems():
            if self is function:
                return flask.url_for(endpoint, **kwargs)
        raise RuntimeError("Unable to find the endpoint name")


pullqueue = PullQueueHandler
