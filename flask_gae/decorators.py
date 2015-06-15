import os
import functools

import flask

from google.appengine.api import users
from google.appengine.api import app_identity


__all__ = ['requires', 'Cron', 'TaskQueue', 'User', 'Administrator',
           'InboundApplication', 'DevAppServer']


def requires(test):
    """
    Decorator for Appengine view tests.

    E.g. To make a view only accessible by Cron::

        @app.route('/scheduled-task')
        @requires(gae.Cron)
        def scheduled_task():
            do_something()
            return "Done"

    Or, also allow application administrators ::

        @app.route('/other-scheduled-task')
        @requires(gae.Cron | gae.Administrator)
        def other_scheduled_task():
            do_something()
            return "Done"

    """
    if not isinstance(test, ViewTest):
        test = test()

    return test._decorator


class TestMeta(type):
    def __or__(a, b):
        return a() | b()

    def __and__(a, b):
        return a() & b()


class ViewTest(object):
    __metaclass__ = TestMeta

    def __init__(self):
        pass

    def __repr__(self):
        return self.__class__.__name__

    def __or__(a, b):
        return _Or(a, b)

    def __and__(a, b):
        return _And(a, b)

    def __call__(self):
        return self

    def _test(self):
        pass

    def _decorator(self, func):
        @functools.wraps(func)
        def _inner(*args, **kwargs):
            if self._test():
                return func(*args, **kwargs)
            flask.abort(403)
        return _inner


class _Or(ViewTest):
    def __init__(self, a, b):
        self.a = a
        self.b = b

    def __repr__(self):
        return "( {} | {} )".format(self.a, self.b)

    def _test(self):
        return self.a._test() or self.b._test()


class _And(ViewTest):
    def __init__(self, a, b):
        self.a = a
        self.b = b

    def __repr__(self):
        return "( {} & {} )".format(self.a, self.b)

    def _test(self):
        return self.a._test() and self.b._test()


class Cron(ViewTest):
    """
    Request must be made by a scheduled task.
    """

    def _test(self):
        return 'X-AppEngine-Cron' in flask.request.headers


class TaskQueue(ViewTest):
    """
    Request must be made by a task queue.

    :param *queue_names: Queue name must be one of these. If no queue names are
        provided, any queue name will be permitted.
    """

    def __init__(self, *queue_names):
        self.queue_names = queue_names

    def _test(self):
        queue = flask.request.headers.get('X-AppEngine-QueueName', None)

        if self.queue_names:
            return queue in self.queue_names

        return bool(queue)


class User(ViewTest):
    """
    Requests must be made by an autenticated administrator only.
    """
    def _test(self):
        return bool(users.get_current_user())


class Administrator(ViewTest):
    """
    Requests must be made by an application administrator only.
    """
    def _test(self):
        return users.is_current_user_admin()


class InboundApplication(ViewTest):
    """
    Requests must be made by another AppEngine application.

    :param *application_ids: A list of AppEngine application id's to allow
        inbound requests from. If this is empty, only the current application
        will be allowed.
    """
    def __init__(self, *application_ids):
        self.application_ids = filter(bool, application_ids)

    def __repr__(self):
        if self.application_ids:
            return "InboundApplication({})".format(
                ', '.join(self.application_ids))
        return "InboundApplication"

    def _test(self):
        incoming_app_id = flask.request.headers.get(
            'X-AppEngine-Inbound-AppId', None)

        if self.application_ids:
            return incoming_app_id in self.application_ids
        else:
            return incoming_app_id == app_identity.get_application_id()


class DevAppServer(ViewTest):
    """
    Only allow requests in the development server.

    Useful in conjunction with the `InboundApplication` test
    as the SDK does not send `X-AppEngine-Inbound-AppId` headers.
    """
    def _test(self):
        return os.environ.get('SERVER_SOFTWARE', '').startswith('Development')

