import flask

from flask.ext import gae


class GaeViewTests(gae.testing.TestCase):
    def create_app(self):
        app = flask.Flask(__name__)

        @app.route('/cron')
        @gae.requires(gae.Cron)
        def requires_cron():
            return "OK"

        @app.route('/taskqueue')
        @gae.requires(gae.TaskQueue)
        def requires_taskqueue():
            return "OK"

        @app.route('/taskqueue2')
        @gae.requires(gae.TaskQueue('test'))
        def requires_specific_taskqueue():
            return "OK"

        @app.route('/user')
        @gae.requires(gae.User)
        def requires_user():
            return "OK"

        @app.route('/administrator')
        @gae.requires(gae.Administrator)
        def requries_admistrator():
            return "OK"

        @app.route('/cron-or-admin')
        @gae.requires(gae.Cron | gae.Administrator)
        def requries_cron_or_admin():
            return "OK"

        return app

    def test_requires_cron(self):
        req1 = self.client.get('/cron')
        self.assert403(req1)

        req2 = self.client.get('/cron', headers={'X-AppEngine-Cron': 'true'})
        self.assert200(req2)

    def test_requires_taskqueue(self):
        # Standard request
        req1 = self.client.get('/taskqueue')
        self.assert403(req1)

        # Any Queue name valid
        req2 = self.client.get(
            '/taskqueue', headers={'X-AppEngine-QueueName': 'default'})
        self.assert200(req2)

        # Wrong Queue name
        req3 = self.client.get(
            '/taskqueue2', headers={'X-AppEngine-QueueName': 'default'})
        self.assert403(req3)

        # Correct queue name
        req4 = self.client.get(
            '/taskqueue2', headers={'X-AppEngine-QueueName': 'test'})
        self.assert200(req4)

    def test_requires_user(self):
        # No user
        req1 = self.client.get('/user')
        self.assert403(req1)

        # User
        self.login_appengine_user('test@example.com', "test", False)
        req2 = self.client.get('/user')
        self.assert200(req2)

    def test_requires_admin(self):
        # No user
        req1 = self.client.get('/administrator')
        self.assert403(req1)

        # User
        self.login_appengine_user('test@example.com', "test", False)
        req2 = self.client.get('/administrator')
        self.assert403(req2)
        self.logout_appengine_user()

        # Administrator
        self.login_appengine_user('test@example.com', "test", True)
        req3 = self.client.get('/administrator')
        self.assert200(req3)
        self.logout_appengine_user()

    def test_cron_or_admin(self):
        # No user
        req1 = self.client.get('/cron-or-admin')
        self.assert403(req1)

        # User
        self.login_appengine_user('test@example.com', "test", False)
        req2 = self.client.get('/cron-or-admin')
        self.assert403(req2)
        self.logout_appengine_user()

        # Administrator
        self.login_appengine_user('test@example.com', "test", True)
        req3 = self.client.get('/cron-or-admin')
        self.assert200(req3)
        self.logout_appengine_user()

        req4 = self.client.get(
            '/cron-or-admin', headers={'X-AppEngine-Cron': 'true'})
        self.assert200(req4)

