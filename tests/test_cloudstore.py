import mock
import flask

import cloudstorage as gcs
from flask.ext import gae


class SendGCSTestCase(gae.testing.TestCase):
    def create_app(self):
        app = flask.Flask(__name__)

        @app.route('/<path:filename>')
        def index(filename):
            return gae.send_gcs_file(
                filename,
                bucket=flask.request.args.get('bucket', None),
                mimetype=flask.request.args.get('mimetype', None),
                add_etags=('noetag' not in flask.request.args),
                add_last_modified=('nolastmod' not in flask.request.args),
                as_attachment=('attachment' in flask.request.args),
                attachment_filename=flask.request.args.get(
                    'attachment_filename', None))

        return app

    def test_get(self):
        self.create_gcs_file('test.txt', mimetype='text/plain')

        resp = self.client.get('/test.txt')
        self.assertBlobkey(resp, filename='test.txt')
        self.assertEqual(resp.mimetype, 'text/plain')

        # GCS seems to use non-utc values.
        # self.assertEqual(
        #    int(time.mktime(resp.last_modified.timetuple())),
        #    f.st_ctime)

        self.assertTrue(resp.cache_control.public)

    def test_alternate_bucket(self):
        self.create_gcs_file('test.json', bucket='bucket_two',
                             mimetype='application/json')

        resp = self.client.get('/test.json?bucket=bucket_two')
        self.assertBlobkey(resp, filename='test.json', bucket='bucket_two')
        self.assertEqual(resp.mimetype, 'application/json')

    def test_missing_file(self):
        resp1 = self.client.get('/file-missing')
        self.assert404(resp1)

        # TODO: Fix this. No way of asserting the file exists without calling
        # the GCS api. Which in turn means 2 api calls. Which means slow.

        # resp2 = self.client.get(
        #    '/file-missing?mimetype=text/plain&noetag=1&nolastmod=1')
        # self.assert404(resp2)

    def test_etags(self):
        self.create_gcs_file('test.txt', mimetype='text/plain')

        resp1 = self.client.get('/test.txt')
        # Etag of an empty file
        self.assertEqual(resp1.get_etag()[0],
                         'd41d8cd98f00b204e9800998ecf8427e')

        resp2 = self.client.get('/test.txt?noetag=1')
        self.assertIsNone(resp2.get_etag()[0])

    def test_attachment(self):
        self.create_gcs_file('test.txt', mimetype='text/plain')

        resp1 = self.client.get('/test.txt?attachment=1')
        self.assertEqual(resp1.headers['Content-Disposition'],
                         "attachment; filename=test.txt")

        resp1 = self.client.get(
            '/test.txt?attachment=1&attachment_filename=wizboingbounce')
        self.assertEqual(resp1.headers['Content-Disposition'],
                         "attachment; filename=wizboingbounce")

    @mock.patch.object(gcs, 'stat', wraps=gcs.stat)
    def test_no_stat_call(self, gcs_stat):
        self.client.get(
            '/file-missing?mimetype=text/plain&noetag=1&nolastmod=1')
        self.assertFalse(gcs_stat.called)

