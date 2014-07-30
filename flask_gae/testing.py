from flask_testing import TestCase as FTTestCase

try:
    import cloudstorage as gcs
except ImportError:
    gcs = None

from google.appengine.ext import testbed
from google.appengine.ext import blobstore
from google.appengine.api import app_identity
from google.appengine.datastore.datastore_stub_util import \
    PseudoRandomHRConsistencyPolicy as PRHRConsistencyPolicy

__all__ = ['TestCase']


class Any(object):
    def __eq__(self, other):
        return True

ANY = Any()


def _test_gcs():
    if gcs is None:
        raise NotImplementedError(
            "GoogleAppEngineCloudStorageClient is not installed")


class TestCase(FTTestCase):
    STUBS = ['datastore_v3', 'memcache', 'app_identity', 'blobstore',
             'files', 'urlfetch']

    datastore_v3_stub = {
        'consistency_policy': PRHRConsistencyPolicy(probability=1)}

    def _pre_setup(self):
        self.testbed = testbed.Testbed()
        self.testbed.activate()

        for stub in self.STUBS:
            stub_args = getattr(self, stub + '_stub', True)
            init = getattr(self.testbed, 'init_' + stub + '_stub')

            if isinstance(stub_args, dict):
                init(**stub_args)
            elif stub:
                init()

        super(TestCase, self)._pre_setup()

    def _post_teardown(self):
        super(TestCase, self)._post_teardown()

    def create_gcs_file(self, filename, data='', bucket=None,
                        mimetype=None):
        bucket = bucket or app_identity.get_default_gcs_bucket_name()
        filename = '/{}/{}'.format(bucket, filename)

        with gcs.open(filename, 'w', content_type=mimetype) as f:
            f.write(data)

        return gcs.stat(filename)

    def assertBlobkey(self, resp, blobkey=None, filename=None, bucket=None):
        """
        Assert a response includes a serving blobkey.

        :param blobkey: The blobkey.
        :param filename: The filename of the GCS file. Needs
            GoogleAppEngineCloudStorageClient installed.
        :param bucket: If using GCS, the bucket to serve the blob from.
        """

        if not blobkey and filename:
            _test_gcs()
            blobkey = blobstore.create_gs_key('/gs/{}/{}'.format(
                bucket or app_identity.get_default_gcs_bucket_name(),
                filename))
        elif not blobkey:
            blobkey = ANY

        self.assertEqual(
            resp.headers[blobstore.BLOB_KEY_HEADER],
            blobkey)

