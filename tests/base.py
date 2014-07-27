from flask_testing import TestCase as FTestCase

from google.appengine.ext import ndb
from google.appengine.ext import testbed
from google.appengine.datastore import datastore_stub_util


ndb.utils.DEBUG = False


class TestCase(FTestCase):
    def _pre_setup(self):
        self.testbed = testbed.Testbed()
        self.testbed.activate()

        policy = datastore_stub_util.PseudoRandomHRConsistencyPolicy(
            probability=1)
        self.testbed.init_datastore_v3_stub(consistency_policy=policy)

        ctx = ndb.get_context()
        ctx.set_cache_policy(False)
        ctx.set_memcache_policy(False)

        self.testbed.init_memcache_stub()

        super(TestCase, self)._pre_setup()

    def _post_teardown(self):
        super(TestCase, self)._post_teardown()


