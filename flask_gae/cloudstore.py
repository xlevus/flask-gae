import flask
import logging

import cloudstorage as gcs
from google.appengine.api import app_identity
from google.appengine.ext import blobstore

logger = logging.getLogger(__name__)


class LazyStat(object):
    """Class to lazily call stat()"""
    # TODO: Make this Async
    # Dependent on
    # https://code.google.com/p/appengine-gcs-client/issues/detail?id=13
    def __init__(self, filename):
        self.filename = filename
        self.data = None

    def __getattr__(self, attr):
        if self.data is None:
            self.data = gcs.stat(self.filename)
        return getattr(self.data, attr)

DEFAULT_GCS_BUCKET = None


def _default_bucket():
    global DEFAULT_GCS_BUCKET
    if DEFAULT_GCS_BUCKET is None:
        DEFAULT_GCS_BUCKET = app_identity.get_default_gcs_bucket_name()
    return DEFAULT_GCS_BUCKET


def send_gcs_file(filename, bucket=None, mimetype=None,
                  add_etags=True, etags=None,
                  add_last_modified=True, last_modified=None,
                  as_attachment=False, attachment_filename=None):
    """
    Serve a file in Google Cloud Storage (gcs) to the client.

    ..note:: When `add_etags`, `add_last_modified` or no `mimetype` is
      provided, two extra RPC calls will be made to retrieve data from
      Cloud Storage.
      If peformance, is a priority, it is advised to provide values for these
      parameters or cache the response with memcache. **But** this will return
      500 responses if the file does not exist in GCS.


    :param filename: The filepath to serve from gcs.

    :param bucket: The GCS bucket. If `None`, the default gcs bucket name will
        be used. *Note* The default bucket name will be cached in local memory.

    :param mimetype: The mimetype to serve the file as. If not provided
        the mimetype as recorded by gcs will be used. The gcs default for
        unknown files is `application/octet-stream`.

    :param add_etags: If `True`, etags as provided by gcs will be added.
    :param etags: Override any etags from GCS.

    :param add_last_modified: If `True` the last-modified header will be added
        using the value from GCS.
    :param last_modified: Override the last-modified value from GCS.

    :param as_attachment: set to `True` if you want to send this file with a
        ``Content-Disposition: attachment`` header.
    :param attachment_filename: the filename for the attachment if it differs
        from the file's filename.

    :returns: A :class:`flask.Response` object.
    """
    try:

        bucket = bucket or _default_bucket()
        gcs_filename = '/{}/{}'.format(bucket, filename)
        blobkey = blobstore.create_gs_key_async('/gs' + gcs_filename)

        stat = LazyStat(gcs_filename)

        if mimetype is None:
            mimetype = stat.content_type

        resp = flask.current_app.response_class('', mimetype=mimetype)

        resp.cache_control.public = True

        if add_etags:
            resp.set_etag(etags or stat.etag)

        if as_attachment:
            if attachment_filename is None:
                attachment_filename = filename

            resp.headers.add('Content-Disposition', 'attachment',
                             filename=attachment_filename)

        if add_last_modified and (last_modified or stat.st_ctime):
            resp.last_modified = last_modified or int(stat.st_ctime)

        resp.headers[blobstore.BLOB_KEY_HEADER] = str(blobkey.get_result())
    except gcs.NotFoundError:
        logger.warning("GCS file %r was not found", gcs_filename)
        flask.abort(404)

    return resp

