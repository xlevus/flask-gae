import flask
import logging

import cloudstorage as gcs
from google.appengine.api import app_identity
from google.appengine.ext import blobstore

logger = logging.getLogger(__name__)


def send_gcs_file(filename, bucket=None, mimetype=None, add_etags=True,
                  as_attachment=False, attachment_filename=None):
    """
    Serve a file in Google Cloud Storage to the client.

    :param filename: The filepath to serve.
    :param bucket: The GCS bucket.
    :param mimetype: The mimetype to serve the file as. If not provided
        the mimetype as recorded by GCS will be used.
    :param add_etags: If true, etags as provided by GCS will be added.
    """
    try:

        bucket = bucket or app_identity.get_default_gcs_bucket_name()
        gcs_filename = '/{}/{}'.format(bucket, filename)
        blobkey = blobstore.create_gs_key('/gs' + gcs_filename)

        stat = gcs.stat(gcs_filename)

    except gcs.NotFoundError:
        logger.warning("GCS file %r was not found", gcs_filename)
        flask.abort(404)

    if mimetype is None:
        mimetype = stat.content_type

    resp = flask.current_app.response_class('', mimetype=mimetype)
    resp.headers[blobstore.BLOB_KEY_HEADER] = str(blobkey)

    resp.cache_control.public = True

    if add_etags:
        resp.set_etag(stat.etag)

    if as_attachment:
        if attachment_filename is None:
            attachment_filename = filename

        resp.headers.add('Content-Disposition', 'attachment',
                         filename=attachment_filename)

    if stat.st_ctime:
        resp.last_modified = int(stat.st_ctime)

    return resp

