import flask
import logging

import cloudstorage as gcs
from google.appengine.api import app_identity
from google.appengine.ext import blobstore

logger = logging.getLogger(__name__)


def send_gcs_file(filename, bucket=None, mimetype=None, add_etags=True,
                  as_attachment=False, attachment_filename=None):
    """
    Serve a file in Google Cloud Storage (gcs) to the client.

    :param filename: The filepath to serve from gcs.

    :param bucket: The GCS bucket. If `None`, the default gcs bucket name will
        be used

    :param mimetype: The mimetype to serve the file as. If not provided
        the mimetype as recorded by gcs will be used. The gcs default for
        unknown files is `application/octet-stream`.

    :param add_etags: If `True`, etags as provided by gcs will be added.

    :param as_attachment: set to `True` if you want to send this file with a
        ``Content-Disposition: attachment`` header.
    :param attachment_filename: the filename for the attachment if it differs
        from the file's filename.

    :returns: A :class:`flask.Response` object.
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

