from .queuehandler import pushqueue

try:
    import cloudstorage as gcs
except ImportError:
    def send_gcs_file():
        raise NotImplementedError(
            "You need toinstall GoogleAppengineCloudStorageClient")
else:
    from .cloudstore import send_gcs_file

from . import testing
