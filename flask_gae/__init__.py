from .queuehandler import pushqueue
from .decorators import *

try:
    import cloudstorage as gcs
except ImportError:
    def send_gcs_file():
        raise NotImplementedError(
            "You need to install GoogleAppengineCloudStorageClient")
else:
    from .cloudstore import send_gcs_file

try:
    from . import testing
except ImportError:
    pass
