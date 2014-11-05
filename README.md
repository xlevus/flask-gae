Flask-GAE
=========

[![Build Status](https://travis-ci.org/xlevus/flask-gae.svg)](https://travis-ci.org/xlevus/flask-gae)


Testing
-------

Flask-GAE provides a base TestCase class that sets up the testbed. By default all
stubs are enabled, but they can be disabled with setting `STUBNAME_stub` to False.
Arguments can be passed to stub initialisers by setting `STUBNAME_stub` to a dictionary.

Example :

```python
from flask.ext import gae

class MyTestCase(gae.testing.TestCase):
    blobstore_stub = False  # Disable the blobstore stub
    datastore_v3_stub = {   # Set the datastore stub's consistency policy
        'consistency_policy': PseudoRandomHRConsistencyPolicy(
            probability=0.9)}

    def test_models(self):
        pass
```


Task Queues
-----------

There is a decorator to provide a simple flask friendly interface to the GAE task queues.

Example :

```python
import flask
from flask.ext import gae

app = flask.Flask(__name__)

@app.route('/myqueuehandler/')
@gae.pushqueue('my-queue-name')
def my_queue_handler(a, b, c):
    do_stuff_with(a)
    do_stuff_with(b)
    do_stuff_with(c)

# Queue my_queue_handler to be called with the args (1, 2, 3)
my_task_queue.queue(1, 2, 3)  
```

*Note* You can not call `my_queue_handler` directly. You must call `my_queue_hander.func` instead.


Cloud Storage API
-----------------

To serve Cloud Storage files :

```python

from flask.ext import gae

@app.route('/gcs/<filename>')
def serve_gcs_file(filename):
    return gae.send_gcs_file(filename)

```

View Decorators
---------------

To make a view only accessible by Cron::

    from flask.ext import gae

    @app.route('/scheduled-task')
    @gae.requires(gae.Cron)
    def scheduled_task():
        do_something()
        return "Done"

Or, also allow application administrators ::

    @app.route('/other-scheduled-task')
    @gae.requires(gae.Cron | gae.Administrator)
    def other_scheduled_task():
        do_something()
        return "Done"

"""

Available tests:

    * ``gae.Cron`` - Cron tasks
    * ``gae.TaskQueue(*queue_names)`` - Restrict view to TaskQueue calls.
    * ``gae.User`` - Restrict view to authenticated users via `google.appengine.api.users`
    * ``gae.Administrator`` - Restrict view to application administrators
    * ``gae.InboundApplication(*application_ids)`` - Restrict view to inbound AppEngine calls only. By default, will limit calls to the current application only.

