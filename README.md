Flask-GAE
=========

Task Queues
-----------

Example ::

    import flask
    from flask.ext import gae
   
    app = flask.Flask(__name__)

    @app.route('/myqueuehandler/')
    @gae.pushqueue('my-queue-name')
    def my_queue_handler(a, b, c):
        do_stuff_with(a)
        do_stuff_with(b)
        do_stuff_with(c)

    my_task_queue.queue(1, 2, 3)  # Queue my_queue_handler to be called with the args (1, 2, 3)


*Note* You can not call `my_queue_handler` directly. You must call `my_queue_hander.func` instead.

