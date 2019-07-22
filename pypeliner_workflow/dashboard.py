#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import multiprocessing as mp

NO_WEB = False

try:
    import flask
except ImportError:
    NO_WEB = True

LOG = logging.getLogger(__name__)


class DashboardProcess(object):

    def __init__(self, pipe):
        self.app = flask.Flask(
            __name__,)
            # template_folder='templates')
        self.app.add_url_rule('/', 'index', self.app_root)
        self.app.add_url_rule('/<name>', 'obj_list', self.obj_list)
        self.app.add_url_rule('/<name>/<int:obj_id>', 'obj_info', self.obj_info)
        self.pipe = pipe

    def app_root(self):
        return flask.render_template('dashbaord.html', name='Application')

    def obj_list(self, name):
        return f'Obj List {name}'

    def obj_info(self, name, obj_id):
        return f'{name} Info {obj_id}'

    def run(self):
        # while True:
        #     update = self.pipe.recv()
        self.app.run('127.0.0.1', 5000, debug=True)

    def __del__(self):
        pass


def _start_dashboard_process(pipe):
    dashboard = DashboardProcess(pipe)
    dashboard.run()


class Dashboard(object):
    # SINGLETON CLASS
    instance = None

    class __Dashboard(object):
        def __init__(self):
            # Create Dashboard process
            global NO_WEB
            if not NO_WEB:
                self.update_pipe, dest_pipe = mp.Pipe()
                self.dash_process = mp.Process(
                    target=_start_dashboard_process,
                    args=(dest_pipe,))
                self.dash_process.start()
            else:
                LOG.error('No flask installed. Install flask for dashboard support')

        def send_update(self, update_obj):
            if not NO_WEB:
                self.update_pipe.send(update_obj)

    def __init__(self):
        if not self.instance:
            self.instance = Dashboard.__Dashboard()

    def __getattr__(self, name):
        return getattr(self.instance, name)
