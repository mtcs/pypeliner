import pypeliner_workflow.dashboard as dashboard

DASHBOARD_ENABLED = False


def enable_dashboard():
    global DASHBOARD_ENABLED
    DASHBOARD_ENABLED = True


class ManagedObject(object):
    __obj_type = None

    def __init__(self):
        global DASHBOARD_ENABLED
        if DASHBOARD_ENABLED:
            self.dashboard = dashboard.Dashboard()
            self.progress(0.0)

    def progress(self, p):
        global DASHBOARD_ENABLED
        if DASHBOARD_ENABLED:
            self.dashboard.send_update({
                'type': self.__obj_type,
                'name': self.__class__.__name__,
                'id': id(self),
                'progress': p,
            })

    def __del__(self):
        self.progress(1.0)


class BasePipeline(ManagedObject):
    def __init__(self):
        super().__init__()
        self.__obj_type = 'pipeline'


class BaseStage(ManagedObject):
    def __init__(self):
        super().__init__()
        self.__obj_type = 'stage'

