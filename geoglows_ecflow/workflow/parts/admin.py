from geoglows_ecflow.workflow.parts.nodes import Family, Task
from geoglows_ecflow.workflow.comfies.ooflow import Event, Trigger
from geoglows_ecflow.workflow.comfies.ooflow import Defstatus, complete
from geoglows_ecflow.workflow.comfies.ooflow import RepeatDate, Variable
import copy


def cp(obj):
    return copy.copy(obj)


class AdminFamily(Family):
    """
    Suite-level admin family.
    """
    def __init__(self):
        super(AdminFamily, self).__init__('admin')

        # public:
        #self.e_catchup = Event('catchup')
        self.e_nocds = Event('no_cds_dissemination')
        self.e_no_web = Event('no_web')
        self.e_no_webtest = Event('no_web_test')
        self.e_no_warnings = Event('No_Red_Cross_Warnings')
        self.e_catchup = Event('Catchup_MODE_no_deadlines')
        self.e_no_flood = Event('No_FloodHazard')

        # private:
        self._n_what = Task('what')  # A task with toggle events

        self.add(
            Task('toggles').add(
                self.e_nocds,
                self.e_no_web,
                self.e_no_webtest,
                self.e_no_warnings,
                self.e_catchup,
                self.e_no_flood,
                #self.e_catchup,
                Trigger('1 == 0'),
            )
         )


class AdminBackupFamily(Family):
    def __init__(self):
        super(AdminBackupFamily,self).__init__('backup')

        self.add(
            Defstatus(complete),
            Family('sync').add(
                Family('lxc').add(
                    Variable('DEST','lxc'),
                    Task('synchronise_assets')),
                Family('lxop').add(
                    Variable('DEST','lxop'),
                    Task('synchronise_assets')),
                Family('ccb').add(
                    Variable('DEST','ccb'),
                    Task('synchronise_assets'))
            )
        )

