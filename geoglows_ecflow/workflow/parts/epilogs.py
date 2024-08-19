from geoglows_ecflow.workflow.comfies.ooflow import Variable, Task, Trigger
from geoglows_ecflow.workflow.comfies.ooflow import all_complete
from geoglows_ecflow.workflow.parts.nodes import Family, Task
from geoglows_ecflow.workflow.comfies.ooflow import NullVariable, NullBool, Bool, NullTrigger

"""
Epilog families are meant to be added to the repeat families.
Epilog family is executed at the end of a repeat iteration. It may do things
like archiving log outputs from tasks or just delay repeat loop increment by
sleeping for a few seconds.
"""

class LogfilesArchiver(Family):

    """
    When added to a family, it will run at the end
    (waits for all children of the parent node to complete
    and for the trigger/timer passed as 'done')
    Archives task log files to ECFS
    """
    def __init__(
            self,
            name = 'logfiles',
            jobenv = NullVariable(),
            done = NullVariable(),
            archive_trigger = NullBool()
        ):
        super(LogfilesArchiver, self).__init__(name)
        self.add(jobenv)

        n_logfilestore = Task('logfilestore')
        n_logfilestore.add(Variable('KEEPLOGS', 'no'))
        n_logfilestore.add(Variable('LOGSTOECFS', 'no'))
        n_logfilestore.add(Variable('MOVE_TO_ECFS', 'yes'))
        n_logfilestore.trigger = archive_trigger

        self._n_logfiles = Task('logfiles')
        self._n_logfiles.add(Variable('MOVE_TO_ECFS', 'no'))
        self._n_logfiles.trigger = done
        self._n_logfiles.trigger &= n_logfilestore.complete
        self.add(n_logfilestore)
        self.add(self._n_logfiles)

    def add_to(self, node):
        self._n_logfiles.trigger &= all_complete(node.children)
        self.add_variable('LOGTASK', node.name())
        super(LogfilesArchiver, self).add_to(node)



class DummyEpilog(Family):
    """
    When added to a family, it will run at the end
    (waits for all children of the parent family
    and for the 'done' trigger/timer).
    Does not do anything, just sleeps for a few seconds.
    """

    def __init__(self, name='last', done=NullTrigger()):
        super(DummyEpilog, self).__init__(name)
        self.add(Task('dummy').add(Variable('SLEEP', 3)))
        if isinstance(done, Bool):
            done = Trigger(done)
        self.add(done)

    def add_to(self, node):
        self.trigger &= all_complete(node.children)
        super(DummyEpilog, self).add_to(node)
