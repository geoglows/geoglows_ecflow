from geoglows_ecflow.workflow.comfies import ooflow
from geoglows_ecflow.workflow.comfies.ooflow import *
from geoglows_ecflow.workflow.comfies.dateandtime import Time


# ----------------------------------------------
# mix-ins for node classes (Task, Family, Suite)
# adding some EFAS-specific properties
# ----------------------------------------------


class CanHaveParent(object):

    """
    Adds the following properties to a Node class:
    .module -- EFAS module (fc, ff, ra, pp, wb, web) to which this node belongs to
    .group  -- EFAS module group (dwd, eud, eue, ...) to which this node belongs to
    .nominal_time -- nominal time node (00, 12, ...) to which this node belongs to
    .stage -- Stage (barrier, main, lag) to which this node belongs to
    .suite -- Suite node
    """

    @property
    def module(self):
        return self.parent.module

    @property
    def group(self):
        return self.parent.group

    @property
    def nominal_time(self):
        return self.parent.nominal_time

    @property
    def stage(self):
        return self.parent.stage

    @property
    def suite(self):
        return self.parent.suite

    @property
    def module_name(self):
        return self.module.name()

    @property
    def group_name(self):
        return self.group.name()

    @property
    def nominal_time_name(self):
        return self.nominal_time.name()

    @property
    def stage_name(self):
        return self.stage.name()

    @property
    def suite_name(self):
        return self.suite.name()

    @property
    def stage_ymd(self):
        return self.stage.ymd



class CannotHaveParent(object):

    @property
    def module(self):
        return Null()

    @property
    def group(self):
        return Null()

    @property
    def nominal_time(self):
        return Null()

    @property
    def stage(self):
        return Null()



class CanHaveChildren(object):

    @property
    def modules(self):
        """
        Return list of Module objects
        contained in this family (if any)
        """
        modules = []
        for child in self.children:
            if isinstance(child, ModuleFamily):
                modules.append(child)
            else:
                modules.extend(child.modules)
        return modules

    @property
    def groups(self):
        """
        Return list of Group objects
        contained in this family (if any)
        """
        groups = []
        for child in self.children:
            if isinstance(child, Group):
                groups.append(child)
            else:
                groups.extend(child.modules)
        return groups

    @property
    def nominal_times(self):
        """
        Return list of NominalTime objects
        contained in this family (if any)
        """
        nominal_times = []
        for child in self.children:
            if isinstance(child, NominalTime):
                nominal_times.append(child)
            else:
                nominal_times.extend(child.nominal_times)
        return nominal_times

    @property
    def stages(self):
        """
        Return list of Stage objects
        contained in this family (if any)
        """
        stages = []
        for child in self.children:
            if isinstance(child, Stage):
                stages.append(child)
            else:
                stages.extend(child.stages)
        return stages



class CannotHaveChildren(object):

    @property
    def modules(self):
        return []

    @property
    def groups(self):
        return []

    @property
    def nominal_times(self):
        return []

    @property
    def stages(self):
        return []



# -----------------------------------------------
# Task, Family and Suite classes with additional
# capabilities added via mix-ins.
# -----------------------------------------------


class Task(ooflow.Task, CanHaveParent, CannotHaveChildren):
    pass


class Family(ooflow.Family, CanHaveParent, CanHaveChildren):
    pass


class Suite(ooflow.Suite, CannotHaveParent, CanHaveChildren):

    def __init__(self, name):
        super(Suite, self).__init__(name)
        self.add_defstatus(suspended)

    @property
    def suite(self):
        return self


# -------------------------------------------
# Nominal time family
# -------------------------------------------


class NominalTime(Family):

    def __init__(self, name, delta_day=0):
        super(NominalTime, self).__init__(name)
        self.time = Time(int(name))
        self.add(
         ooflow.Variable('EMOS_BASE', name),
         ooflow.Variable('DELTA_DAY', delta_day)
        )

    @property
    def nominal_time(self):
        return self


# --------------------------------------------
# Stage family
# --------------------------------------------

class Stage(Family):

    def __init__(self, name, start_date, end_date=20420101, step=1):
        super(Stage, self).__init__(name)
        self.ymd = ooflow.RepeatDate('YMD', start_date, end_date, step)
        self.add(self.ymd)

    @property
    def stage(self):
        return self



# --------------------------------------------
# Group family
# --------------------------------------------


class GroupFamily(Family):

    def __init__(self, name):
        super(GroupFamily, self).__init__(name)
        self.add(ooflow.Variable('MODGRP', name))

    @property
    def group(self):
        return self

    def add_module(self, module):
        raise NotImplementedError()
