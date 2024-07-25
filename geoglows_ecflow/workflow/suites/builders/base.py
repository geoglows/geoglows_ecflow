import os
from ..comfies.sdeploy import BaseBuilder as ComfiesBaseBuilder
from ..comfies.config import ymd


class BaseBuilder(ComfiesBaseBuilder):

    def build(self):

        # Setting limits on how many tasks can run at once

        max_tasks_main = self.config.get('max_tasks_main', type=int, default=26)
        max_tasks_lag = self.config.get('max_tasks_lag', type=int, default=10)

        self.suite.add_limit('make', 5)
        self.suite.add_limit('barrier', 5)
        self.suite.add_limit('main', max_tasks_main)
        self.suite.add_limit('lag', max_tasks_lag)

        # Adding some basic suite-level variables:

        first_date = self.config.get('first_date', type=ymd)
        first_barrier = self.config.get('first_barrier', type=ymd, default=first_date)
        last_date = self.config.get('last_date', type=ymd, default='20410101')

        self.suite.add_variable('FIRST_BARRIER', first_barrier)
        self.suite.add_variable('FIRST_DATE', first_date)
        self.suite.add_variable('EMOS_BASE', '00')
