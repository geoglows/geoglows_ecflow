from geoglows_ecflow.workflow.parts.nodes import Family, Task, Trigger
from geoglows_ecflow.workflow.parts.packages import PackageInstallers
from geoglows_ecflow.workflow.comfies.ooflow import Variable, Label, Defuser, complete
from geoglows_ecflow.workflow.comfies.ooflow import NullEvent, Null

"""
Nodes executed once to initialise newly created suite.
"""


class MakeFamily(Family):

    """
    sync_install tasks run on the computational
    cluster and install EFAS software packages.
    """

    def __init__(self):

        super(MakeFamily, self).__init__('make')

        # Create a family of package installers
        # (each task installs an individual EFAS package)

        n_packages = PackageInstallers(
                packages = [
                    'scripts',
                    'rapidpy',
                    'geoglows_ecflow',
                    'basininflow'
                    ]
                )

        self.add(
         Variable('SMSTRIES', 1),
         n_packages
        )

        self.add_inlimit('make')
