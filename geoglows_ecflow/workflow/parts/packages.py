"""
'PackageInstallers' is a family of package installation scripts.
"""

from geoglows_ecflow.workflow.comfies.ooflow import Family, Task, Variable, Label, Trigger


supported_packages = (
    'scripts',
    'executables'
)


class PackageInstallers(Family):
    def __init__(self, packages=[], name='packages'):
        super(PackageInstallers, self).__init__(name)
        for package_name in packages:
            self.add_installer(package_name)

    def add_installer(self, package_name):
        if package_name not in supported_packages:
            raise ValueError(f'package "{package_name}" is not supported')
        self.add(
            Family(package_name).add(
                Variable('PACKAGE', package_name),
                Task('install_package').add(
                    Label('stage', ''),
                    Label('install', ''),
                )
            )
        )
