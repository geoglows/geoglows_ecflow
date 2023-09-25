import sys
import os
from ecflow import Client


def ping(host_port: str = "localhost:2500") -> None:
    """Ping server.

    Args:
        host_port (str, optional): Server host and port. Defaults to
            "localhost:2500".
    """
    try:
        ci = Client(host_port)
        ci.ping()

    except RuntimeError as e:
        print("ping failed:", str(e))


def add_definition(definition: str, host_port: str = "localhost:2500") -> None:
    """Add definition to the server.

    Args:
        definition (str): Path to job definition file. E.g.
            "/path/to/geoglows_forecast.def".
        host_port (str, optional): Server host and port. Defaults to
            "localhost:2500".
    """
    # Get definition name
    definition_name = os.path.basename(definition).split('.')[0]

    # Check server connection
    ping(host_port)

    # Load the definition into the server
    try:
        print(f"Loading '{definition_name}' definition into the server.")
        ci = Client(host_port)

        ci.sync_local()   # sync definitions from server
        defs = ci.get_defs() # retrieve the defs from ci

        # Add definition if not exist, replace if exist
        if defs is None:
            print("No definition in server, loading definition.")
            ci.load(definition)

        else:
            print("Reload definition.")
            ci.replace(f"/{definition_name}", definition)

        # Restart the server
        print("Restarting the server.")
        ci.restart_server()

    except RuntimeError as e:
        print("Failed:", e)


def begin(name: str, host_port: str = "localhost:2500") -> None:
    """Begin definition.

    Args:
        name (str): Name of the definition. E.g. "geoglows_forecast".
        host_port (str, optional): Server host and port. Defaults to
            "localhost:2500".
    """
    # Check server connection
    ping(host_port)

    # Load the definition into the server
    try:
        ci = Client(host_port)

        # Begin the suite
        print(f"Begin '{name}' suite.")
        ci.begin_suite(name)

    except RuntimeError as e:
        print("Failed:", e)


if __name__ == "__main__":  # pragma: no cover
    if sys.argv[1] == "add_def":
        add_definition(*sys.argv[2:])
    if sys.argv[1] == "begin_def":
        begin(*sys.argv[2:])
