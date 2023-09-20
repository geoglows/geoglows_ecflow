import sys
import os
import ecflow


def main(definition: str, host_port: str = "localhost:2500") -> None:
    """Add definition to the server and begin the suite.

    Args:
        definition (str): Path to job definition file. E.g.
            "/path/to/geoglows_forecast.def".
        host_port (str, optional): Server host and port. Defaults to
            "localhost:2500".
    """
    # Get definition name
    definition_name = os.path.basename(definition).split('.')[0]

    # Check server connection
    try:
        ci = ecflow.Client(host_port)
        ci.ping()

    except RuntimeError as e:
        print("ping failed: ", str(e))

    # Load the definition into the server
    try:
        print(f"Loading '{definition_name}' definition into the server.")
        ci = ecflow.Client(host_port)

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

        # Begin the suite
        print(f"Begin '{definition_name}' suite.")
        ci.begin_suite(definition_name)

    except RuntimeError as e:
        print("Failed: ", e)


if __name__ == "__main__":
    main(*sys.argv[1:])
