import os
from geoglows_ecflow.client import ping, add_definition, begin
from ecflow import Defs


def test_ping(mocker, capfd):
    # Mock the Client and Client.ping()
    mock_client = mocker.patch('geoglows_ecflow.client.Client')
    mock_client().ping.side_effect = [True, RuntimeError('test')]

    # Call the ping function and check the output
    ping()
    out, _ = capfd.readouterr()
    assert 'ping failed' not in out

    ping()
    out, _ = capfd.readouterr()
    assert 'ping failed: test' == out.split('\n')[0]


def test_add_definition(mocker, capfd):
    # Mock definition
    mock_client = mocker.patch('geoglows_ecflow.client.Client')
    mock_defs_path = os.path.join(os.path.dirname(
        os.path.dirname(__file__)), "files", "mock_defs_novar.def"
    )
    mock_defs = Defs(mock_defs_path)
    mock_defs_name = os.path.basename(mock_defs_path).split('.')[0]
    mock_client().get_defs.side_effect = [None, mock_defs, RuntimeError('test')]

    mocker.patch(
        'geoglows_ecflow.client.os.path.basename',
        return_value=mock_defs_name
    )

    # Test "defs == None" print messages
    add_definition(mock_defs_path)
    out, _ = capfd.readouterr()
    assert f"Loading '{mock_defs_name}' definition into the server." in out
    assert "No definition in server, loading definition." in out

    # Test "defs exists" print messages
    add_definition(mock_defs_path)
    out, _ = capfd.readouterr()
    assert "Reload definition." in out

    # Test RuntimeError print messages
    add_definition(mock_defs_path)
    out, _ = capfd.readouterr()
    assert "Failed: test" in out


def test_begin(mocker, capfd):
    # Mocks
    mock_client = mocker.patch('geoglows_ecflow.client.Client')
    mock_client().begin_suite.side_effect = [True, RuntimeError('test')]
    mock_suite_name = 'test_suite'

    # Call the begin function
    begin(mock_suite_name)
    out, _ = capfd.readouterr()

    assert f"Begin '{mock_suite_name}' suite." == out.split('\n')[0]

    # Test RuntimeError
    begin(mock_suite_name)
    out, _ = capfd.readouterr()
    assert 'Failed: test' == out.split('\n')[1]
