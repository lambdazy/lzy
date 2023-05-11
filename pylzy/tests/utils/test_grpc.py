import time
import jwt
import pytest
from lzy.utils.grpc import build_token


def test_build_token(get_test_data_path, monkeypatch):
    etalon_time = 1682700817.93536

    def mock_time():
        return etalon_time

    monkeypatch.setattr(time, 'time', mock_time)

    username = 'lzy-test-user'
    private_key_path = get_test_data_path('id_rsa_test')
    public_key_path = get_test_data_path('id_rsa_test.pub')
    public_key = public_key_path.read_text().strip()

    token = build_token(username, private_key_path)

    with pytest.raises(jwt.exceptions.ExpiredSignatureError):
        decoded = jwt.decode(token, public_key, algorithms=["PS256"])

    # by passing leeway we are supressing any expiration problems
    decoded = jwt.decode(token, public_key, algorithms=["PS256"], leeway=etalon_time)

    assert decoded == {
        'exp': etalon_time + 7 * 24 * 60 * 60,
        'iat': etalon_time,
        'iss': username,
        'nbf': etalon_time,
        'pvd': 'GITHUB'
    }
