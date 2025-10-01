import os
import requests


def test_ann():
    """
    Test the ANN similarity endpoint by sending a sample request.
    """
    gw = os.getenv('ANN_GW', 'https://REPLACE-ANN-GW')
    r = requests.post(
        f"{gw}/similarity",
        json={'q': 'test', 'k': 3},
        timeout=10
    )
    assert r.status_code == 200
    return r.json()  # Return the response JSON for inspection


if __name__ == '__main__':
    print(test_ann())
