import json
import os
import shutil
import requests


def main(data, token, page=1, acc=None) -> str:
    if acc is None:
        acc = []

    response = requests.get(
        url='https://fake-api-vycpfa6oca-uc.a.run.app/sales',
        params={'date': data, 'page': page},
        headers={'Authorization': token},
    )

    if 200 == response.status_code:
        acc = acc + response.json()
        return main(data, token, page + 1, acc)

    if 404 == response.status_code:
        return json.dumps(acc)

    raise Exception('Can not parse API')


def save(payload, base_path, data):
    if os.path.isdir(base_path):
        shutil.rmtree(base_path)
    os.makedirs(base_path, exist_ok=True)
    path = f'{base_path}/sales-{data}.json'
    with open(path, 'w+') as f:
        f.write(payload)


if __name__ == '__main__':
    data = '2022-08-09'

    AUTH_TOKEN = os.environ['AUTH_TOKEN']
    payload = main(data, AUTH_TOKEN)

    base_path = f'/app/file_storage/raw/sales/{data}'

    save(payload, base_path, data)
