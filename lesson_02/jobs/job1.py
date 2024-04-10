import os
import shutil

import requests


def main(data, page, token):
    response = requests.get(
        url='https://fake-api-vycpfa6oca-uc.a.run.app/sales',
        params={'date': data, 'page': page},
        headers={'Authorization': token},
    )

    if 200 == response.status_code:
        return response.text

    raise Exception('Can not parse API')


# todo: идемпотентность на двух джобах
def save(payload, base_path, data, page):
    shutil.rmtree(base_path)
    os.makedirs(base_path, exist_ok=True)
    path = f'{base_path}/sales-{data}-{page}.json'
    with open(path, 'w+') as f:
        f.write(payload)


if __name__ == '__main__':
    data = '2022-08-09'
    page = 2

    AUTH_TOKEN = os.environ['AUTH_TOKEN']
    payload = main(data, page, AUTH_TOKEN)

    base_path = f'/app/file_storage/raw/sales/{data}'

    save(payload, base_path, data, page)
