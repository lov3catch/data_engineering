import json
import os
import shutil
from typing import Union, Optional, Dict

import requests


def main(date: str, token: str, page: int = 1,
         acc: Optional[list] = None) -> str:
    if acc is None:
        acc = []

    params: Dict[str, Union[str, int]] = {'date': date, 'page': page}

    response = requests.get(
        url='https://fake-api-vycpfa6oca-uc.a.run.app/sales',
        params=params,
        headers={'Authorization': token},
    )

    if 200 == response.status_code:
        acc = acc + response.json()
        return main(date, token, page + 1, acc)

    if 404 == response.status_code:
        return json.dumps(acc)

    raise Exception('Can not parse API')


def save(payload: str, base_path: Union[str, os.PathLike], date: str) -> None:
    if os.path.isdir(base_path):
        shutil.rmtree(base_path)
    os.makedirs(base_path, exist_ok=True)
    path = f'{base_path}/sales-{date}.json'
    with open(path, 'w+') as f:
        f.write(payload)


if __name__ == '__main__':
    date = '2022-08-09'

    AUTH_TOKEN = os.environ['AUTH_TOKEN']
    payload = main(date, AUTH_TOKEN)

    base_path = f'/app/file_storage/raw/sales/{date}'

    save(payload, base_path, date)
