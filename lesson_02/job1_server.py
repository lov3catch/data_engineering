import os
from typing import Union

from flask import Flask, request

from jobs import job1

app = Flask(__name__)

AUTH_TOKEN: str = os.environ['AUTH_TOKEN']
BASE_DIR: Union[str, os.PathLike] = os.environ['BASE_DIR']


@app.route("/", methods=['POST'])
def job1_endpoint() -> tuple:
    payload: dict = request.get_json()

    date: str = payload['date']

    data: str = job1.main(date, AUTH_TOKEN)

    job1.save(data, os.path.join(BASE_DIR, payload['raw_dir']),
              payload['date'])

    return {}, 201
