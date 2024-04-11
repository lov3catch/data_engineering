import os
from typing import Union

from flask import Flask, request

from jobs import job2

app = Flask(__name__)

BASE_DIR: Union[str, os.PathLike] = os.environ['BASE_DIR']


@app.route("/", methods=['POST'])
def job2_endpoint() -> tuple:
    payload: dict = request.get_json()

    src: Union[str, os.PathLike] = os.path.join(BASE_DIR, payload['raw_dir'])
    dest: Union[str, os.PathLike] = os.path.join(BASE_DIR, payload['stg_dir'])

    job2.main(src, dest)

    return {}, 201
