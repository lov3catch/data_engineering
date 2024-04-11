import os

from flask import Flask, request

from jobs import job2

app = Flask(__name__)

BASE_DIR = os.environ['BASE_DIR']


@app.route("/", methods=['POST'])
def job2_endpoint():
    payload = request.get_json()

    src = os.path.join(BASE_DIR, payload['raw_dir'])
    dest = os.path.join(BASE_DIR, payload['stg_dir'])

    job2.main(src, dest)

    return {}, 201
