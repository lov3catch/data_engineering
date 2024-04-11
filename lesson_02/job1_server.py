import os

from flask import Flask, request

from jobs import job1

app = Flask(__name__)

AUTH_TOKEN = os.environ['AUTH_TOKEN']
BASE_DIR = os.environ['BASE_DIR']


@app.route("/", methods=['POST'])
def job1_endpoint():
    payload = request.get_json()

    data = job1.main(payload['date'], AUTH_TOKEN)

    job1.save(data, os.path.join(BASE_DIR, payload['raw_dir']), payload['date'])

    return {}, 201
