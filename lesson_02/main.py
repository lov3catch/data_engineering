import os

from flask import Flask, request

from jobs import job1, job2

app = Flask(__name__)
AUTH_TOKEN = os.environ['AUTH_TOKEN']


@app.route("/job1", methods=['POST'])
def job1_endpoint():
    payload = request.get_json()

    data = job1.main(payload['data'], payload['page'], AUTH_TOKEN)

    job1.save(data, payload['raw_dir'], payload['data'], payload['page'])

    return {}


@app.route("/job2", methods=['POST'])
def job2_endpoint():
    payload = request.get_json()

    src = payload['src']
    dest = payload['dest']

    job2.main(src, dest)

    return {}
