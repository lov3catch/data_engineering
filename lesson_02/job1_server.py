import os

from flask import Flask, request

from jobs import job1

app = Flask(__name__)
AUTH_TOKEN = os.environ['AUTH_TOKEN']


@app.route("/", methods=['POST'])
def job1_endpoint():
    payload = request.get_json()

    data = job1.main(payload['date'], payload['page'], AUTH_TOKEN)

    job1.save(data, payload['raw_dir'], payload['date'], payload['page'])

    return {}, 201
