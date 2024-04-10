from flask import Flask, request

from jobs import job2

app = Flask(__name__)


@app.route("/", methods=['POST'])
def job2_endpoint():
    payload = request.get_json()

    src = payload['raw_dir']
    dest = payload['stg_dir']

    job2.main(src, dest)

    return {}, 201
