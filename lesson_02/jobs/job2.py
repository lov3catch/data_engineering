import json
import os
import shutil

from fastavro import writer, parse_schema


def main(src, dest):
    with open('/app/schema.avsc', 'r') as f:
        schema = json.load(f)

    parsed_schema = parse_schema(schema)

    files = os.listdir(src)

    if not files:
        return

    if os.path.isdir(dest):
        shutil.rmtree(dest)
    os.makedirs(dest, exist_ok=True)

    for file in files:
        file_src = os.path.join(src, file)
        file_dest = os.path.join(dest, file.replace('.json', '.avro'))

        with open(file_dest, 'wb') as out:
            with open(file_src, 'r') as data:
                writer(out, parsed_schema, json.loads(data.read()))


if __name__ == '__main__':
    src = '/app/file_storage/raw/sales/2022-08-09'
    dest = '/app/file_storage/stg/sales/2022-08-09'

    main(src, dest)
