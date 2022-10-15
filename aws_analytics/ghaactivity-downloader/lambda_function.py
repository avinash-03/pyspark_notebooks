import json,os
from download import download_file
from upload import upload_s3
from util import get_prev_file_name, \
    get_next_file_name, upload_bookmark


bu = os.environ.get("BUCKET_NAME")
print(bu)


def lambda_handler(event, context):
    # TODO implement
    global upload_res
    file = "2015-01-01-15.json.gz"
    res = download_file(file)

    bucket_name = os.environ.get("BUCKET_NAME")
    bookmark_file = os.environ.get("BOOKMARK_FILE")
    baseline_file = os.environ.get("BASELINE_FILE")
    file_prefix = os.environ.get("FILE_PREFIX")
    while True:
        prev_file_name = get_prev_file_name(bucket_name, file_prefix, bookmark_file, baseline_file)
        file_name = get_next_file_name(prev_file_name)
        download_res = download_file(file_name)
        if download_res.status_code == 404:
            print(f"invalid file name or downloads caught up till {prev_file_name}")
            break

        upload_res = upload_s3(
            body=res.content,
            bucket=bucket_name,
            file=f"{file_prefix}/{file_name}"
        )
        with open(file_name, 'wb') as f:
            f.write(download_res.content)

        print(f"File {file_name} successfully processed")
        upload_bookmark(bucket_name, file_prefix, bookmark_file, file_name)
    return upload_res

