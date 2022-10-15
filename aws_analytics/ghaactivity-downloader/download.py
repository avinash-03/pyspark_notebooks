import requests

file = "2015-01-01-15.json.gz"

def download_file(file):
    url = f"https://data.gharchive.org/{file}"
    res = requests.get(url)
    return res

# res = download_file(file)
# print(res.content)