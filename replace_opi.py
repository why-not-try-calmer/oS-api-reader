from urllib.parse import quote
from requests.auth import HTTPBasicAuth
import requests

url = 'https://api.opensuse.org/search/released/binary?match=disturl="obs://..."'
quoted = quote(url)
resp = requests.get(url, auth=HTTPBasicAuth("Nycticorax", "Trinity779"))
print(resp.text)