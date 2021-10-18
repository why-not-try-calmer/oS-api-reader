import lxml.etree
import requests
from requests.models import HTTPBasicAuth

url = "https://api.opensuse.org"
endpoint = "/search/published/binary/id"
url_endpoint = url + endpoint
distribution = "openSUSE:Factory"
xpath = "contains-ic(@name, 'zypper') and path/project='%s'" % (distribution)
url = requests.Request('GET', url_endpoint, params={'match': xpath}).prepare().url
if url:
    print(f"URL: {url}")
    response = requests.get(url, auth=HTTPBasicAuth("Nycticorax", "Trinity779"))
    print(f"RESPONSE: {response.status_code}")
    print(response.text)
    """
    dom = lxml.etree.fromstring(r.text)
    print(dom)
    """