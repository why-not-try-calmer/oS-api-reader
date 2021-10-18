from typing import Dict, List
from aiohttp.helpers import BasicAuth
from requests import Request
from dataclasses import dataclass
import lxml.etree
import aiohttp
import asyncio


@dataclass
class PreparedRequest:
    url: str


@dataclass
class Query:
    base_url: str
    api_path: str
    distribution: str
    match_keyword: str

    def build(self) -> PreparedRequest:
        url_endpoint = self.base_url + self.api_path
        xpath = "contains-ic(@name, %s) and path/project='%s'" % (
            self.match_keyword, self.distribution)
        prep_req = Request('GET', url_endpoint, params={'match': xpath, "limit": 0}).prepare().url
        if not prep_req:
            raise Exception ("Unable to build prepared request!")
        return PreparedRequest(prep_req)


async def request(preq: PreparedRequest) -> List[Dict]:
    async with aiohttp.ClientSession(auth=BasicAuth("Nycticorax", "Trinity779")) as s:
        r = await s.get(preq.url)
        if not r.status == 200:
            raise Exception(f"Server responded with HTTP error code: {r.status}")
        dom = lxml.etree.fromstring(await r.text(), parser=None)
        return [{k: v for k, v in b.items()} for b in dom.xpath('/collection/binary')]


async def main() -> None:
    q = Query(
        "https://api.opensuse.org",
        "/search/published/binary/id",
        "openSUSE:Factory",
        "'zypper'"
    )
    result = await request(q.build())
    print(result)

asyncio.run(main())


"""
19421 -- TW
19427 -- Leap
"""