from functools import reduce
from typing import Any, Dict, Generator, List
from aiohttp.helpers import BasicAuth
from requests import Request
from dataclasses import dataclass
import aiofiles
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
        xpath = "contains-ic(@name, '%s') and path/project='%s'" % (
            self.match_keyword, self.distribution)
        prep_req = Request('GET', url_endpoint, params={
                           'match': xpath, "limit": 0}).prepare().url
        if not prep_req:
            raise Exception("Unable to build prepared request!")
        return PreparedRequest(prep_req)


@dataclass
class PackageEntry:
    arch: str
    baseproject: str
    filepath: str
    name: str
    package: str
    project: str
    release: str
    repository: str
    version: str

    def format_entry(self) -> str:
        return self.repository
        #return "\n".join(vars(self).values())


class Entries:
    @staticmethod
    def build_entries(entries: List[Dict[Any, Any]]) -> Generator[PackageEntry, None, None]:
        names = []
        for e in entries:
            name, project, repository = e['name'], e['project'], e['repository']
            if not "home:" in project and not name in names and any(sub in repository for sub in ["Tumbleweed", "openSUSE"]):
                names.append(name)
                yield PackageEntry(
                    name=name,
                    project=project,
                    repository=repository,
                    arch=e['arch'],
                    baseproject=e['baseproject'],
                    filepath=e['filepath'],
                    package=e['package'],
                    release=e['release'],
                    version=e['version']
                )

    @staticmethod
    def sort(entries: Generator[PackageEntry, None, None]) -> List[PackageEntry]:
        return sorted(entries, key=lambda e: e.name)


async def request(preq: PreparedRequest) -> List[Dict]:
    async with aiohttp.ClientSession(auth=BasicAuth("Nycticorax", "Trinity779")) as s:
        r = await s.get(preq.url)
        if not r.status == 200:
            raise Exception(
                f"Server responded with HTTP error code: {r.status}")
        dom = lxml.etree.fromstring(await r.text(), parser=None)
        return [{k: v for k, v in b.items()} for b in dom.xpath('/collection/binary')]


async def write_to_disk(contents: List[str]) -> None:
    async with aiofiles.open("output.txt", "w") as f:
        await f.write("\n".join(contents))
    print("Done writing to file.")


async def main() -> None:
    q = Query(
        "https://api.opensuse.org",
        "/search/published/binary/id",
        "openSUSE:Factory",
        "opera"
    )
    res = await request(q.build())
    entries = Entries.sort(Entries.build_entries(res))
    formatted = [e.format_entry() for e in entries]
    await write_to_disk(formatted)


asyncio.run(main())


"""
19421 -- TW
19427 -- Leap
"""
