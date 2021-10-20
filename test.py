from operator import attrgetter
import re
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


class Entries:
    @staticmethod
    def build(items: List[Dict[str, Any]]) -> Generator[PackageEntry, None, None]:
        names = []
        for i in items:
            name = i['name']
            if not name in names and Entries.is_valid_package_entry(i) and Entries.is_relevant(i):
                names.append(name)
                yield PackageEntry(
                    name=name,
                    project=i['project'],
                    repository=i['repository'],
                    arch=i['arch'],
                    baseproject=i['baseproject'],
                    filepath=i['filepath'],
                    package=i['package'],
                    release=i['release'],
                    version=i['version']
                )

    @staticmethod
    def is_valid_package_entry(item: Dict[str, Any]) -> bool:
        the_keys = PackageEntry.__dataclass_fields__.keys()
        
        if missing := [k for k in the_keys if k not in item.keys()]:
            raise Exception(f"Missed key for item: {missing}")

        if missed := [k for k in item.keys() if k not in the_keys]:
            print(f"Missed key for item: {missed}")

        return True

    @staticmethod
    def is_relevant(item: Dict[str, Any]) -> bool:

        if "home:" in item['project']:
            return False

        if any(sub in item['repository'].lower() for sub in ["tumbleweed", "opensuse"]):
            return False

        if ":branches:" in item['project']:
            return False

        regex = r"-(debuginfo|debugsource|buildsymbols|devel|lang|l10n|trans|doc|docs)(-.+)?$"
        if re.match(regex, item['name']):
            return False

        if item['arch'] == "src":
            return False

        return True

    @staticmethod
    def sort_on(attr: str, entries: Generator[PackageEntry, None, None]) -> List[PackageEntry]:
        return sorted(entries, key=attrgetter(attr))

    @staticmethod
    def format(es: Generator[PackageEntry, None, None]) -> List[str]:
        return ["\n".join(vars(e).values()) for e in es]


async def get_package_items(preq: PreparedRequest) -> List[Dict[str, Any]]:
    async with aiohttp.ClientSession(auth=BasicAuth("Nycticorax", "Trinity779")) as s:
        r = await s.get(preq.url)
        if not r.status == 200:
            raise Exception(
                f"Server responded with HTTP error code: {r.status}")
        dom = lxml.etree.fromstring(await r.text(), parser=None)
        return [{k: v for k, v in b.items()} for b in dom.xpath("/collection/binary")]


async def write_to_disk(contents: List[str]) -> None:
    async with aiofiles.open("output.txt", "w") as f:
        await f.write("\n".join(contents))
    print("Done writing to file.")


async def main() -> None:
    q = Query(
        "https://api.opensuse.org",
        "/search/published/binary/id",
        "openSUSE:Factory",  # openSUSE:Leap:15.3
        "chess"
    )
    req = q.build()
    items = await get_package_items(req)
    entries = Entries.build(items)
    print("\n".join(Entries.format(entries)))
    # await write_to_disk(formatted)


asyncio.run(main())


"""
19421 -- TW
19427 -- Leap
"""
