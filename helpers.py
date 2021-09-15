from typing import Any, AsyncGenerator, Callable, List, Optional, Tuple
from aiocache.decorators import cached
from aiocache.factory import Cache
from bs4 import BeautifulSoup
from functools import partial, reduce, wraps
import aiofiles
import aiohttp
import asyncio
import timeit


def timef(f: Callable) -> Callable:
    wraps(f)

    async def inner(*args, **kwargs):
        t0 = timeit.default_timer()
        res = await f(*args, **kwargs)
        t1 = timeit.default_timer()
        await aprint(f"Ran {f.__name__} in {t1-t0} seconds.")
        return res
    return inner


@cached(cache=Cache.MEMORY)
async def run_request(session: aiohttp.ClientSession, endpoint: str) -> str:
    async with session.get(f"https://api.opensuse.org/{endpoint}") as response:
        return await response.text()


async def to_file(file_name: str, data_list: List[str]) -> None:
    async with aiofiles.open(file_name, "w") as f:
        await f.write("\n".join(data_list))


def as_async(f):
    @wraps(f)
    async def inner(*args, **kwargs):
        loop = asyncio.get_running_loop()
        saturated = partial(f, *args, **kwargs)
        return await loop.run_in_executor(None, saturated)
    return inner


async def aprint(s: str) -> None:
    await as_async(print)(s)


def partition_left_right(items: List[Any], condition: Callable[[Any], bool]) -> Tuple[List[Any], List[Any]]:
    def reducer(acc, val):
        index = 1 if condition(val) else 0
        acc[index].append(val)
        return acc
    return reduce(reducer, items, ([], []))


def parse_entries(to_parse: str) -> List[str]:
    soup = BeautifulSoup(to_parse, "lxml-xml")
    entries = soup.find_all("entry")
    return [e.attrs["name"] for e in entries]


def parse_userids(to_parse: str) -> List[str]:
    soup = BeautifulSoup(to_parse, "lxml-xml")
    people = soup.find_all("person")
    return [e.attrs["userid"] for e in people if "userid" in e.attrs]


"""
def parse_groups_data(to_parse: str) -> List[Group]:
    soup = BeautifulSoup(to_parse, "lxml-xml")
    groups = soup.find_all("group")
    groups_data = []
    for g in groups:
        title = g.find_next("title").text
        email = g.find_next("email").text if g.find_next("email") else None
        people = [p.attrs["userid"]
                  for p in g.find_all_next("person") if "userid" in p.attrs]
        maintainers = [p.attrs["userid"]
                       for p in g.find_all_next("maintainer") if "userid" in p.attrs]
        groups_data.append(Group(**{"title": title, "email": email,
                                    "people": people, "maintainers": maintainers}))
    return groups_data
"""


def parse_devel_project(to_parse: str) -> Optional[str]:
    soup = BeautifulSoup(to_parse, "lxml-xml")
    entry = soup.find("devel")
    if not entry:
        return None
    else:
        return entry.attrs["project"]


"""
def collect_unique_contributors(groups: List[Group]) -> Dict[str, Any]:
    def reducer(d: Dict[str, Any], group: Group) -> Dict[str, Any]:
        for p in group.people + group.maintainers:
            attribution = "contributed" if p in group.people else "maintained"
            other = "maintained" if attribution == "contributed" else "contributed"
            if p in d:
                d[p][attribution].append(group.title)
                d[p][f"count_{attribution}"] += 1
            else:
                d[p] = {"userid": p, attribution: [group.title],
                        f"count_{attribution}": 1, other: [], f"count_{other}": 0}
        return d
    return reduce(reducer, groups, {})
"""

"""
def filter_out_substrings(entries: List[str], substrings: List[str]) -> List[str]:
    return [e for e in entries if not any(s for s in substrings if s in e)]
"""


async def collect_packages_metadata(session: aiohttp.ClientSession, project: str, packages_names: List[str]) -> AsyncGenerator[Tuple[str, List[str], str], Any]:
    for name in packages_names:
        package_meta_data = await run_request(session, f"{project}/{name}/_meta")
        meta_userids = parse_userids(package_meta_data)
        devel_project = parse_devel_project(package_meta_data)
        yield name, meta_userids, devel_project or ""


async def collect_devel_projects(config, session: aiohttp.ClientSession, projects_names: List[str]) -> AsyncGenerator[Tuple[str, List[str]], Any]:
    for name in projects_names:
        devel_project_data = await run_request(session, f"{config.source_path}/{name}/_meta")
        devel_userids = parse_userids(devel_project_data)
        yield name, devel_userids
