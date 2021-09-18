import aiofiles
from helpers import aprint, collect_devel_projects, collect_packages_metadata, parse_entries, run_request, timef, to_file
from typing import Dict, List, Set, Tuple
from attr import dataclass
import aiohttp
import asyncio


@dataclass
class Config:
    project_path: str
    source_path: str
    out_factory_maintainers: str
    out_factory_maintainers_with_counts: str
    out_factory_packages: str
    out_factory_devel_projects: str
    out_factory_devel_projects_maintainers_packages: str


@dataclass
class Maintainer:
    userid: str
    contributions: Set[str]


@dataclass
class Package:
    name: str
    meta_contributors: Set[str]


@dataclass
class DevelProject:
    name: str
    packages: List[Package]
    devel_contributors: Set[str]


@timef
async def get_packages_names(config: Config, session: aiohttp.ClientSession) -> List[str]:
    packages = await run_request(session, config.project_path)
    packages_names = parse_entries(packages)
    return packages_names


@timef
async def write_packages_metadata(config: Config, session: aiohttp.ClientSession, packages_names: List[str]) -> None:
    lines: List[str] = []
    async for package_name, _, devel_project_name in collect_packages_metadata(session, config.project_path, packages_names):
        line = f"{package_name} => {devel_project_name}"
        lines.append(line)
        await aprint(line)
    await to_file("output/package_to_devel_project.txt", lines)


@timef
async def get_maintainers_devels_projects(config: Config, session: aiohttp.ClientSession, packages_names: List[str]) -> Tuple[Dict, Dict]:

    all_devel_projects: Dict[str, DevelProject] = {}
    all_maintainers: Dict[str, Maintainer] = {}

    async for package_name, meta_userids, devel_project in collect_packages_metadata(session, config.project_path, packages_names):
        if devel_project:
            for u in meta_userids:
                if not u in all_maintainers:
                    all_maintainers[u] = Maintainer(u, set([devel_project]))
                all_maintainers[u].contributions.add(package_name)
            if not devel_project in all_devel_projects:
                all_devel_projects[devel_project] = DevelProject(devel_project, [Package(
                    package_name, set(meta_userids))], set([devel_project]) or set())
            if not package_name in all_devel_projects[devel_project].packages:
                all_devel_projects[devel_project].packages.append(
                    Package(package_name, set(meta_userids)))

    async for devel_project, devel_userids in collect_devel_projects(config, session, list(all_devel_projects.keys())):
        for u in devel_userids:
            if not u in all_maintainers:
                all_maintainers[u] = Maintainer(u, set([devel_project]))
            all_maintainers[u].contributions.add(devel_project)
        all_devel_projects[devel_project].devel_contributors.update(
            devel_userids)

    return all_maintainers, all_devel_projects


@timef
async def write_devels_projects_maintainers(config: Config, maintainers: Dict[str, Maintainer], projects: Dict[str, DevelProject]) -> None:
    async with aiofiles.open(config.out_factory_devel_projects_maintainers_packages, "w") as f:
        await f.write("Devel projects\n---")
        for p in projects.values():
            packages = [pak.name for pak in p.packages]
            packages_lines = "\n".join(sorted(packages, key=None))
            contributors = p.devel_contributors
            contributors_lines = "\n".join(sorted(contributors, key=None))
            await f.write(f"\n\n* {p.name.upper()} *")
            await f.write(f"\n\n* Packages ({len(packages)})\n")
            await f.write(packages_lines)
            await f.write(f"\n\n* Contributors ({len(contributors)})\n")
            await f.write(contributors_lines)
    await to_file(config.out_factory_maintainers, sorted(list(maintainers), key=None))


@timef
async def write_maintainers_stats(config: Config, maintainers: Dict[str, Maintainer]) -> None:
    all_maintainers = {k: v for k, v in sorted(maintainers.items(
    ), key=lambda t: len(t[1].contributions) + len(t[1].contributions), reverse=True)}
    async with aiofiles.open(config.out_factory_maintainers_with_counts, "w") as f:
        await f.write("Maintainers (devel projects contributed to, details)")
        await f.write("\n---")
        for m in all_maintainers.values():
            contributions = list(m.contributions)
            await f.write(f"\n* {m.userid} ({len(contributions)})\n")
            await f.write("\n".join(contributions))
            await f.write("\n")


@timef
async def run_the_show(config: Config, session: aiohttp.ClientSession) -> None:
    packages_names = await get_packages_names(config, session)
    # await write_packages_metadata(config, session, packages_names)
    maintainers, projects = await get_maintainers_devels_projects(config, session, packages_names)
    await write_devels_projects_maintainers(config, maintainers, projects)
    await write_maintainers_stats(config, maintainers)


async def main():
    config = Config(
        project_path="source/openSUSE:Factory",
        source_path="source",
        out_factory_maintainers="output/_maintainers.txt",
        out_factory_maintainers_with_counts="output/_maintainers_with_counts.txt",
        out_factory_packages="output/_factory_packages.txt",
        out_factory_devel_projects="output/_factory_devel_projects.txt",
        out_factory_devel_projects_maintainers_packages="output/_devel_project.txt"
    )
    async with aiohttp.ClientSession(
        auth=aiohttp.BasicAuth(
            login=env["USER"],
            password=env["PASSWORD"]
        )
    ) as session:
        await run_the_show(config, session)

if __name__ == "__main__":
    from dotenv import dotenv_values
    env = dotenv_values(".env")
    asyncio.run(main())
