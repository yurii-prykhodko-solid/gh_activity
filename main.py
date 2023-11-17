import csv
import os
from datetime import datetime
from typing import NamedTuple, Iterable, List

import dotenv
import polars as pl
from github import Github, Auth
from github.Event import Event


class GhEvent(NamedTuple):
    type: str
    created_at: datetime
    actor: str
    repo: str
    name: str


class Cfg(NamedTuple):
    gh_token: str
    username: str
    repo_owners: List[str]

    OWNERS_TO_SEARCH = [
        'solid-software',
        'savorlabs',
    ]

    @classmethod
    def load(cls):
        dotenv.load_dotenv('.env')

        return cls(
            gh_token=os.environ['GH_TOK'],
            username=os.environ['GH_USERNAME'],
            repo_owners=os.environ['CLIENTS'].split(',')
        )


g_cfg: Cfg


class Gh:
    def __init__(self):
        global g_cfg
        auth = Auth.Token(g_cfg.gh_token)
        self.gh = Github(auth=auth)
        self.actor = g_cfg.username
        self.ext_owners = g_cfg.repo_owners

    def _get_ext_repo_events(self, owner: str) -> Iterable[Event]:
        for e in self.gh.get_repo('solid-software/worklog').get_events():
            if e.actor.login == self.actor:
                yield e
        for e in self.gh.get_user(self.actor).get_events():
            yield e

    def get_user_events_from_ext_repos(self) -> Iterable[Event]:
        # for o in [*self.ext_owners, self.actor]:
        for e in self._get_ext_repo_events(''):
            yield e


# def gh_sbx():
# gh = get_github_client()
# print([i.actor.login for i in gh.get_repo('solid-software/worklog', lazy=True).get_events()])
# rs = gh.get_user('savorlabs').get_repos()
# for e in rs:
#     print(e)


def get_events():
    gh = Gh()

    csv_file = open('evs.csv', 'w+')
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow(list(GhEvent._fields))
    events = gh.get_user_events_from_ext_repos()
    for e in events:
        if e.type.startswith('PullRequest'):
            csv_writer.writerow([
                e.type, e.created_at, e.actor.name, e.repo.name, e.payload['pull_request']['title']
            ])
        if e.type.startswith('Issue'):
            csv_writer.writerow([
                e.type, e.created_at, e.actor.name, e.repo.name, e.payload['issue']['title']
            ])


def arrange_events():
    df = pl.read_csv('evs.csv', try_parse_dates=True, columns=list(GhEvent._fields))
    df: pl.DataFrame = df.sort(by=pl.col('created_at'))

    evs = _concat_adjacent_events(df)

    evs = evs.with_columns(
        (pl.col('end') - pl.col('start')).alias('dur').dt.minutes() / 60
    )

    evs = evs.with_columns(
        pl.col('dur').cast(pl.Utf8)
    )
    # print(evs)
    evs.write_csv('evs_sorted.csv')


def _concat_adjacent_events(df: pl.DataFrame) -> pl.DataFrame:
    evs = pl.DataFrame()

    start: datetime = None
    ongoing: str = None
    for e in df.iter_rows(named=True):
        e = GhEvent(**e)
        current = e.name
        created_at: datetime = e.created_at

        if start is None:
            start = created_at
            ongoing = current

        if start.day != created_at.day:
            evs = evs.vstack(
                pl.DataFrame(
                    data={
                        'name': current,
                        'start': start,
                        'end': start,
                        'repo': e.repo,
                        'type': e.type
                    }
                )
            )
            ongoing = current
            start = created_at
            continue

        if ongoing != current or start.day != created_at.day:
            evs = evs.vstack(
                pl.DataFrame(
                    data={
                        'name': current,
                        'start': start,
                        'end': created_at,
                        'repo': e.repo,
                        'type': e.type
                    }
                )
            )
            start = None
            ongoing = None
    return evs


if __name__ == '__main__':
    g_cfg = Cfg.load()
    # gh_sbx()
    get_events()
    arrange_events()
