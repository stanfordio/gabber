from collections import deque
import os
from time import sleep
import click
import requests
from itertools import islice
from datetime import datetime, date, timedelta, timezone
from loguru import logger
from requests.sessions import HTTPAdapter
from tqdm import tqdm
from typing import Iterable, Iterator, List
from urllib3 import Retry
from concurrent import futures
import random
import json
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup
from dateutil.parser import parse as date_parse
from ratelimit import limits, sleep_and_retry

# Setup loggers
logger.remove()

REQUESTS_PER_SESSION_REFRESH = 5000


def json_set_default(obj):
    logger.warning("Unable to fully serialize JSON data!")
    return f"[unserializable: {str(obj)}]"


def write_tqdm(*args, **kwargs):
    return tqdm.write(*args, end="", **kwargs)


logger.add(write_tqdm)

# Setup proxies
proxies = {"http": os.getenv("HTTP_PROXY"), "https": os.getenv("HTTPS_PROXY")}
headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36",
}

# Constants
GAB_BASE_URL = "https://gab.com"
GAB_API_BASE_URL = "https://gab.com/api/v1"


def await_any(items: List[futures.Future], pop=True):
    done, _not_done = futures.wait(items, return_when=futures.FIRST_COMPLETED)
    if pop:
        for item in done:
            items.remove(item)
    return done


class Client:
    def __init__(self, username: str, password: str, threads: int):
        self.username = username
        self.password = password
        self.threads = threads
        self._requests_since_refresh = 0
        if username and password:
            self.sess_cookie = self.get_sess_cookie(username, password)

    # Rate-limited _get function
    @sleep_and_retry
    @limits(calls=100, period=1)
    def _get(self, *args, skip_sess_refresh=False, **kwargs):
        """Wrapper for requests.get(), except it supports retries."""

        s = requests.Session()
        retries = Retry(
            total=10,
            backoff_factor=0.5,
            status_forcelist=[413, 429, 503, 403, 500, 502, 523, 520],
        )
        s.mount("http://", HTTPAdapter(max_retries=retries))
        s.mount("https://", HTTPAdapter(max_retries=retries))

        response = s.get(*args, proxies=proxies, headers=headers, timeout=30, **kwargs)
        logger.info(f"GET: {response.url}")

        if not skip_sess_refresh:
            self._requests_since_refresh += 1
            if self._requests_since_refresh > REQUESTS_PER_SESSION_REFRESH:
                logger.info(
                    f"Refreshing session... {self._requests_since_refresh} requests since last refresh..."
                )
                self.sess_cookie = self.get_sess_cookie(self.username, self.password)
                self._requests_since_refresh = 0

        return response

    def pull_user(self, id: int) -> dict:
        """Pull the given user's information from Gab. Returns None if not found."""

        result = {
            "_pulled": datetime.now().isoformat(),
            "id": str(
                id
            ),  # When the pull errors, we still want to have the ID. It's ok that data from Gab will probably override this field.
        }

        logger.info(f"Pulling user #{id}...")
        try:
            resp = self._get(GAB_API_BASE_URL + f"/accounts/{id}")
            result.update(_status_code=resp.status_code)

            if resp.status_code != 200:
                logger.warning(
                    f"Pulling user #{id} had non-200 status code ({resp.status_code})"
                )
                result.update(
                    **{
                        "_available": False,
                    }
                )
                return result

            result.update(_available=True, **resp.json())
        except json.JSONDecodeError as e:
            logger.error(f"JSON error #{id}: {str(e)}")
            result.update(_error={str(e)})
            return result
        except Exception as e:
            logger.error(f"Misc. error while pulling user {id}: {str(e)}")
            result.update(_error={str(e)})
            return result

        if result.get("error") == "Record not found":
            result.update(_available=False, _error=result.get("error"))

        return result

    def pull_group(self, id: int) -> dict:
        """Pull the given group's information from Gab. Returns None if not found."""

        result = {
            "_pulled": datetime.now().isoformat(),
            "id": str(
                id
            ),  # When the pull errors, we still want to have the ID. It's ok that data from Gab will probably override this field.
        }

        logger.info(f"Pulling group #{id}...")
        try:
            resp = self._get(GAB_API_BASE_URL + f"/groups/{id}")
            result.update(_status_code=resp.status_code)

            if resp.status_code != 200:
                logger.warning(
                    f"Pulling group #{id} had non-200 status code ({resp.status_code})"
                )
                result.update(
                    **{
                        "_available": False,
                    }
                )
                return result

            result.update(_available=True, **resp.json())
        except json.JSONDecodeError as e:
            logger.error(f"JSON error #{id}: {str(e)}")
            result.update(_error={str(e)})
            return result
        except Exception as e:
            logger.error(f"Misc. error while pulling group {id}: {str(e)}")
            result.update(_error={str(e)})
            return result

        if result.get("error") == "Record not found":
            result.update(_available=False, _error=result.get("error"))

        return result

    def pull_group_posts(self, id: int, depth: int) -> Iterable[dict]:
        """Pull the given group's posts from Gab."""

        page = 1
        # If we hit any kind of error, we increment this and try to pull the page again.
        tries_since_failure = 0

        while page <= depth and tries_since_failure < 5:
            if tries_since_failure > 0:
                logger.info("Retrying after 30 seconds...")
                sleep(30)
            try:
                results = self._get(
                    GAB_API_BASE_URL + f"/timelines/group/{id}",
                    params={
                        "sort_by": "newest",
                        "page": page,
                    },
                    cookies=self.sess_cookie,
                ).json()
            except json.JSONDecodeError as e:
                logger.error(
                    f"Unable to pull group #{id}'s statuses, potentially retrying: {e}"
                )
                tries_since_failure += 1
                continue
            except Exception as e:
                logger.error(
                    f"Misc. error while pulling statuses for group #{id}, potentially retrying: {e}"
                )
                tries_since_failure += 1
                continue

            if "error" in results:
                logger.error(
                    f"API returned an error while pulling group #{id}'s statuses, potentially retrying: {results}"
                )
                tries_since_failure += 1
                continue
            if len(results) == 0:
                # This is the only _good_ case
                break

            tries_since_failure = 0
            for result in results:
                result["_pulled"] = datetime.now().isoformat()
                yield result

            page += 1

    def pull_group_and_posts(self, id: int, pull_posts: bool, depth: int) -> dict:
        """Pull both a group and their its from Gab. Returns a tuple of (group, posts). Posts is an empty list if the group is not found (i.e., None)."""

        group = self.pull_group(id)
        posts = list(
            self.pull_group_posts(id, depth)
            if group.get("_available", False) and pull_posts and pull_posts
            else []
        )

        if group is None or group.get("_available", False):
            logger.info(f"Group #{id} does not exist.")
        else:
            logger.info(f"Pulled {len(posts)} posts from group #{id}.")

        return (group, posts)

    def pull_statuses(
        self,
        id: int,
        created_after: date,
        replies: bool,
        expected_count: int = None,
        retries_remaining: int = 3,
    ) -> List[dict]:
        """Pull the given user's statuses from Gab. Returns an empty list if not found."""

        params = {}
        all_posts = []
        while True:
            try:
                url = GAB_API_BASE_URL + f"/accounts/{id}/statuses"
                if not replies:
                    url += "?exclude_replies=true"
                result = self._get(url, params=params, cookies=self.sess_cookie).json()
            except json.JSONDecodeError as e:
                logger.error(f"Unable to pull user #{id}'s statuses': {e}")
                break
            except Exception as e:
                logger.error(f"Misc. error while pulling statuses for {id}: {e}")
                break

            if "error" in result:
                logger.error(
                    f"API returned an error while pulling user #{id}'s statuses: {result}"
                )
                break

            if len(result) == 0:
                break

            if not isinstance(result, list):
                logger.error(f"Result is not a list (it's a {type(result)}): {result}")

            posts = sorted(result, key=lambda k: k["id"])
            params["max_id"] = posts[0]["id"]

            most_recent_date = (
                date_parse(posts[-1]["created_at"]).replace(tzinfo=timezone.utc).date()
            )
            if created_after and most_recent_date < created_after:
                # Current and all future batches are too old
                break

            for post in posts:
                post["_pulled"] = datetime.now().isoformat()
                date_created = (
                    date_parse(post["created_at"]).replace(tzinfo=timezone.utc).date()
                )
                if created_after and date_created < created_after:
                    continue

                all_posts.append(post)

        if expected_count is not None and retries_remaining > 0:
            # If we have everything we expect *within a threshold of 0.95*, we're good to go!
            if expected_count == 0 or (len(all_posts) / expected_count) > 0.95:
                return all_posts

            logger.warning(
                f"Expected {expected_count} statuses from #{id} but only found {len(all_posts)} — retrying ({retries_remaining - 1} further retries remaining)"
            )
            return self.pull_statuses(
                id,
                created_after,
                replies,
                expected_count=expected_count,
                retries_remaining=retries_remaining - 1,
            )

        return all_posts

    def pull_user_and_posts(
        self, id: int, pull_posts: bool, created_after: date, replies: bool
    ) -> dict:
        """Pull both a user and their posts from Gab. Returns a tuple of (user, posts). Posts is an empty list if the user is not found (i.e., None)."""

        user = self.pull_user(id)

        posts = (
            self.pull_statuses(
                id,
                created_after,
                replies,
                expected_count=user.get("statuses_count") if user is not None else None,
            )
            if user.get("_available") and pull_posts
            else []
        )

        if user is None or not user.get("_available", False):
            logger.info(f"User #{id} does not exist.")
        else:
            logger.info(
                f"Pulled {len(posts)} (Gab claims {user.get('statuses_count')}) posts from user #{id} (@{user['username']})."
            )
            if user.get("statuses_count") < len(posts):
                logger.warning(
                    f"Pulled posts for user #{id} does not match Gab's claim! (We have {len(posts)}, but Gab says this user has {user.get('statuses_count')} statuses.)"
                )

        return (user, posts)

    def find_latest_user(self) -> int:
        """Binary search to find the approximate latest user."""

        lower_bound = 5318531  # Update this from time to time
        logger.debug("Finding upper bound for user search...")
        upper_bound = lower_bound
        while self.pull_user(upper_bound) != None:
            logger.debug(f"User {upper_bound} exists; bumping upper bound...")
            upper_bound = round(upper_bound * 1.2)

        logger.debug(f"Found upper bound for users at ID {upper_bound}")

        user = None
        while lower_bound <= upper_bound:
            middle = (lower_bound + upper_bound) // 2
            middle_user = self.pull_user(middle)
            if middle_user is not None:
                user = middle_user

            if middle_user is not None:
                lower_bound = middle + 1
            else:
                upper_bound = middle - 1

        created_at = date_parse(user["created_at"]).replace(tzinfo=timezone.utc)
        delta = datetime.utcnow().replace(tzinfo=timezone.utc) - created_at
        if delta > timedelta(minutes=30):
            logger.error(
                f"The most recent user was created more than 30 minutes ago ({user['username']} @ {user['created_at']}, {round(delta.total_seconds() / 60)} mins ago)... that doesn't seem right!"
            )
            raise RuntimeError("Unable to find plausibly most recent user")

        logger.info(
            f"The latest user on Gab is (roughly) {user['username']} (ID {user['id']}), created at {user['created_at']} ({delta.total_seconds() / 60} minutes ago)"
        )

        return user

    # Adapted from https://github.com/ChrisStevens/garc
    def get_sess_cookie(self, username, password):
        """Logs in to Gab account and returns the session cookie"""
        url = GAB_BASE_URL + "/auth/sign_in"
        try:
            login_req = self._get(url, skip_sess_refresh=True)
            login_req.raise_for_status()

            login_page = BeautifulSoup(login_req.text, "html.parser")
            csrf = login_page.find("meta", attrs={"name": "csrf-token"})["content"]
            if not csrf:
                logger.error("Unable to get csrf token from sign in page!")
                return None

            payload = {
                "user[email]": username,
                "user[password]": password,
                "authenticity_token": csrf,
            }
            sess_req = requests.request(
                "POST", url, params=payload, cookies=login_req.cookies, headers=headers
            )
            sess_req.raise_for_status()
        except requests.exceptions.HTTPError as e:
            logger.error(f"Failed request to login page: {str(e)}")
            return None

        if not sess_req.cookies.get("_session_id"):
            raise ValueError("Invalid gab.com credentials provided!")

        return sess_req.cookies


@click.group()
@click.option(
    "--user",
    default=os.environ.get("GAB_USER", ""),
    help="Username to gab.com account. Required to pull posts. If unspecified, uses GAB_USER environment variable.",
)
@click.option(
    "--password",
    default=os.environ.get("GAB_PASS", ""),
    help="Password to gab.com account. Required to pull posts. If unspecified, uses GAB_PASS environment variable.",
)
@click.option(
    "--threads",
    default=25,
    help="Number of threads to use in the pull (if unspecified, defaults to 25).",
    type=int,
)
@click.pass_context
def cli(ctx, user, password, threads):
    ctx.ensure_object(dict)
    ctx.obj["client"] = Client(user, password, threads)


@cli.command("users")
@click.option(
    "--users-file",
    default="gab_users.jsonl",
    help="Where to output the user file to.",
)
@click.option(
    "--posts-file",
    default="gab_posts.jsonl",
    help="Where to output the posts file to.",
)
@click.option("--first", default=0, help="The first user ID to pull.", type=int)
@click.option("--last", default=None, help="The last user ID to pull.", type=int)
@click.option(
    "--created-after",
    default=None,
    help="Only pull posts created on or after the specified date, e.g. 2021-10-02 (defaults to none).",
    type=date.fromisoformat,
)
@click.option(
    "--posts/--no-posts", default=False, help="Pull posts (WIP; defaults to no posts)."
)
@click.option(
    "--replies/--no-replies",
    default=False,
    help="Include replies when pulling posts (defaults to no replies)",
)
@click.pass_context
def users(
    ctx,
    users_file: str,
    posts_file: str,
    first: int,
    last: int,
    created_after: date,
    posts: bool,
    replies: bool,
):
    """Pull users and (optionally) posts from Gab."""

    client: Client = ctx.obj["client"]

    if posts and (not client.username or not client.password):
        raise ValueError("To pull data you must provide a Gab username and password!")

    if last is None:
        last = client.find_latest_user()["id"]

    users = iter(range(first, int(last) + 1))

    with open(users_file, "w") as user_file, open(posts_file, "w") as posts_file:
        with ThreadPoolExecutor(max_workers=client.threads) as ex, tqdm(
            total=int(last) + 1 - first
        ) as pbar:
            # Submit initial work
            f = list(
                ex.submit(
                    client.pull_user_and_posts, user_id, posts, created_after, replies
                )
                for user_id in islice(users, client.threads * 2)
            )

            while len(f) > 0:
                pbar.update(1)
                try:
                    done = await_any(f)
                    for completed in done:
                        (user, found_posts,) = completed.result(
                            0
                        )  # Waits until complete

                        if user is not None:
                            print(
                                json.dumps(user, default=json_set_default),
                                file=user_file,
                                flush=True,
                            )
                            for post in found_posts:
                                print(
                                    json.dumps(post, default=json_set_default),
                                    file=posts_file,
                                    flush=True,
                                )

                            logger.info(f"Wrote user #{user['id']} to disk...")
                except Exception as e:
                    logger.warning(f"Encountered exception in thread pool: {str(e)}")
                    raise e

                # Schedule more work, if available
                try:
                    for _ in range(len(done)):
                        f.append(
                            ex.submit(
                                client.pull_user_and_posts,
                                next(users),
                                posts,
                                created_after,
                                replies,
                            )
                        )
                except StopIteration:
                    # No more unscheduled users to process
                    pass


@cli.command("groups")
@click.option(
    "--groups-file",
    default="gab_groups.jsonl",
    help="Where to output the groups file to.",
)
@click.option(
    "--posts-file",
    default="gab_posts.jsonl",
    help="Where to output the posts file to.",
)
@click.option("--first", default=0, help="The first group ID to pull.", type=int)
@click.option("--last", default=70000, help="The last group ID to pull.", type=int)
@click.option(
    "--depth", default=10000, help="How many pages of posts to retrieve.", type=int
)
@click.option("--posts/--no-posts", default=False, help="Pull posts.")
@click.pass_context
def groups(
    ctx,
    groups_file: str,
    posts_file: str,
    first: int,
    last: int,
    depth: int,
    posts: bool,
):
    """Pull groups and (optionally) their posts from Gab."""

    client: Client = ctx.obj["client"]

    if posts and (not client.username or not client.password):
        raise ValueError("To pull posts you must provide a Gab username and password!")

    groups = iter(range(first, int(last) + 1))

    with open(groups_file, "w") as groups_file, open(posts_file, "w") as posts_file:
        with ThreadPoolExecutor(max_workers=client.threads) as ex, tqdm(
            total=int(last) + 1 - first
        ) as pbar:
            # Submit initial work
            f = list(  # Right now, this list will just grow to infinity as work is completed. In theory we could pop once we finish processing a group.
                ex.submit(client.pull_group_and_posts, group, posts, depth)
                for group in islice(groups, client.threads * 2)
            )

            while len(f) > 0:
                pbar.update(1)
                try:
                    done = await_any(f)
                    for completed in done:
                        (group, found_posts,) = completed.result(
                            0
                        )  # Waits until complete

                        if group is not None:
                            print(
                                json.dumps(group, default=json_set_default),
                                file=groups_file,
                                flush=True,
                            )
                            for post in found_posts:
                                print(
                                    json.dumps(post, default=json_set_default),
                                    file=posts_file,
                                    flush=True,
                                )

                        logger.info(f"Wrote group #{group['id']} to disk...")
                except Exception as e:
                    logger.warning(f"Encountered exception in thread pool: {str(e)}")
                    raise e

                # Schedule more work, if available
                try:
                    for _ in range(len(done)):
                        f.append(
                            ex.submit(
                                client.pull_group_and_posts, next(groups), posts, depth
                            )
                        )
                except StopIteration:
                    # No more unscheduled groups to process
                    logger.info("No more groups to process!")


def cli_entrypoint():
    cli(obj={})


if __name__ == "__main__":
    cli_entrypoint()
