import os
import sys
import string
import re
from time import sleep
import click
import requests
from itertools import islice
from datetime import datetime, date, timedelta, timezone
from loguru import logger
from requests.sessions import HTTPAdapter
import json
from concurrent.futures import ThreadPoolExecutor
from urllib3 import Retry
from concurrent import futures


from tqdm import tqdm
from typing import Iterable, List
from dateutil.parser import parse as date_parse
from ratelimit import limits, sleep_and_retry
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
import selenium.common.exceptions
from selenium import webdriver

# Setup loggers
logger.remove()

REQUESTS_PER_SESSION_REFRESH = 5000


def json_set_default(obj):
    logger.warning("Unable to fully serialize JSON data!")
    return f"[unserializable: {str(obj)}]"


def write_tqdm(*args, **kwargs):
    return tqdm.write(*args, end="", **kwargs, file=sys.stderr)


logger.add(write_tqdm)

proxies = {"http": os.getenv("http_proxy"), "https": os.getenv("https_proxy")}

# Constants
GAB_BASE_URL = "https://gab.com"
GAB_API_BASE_URL = "https://gab.com/api/"


def await_any(items: List[futures.Future], pop=True):
    done, _not_done = futures.wait(items, return_when=futures.FIRST_COMPLETED)
    if pop:
        for item in done:
            items.remove(item)
    return done


def extract_url_from_link_header(link: string) -> string:
    """Helper method to pull urls from link header for iteration through accounts"""
    pattern = "https?://.+?max_id=\d+"
    matched_links = re.findall(pattern, link)
    if matched_links:
        return re.findall(pattern, link)[0]
    else:
        return ""


class Client:
    def __init__(self, username: str, password: str, threads: int):
        self.username = username
        self.password = password
        self.threads = threads
        self._requests_since_refresh = 0
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:103.0) Gecko/20100101 Firefox/103.0",
        }
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
        response = s.get(
            *args, proxies=proxies, headers=self.headers, timeout=30, **kwargs
        )
        logger.info(f"GET: {response.url}")
        logger.info(f"Response status: {response.status_code}")

        if not skip_sess_refresh:
            self._requests_since_refresh += 1
            if self._requests_since_refresh > REQUESTS_PER_SESSION_REFRESH:
                logger.info(
                    f"Refreshing session... {self._requests_since_refresh} requests since last refresh..."
                )
                self.sess_cookie = self.get_sess_cookie(self.username, self.password)
                self._requests_since_refresh = 0

        return response

    # Account lookup by username
    def lookup_by_username(self, username: str):
        gab_username_link = GAB_API_BASE_URL + "v1/account_by_username/" + username
        try:
            resp = self._get(gab_username_link)
            response_data = resp.json()

            if resp.status_code != 200:
                logger.warning(
                    f"Pulling user #{id} had non-200 status code ({resp.status_code})"
                )
                print("N/A - Username not found")
            else:
                print(response_data["id"])

        except json.JSONDecodeError as e:
            logger.error(f"JSON error #{id}: {str(e)}")
            print("N/A")
        except Exception as e:
            logger.error(f"Misc. error while pulling user {id}: {str(e)}")
            print("N/A")

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
            resp = self._get(GAB_API_BASE_URL + f"v1/accounts/{id}")
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
            resp = self._get(GAB_API_BASE_URL + f"v1/groups/{id}")
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
                    GAB_API_BASE_URL + f"v1/timelines/group/{id}",
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

        if group is None or not group.get("_available", False):
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
                url = GAB_API_BASE_URL + f"v1/accounts/{id}/statuses"
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

    def search(self, query: str, depth: int):
        all_results = []
        page = 1

        while page <= depth:
            try:
                result = self._get(
                    GAB_API_BASE_URL + f"v3/search?q={query}",
                    params={
                        "onlyVerified": "false",
                        "type": "status",
                        "page": page,
                    },
                    cookies=self.sess_cookie,
                ).json()

            except json.JSONDecodeError as e:
                logger.error(f"Unable to pull search for {query} : {e}")
                break
            except Exception as e:
                logger.error(f"Misc. error while searching for {query}: {e}")
                break

            if "error" in result:
                logger.error(
                    f"API returned an error while searching for {query}: {result}"
                )
                break

            if len(result) == 0:
                break

            page += 1
            all_results.append(result)

        return all_results

    def _was_account_created(self, id: int, accounts_or_groups: string) -> bool:
        """
        Determine whether account was created, even if suspended.
        Returns true if request for account ID returns 200 or 410.
        """
        result = self._get(GAB_API_BASE_URL + f"v1/{accounts_or_groups}/{id}")
        logger.info(
            f"Current status code on account #{id}: {result.status_code} {result.status_code == 200 or result.status_code == 410}"
        )
        return result.status_code == 200 or result.status_code == 410

    def find_latest_user(self) -> int:
        return self._find_latest()

    def find_latest_group(self) -> int:
        return self._find_latest(lower_bound=65937, accounts_or_groups="groups")

    def _find_latest(
        self, lower_bound: int = 5318531, accounts_or_groups: string = "accounts"
    ) -> int:
        """Binary search to find the approximate latest user."""
        # lower_bound: Update this from time to time
        logger.debug("Finding upper bound for user search...")
        upper_bound = lower_bound
        # result = self.pull_user(upper_bound)
        logger.info(f"Available? {self.pull_user(upper_bound)['_available']}")
        while self._was_account_created(upper_bound, accounts_or_groups):
            logger.debug(f"User {upper_bound} exists; bumping upper bound...")
            upper_bound = round(upper_bound * 1.2)

        logger.debug(f"Found upper bound for users at ID {upper_bound}")

        user_id = None
        while lower_bound <= upper_bound:
            middle = (lower_bound + upper_bound) // 2
            middle_user = self._was_account_created(middle, accounts_or_groups)
            if middle_user:
                user_id = middle

            if middle_user:
                lower_bound = middle + 1
            else:
                upper_bound = middle - 1
            logger.debug(f"Upper bound: {upper_bound}. Lower bound: {lower_bound}")

        logger.info(f"Retrieving user: {middle}")

        if accounts_or_groups == "accounts":
            user = self.pull_user(user_id)
            time_limit = 30
        else:
            user = self.pull_group(user_id)
            time_limit = 1440  # 24 hrs
        if user["_available"]:
            created_at = date_parse(user["created_at"]).replace(tzinfo=timezone.utc)
            delta = datetime.utcnow().replace(tzinfo=timezone.utc) - created_at
            if delta > timedelta(minutes=time_limit):
                logger.error(
                    f"The most recent user was created more than 30 minutes ago ({user['username']} @ {user['created_at']}, {round(delta.total_seconds() / 60)} mins ago)... that doesn't seem right!"
                )
                raise RuntimeError("Unable to find plausibly most recent user")

            if accounts_or_groups == "accounts":
                logger.info(
                    f"The latest user on Gab is (roughly) {user['username']} (ID {user['id']}), created at {user['created_at']} ({delta.total_seconds() / 60} minutes ago)"
                )
            else:
                logger.info(
                    f"The latest group on Gab is (roughly) {user['title']} (ID {user['id']}), created at {user['created_at']} ({delta.total_seconds() / 60} minutes ago)"
                )

        return int(user["id"])

    def pull_follow(self, id: int, endpoint: string):
        follows = []

        logger.info(f"Pulling followers for user {id}.")
        try:
            resp = self._get(GAB_API_BASE_URL + f"v1/accounts/{id}/{endpoint}")

            if resp.status_code != 200:
                logger.warning(
                    f"Pulling followers for #{id} had non-200 status code ({resp.status_code})"
                )
                return

            follows.extend(resp.json())

            yield follows
            while "Link" in resp.headers:
                logger.debug(f"Counted {len(follows)} for account {id}.")
                next_followers_url = extract_url_from_link_header(resp.headers["Link"])
                logger.debug(f"Next URL to pull: {next_followers_url}")

                if not next_followers_url:
                    logger.debug(f"Counted {len(follows)} for account {id}.")
                    break
                else:
                    resp = self._get(next_followers_url)
                    resp_follows = resp.json()
                    for follow in resp_follows:
                        follow["_pulled"] = datetime.now().isoformat()
                        follow["_source_id"] = id
                    follows.extend(resp_follows)
                    yield resp_follows

        except json.JSONDecodeError as e:
            logger.error(f"JSON error #{id}: {str(e)}")
            return
        except Exception as e:
            logger.error(f"Misc. error while pulling user {id}: {str(e)}")
            return

    # Adapted from https://github.com/ChrisStevens/garc
    def get_sess_cookie(self, username, password):
        """Logs in to Gab account and returns the session cookie"""
        url = GAB_BASE_URL + "/auth/sign_in"
        logger.debug("Getting session cookie.")

        def bearer_auth_listener(eventdata):
            req_headers = eventdata["params"]["request"]["headers"]
            if "Authorization" in req_headers:
                self.headers["Authorization"] = req_headers["Authorization"]

        options = webdriver.ChromeOptions()
        options.add_argument("--disable-gpu")
        options.headless = True
        driver = uc.Chrome(enable_cdp_events=True, options=options)
        driver.add_cdp_listener("Network.requestWillBeSent", bearer_auth_listener)
        driver.get(url)
        cookies = {}

        # TODO: see if we can iterate on retries to increase sleep times
        try:
            sleep(5)  # sleep to allow page to load, cf_challenge to complete.
            username_input = driver.find_element(By.ID, "user_email")
            password_input = driver.find_element(By.ID, "user_password")
            login_button = driver.find_element(By.CLASS_NAME, "btn")

            username_input.send_keys(username)
            password_input.send_keys(password)
            login_button.click()
            sleep(5)  # sleep to allow page to load
            # Selenium-based driver pulls more cookie metadata than needed. Move cookies to key-value pair.
            for cookie in driver.get_cookies():
                cookies[cookie["name"]] = cookie["value"]
            # TODO: check that required session cookie is present

            driver.close()
            return cookies
        except selenium.common.exceptions.NoSuchElementException as no_element:
            logger.error("Page did not load quickly enough.")
            logger.exception(no_element)
            # Without a valid session cookie, pulls for posts will not terminate.
            # Raise exception and terminate here.
            raise (no_element)


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


@cli.command("lookup")
@click.argument("username", type=str)
@click.pass_context
def lookup(ctx, username: str):

    client: Client = ctx.obj["client"]
    if not client.username or not client.password:
        raise ValueError("To pull data you must provide a Gab username and password!")

    client.lookup_by_username(username)


@cli.command("followers")
@click.option("--id", help="User id from which to pull followers.", type=int)
@click.option(
    "--followers-file",
    default="gab_followers.jsonl",
    help="Where to output the followers file to",
)
@click.pass_context
def followers(ctx, followers_file: string, id: int):
    """
    Experimental feature: pull followers from a Gab account.
    """
    client: Client = ctx.obj["client"]
    if not client.username or not client.password:
        raise ValueError("To pull data you must provide a Gab username and password!")
    if id is None:
        id = client.find_latest_user()

    with open(followers_file, "w") as followers_file:
        follow_generator = client.pull_follow(id, endpoint="followers")
        for followers in follow_generator:
            for account in followers:
                print(
                    json.dumps(account, default=json_set_default),
                    file=followers_file,
                    flush=True,
                )


@cli.command("following")
@click.option(
    "--id", help="User id from which to pull accounts they are following.", type=int
)
@click.option(
    "--following-file",
    default="gab_following.jsonl",
    help="Where to output the following file to",
)
@click.pass_context
def following(ctx, following_file: string, id: int):
    """
    Experimental feature: pull list of accounts that a Gab account follows.
    """
    client: Client = ctx.obj["client"]
    if not client.username or not client.password:
        raise ValueError("To pull data you must provide a Gab username and password!")
    if id is None:
        id = client.find_latest_user()

    with open(following_file, "w") as following_file:
        follow_generator = client.pull_follow(id, endpoint="following")
        for following in follow_generator:
            for account in following:
                print(
                    json.dumps(account, default=json_set_default),
                    file=following_file,
                    flush=True,
                )


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
        last = client.find_latest_user()

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
    """Pull groups and (optionally) their posts from Gab. Can pull at most 250 pages of posts per group (5000 posts)."""

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


@cli.command("search")
@click.option(
    "--depth",
    default=10,
    help="How many pages of search results to retrieve.",
    type=int,
)
@click.option("--query", help="The query to search for.", type=str)
@click.pass_context
def search(ctx, query: str, depth: int):
    """Query the Search API."""

    client: Client = ctx.obj["client"]

    if not client.username or not client.password:
        raise ValueError("To pull posts you must provide a Gab username and password!")

    print(json.dumps(client.search(query, depth)))


def cli_entrypoint():
    cli(obj={})


if __name__ == "__main__":
    cli_entrypoint()
