from collections import deque
import os
import click
import requests
from itertools import islice
from datetime import datetime, date, timedelta, timezone
from loguru import logger
from requests.sessions import HTTPAdapter
from tqdm import tqdm
from typing import Iterator, List
from urllib3 import Retry
import random
import json
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup
from dateutil.parser import parse as date_parse
from ratelimit import limits, sleep_and_retry

# Setup loggers
logger.remove()


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

# Rate-limited _get function
@sleep_and_retry
@limits(calls=10, period=1)
def _get(*args, **kwargs):
    """Wrapper for requests.get(), except it supports retries. Also parses json."""

    s = requests.Session()
    retries = Retry(total=5, backoff_factor=0.1)
    s.mount("http://", HTTPAdapter(max_retries=retries))
    s.mount("https://", HTTPAdapter(max_retries=retries))

    response = s.get(*args, proxies=proxies, headers=headers, timeout=5, **kwargs)
    return response


def pull_user(id: int) -> dict:
    """Pull the given user's information from Gab. Returns None if not found."""

    logger.info(f"Pulling user #{id}...")
    try:
        result = _get(GAB_API_BASE_URL + f"/accounts/{id}").json()
    except json.JSONDecodeError as e:
        logger.error(f"Unable to pull user #{id}: {str(e)}")
        return None

    if result.get("error") == "Record not found":
        return None

    result["_pulled"] = datetime.now().isoformat()
    return result


def pull_statuses(id: int, sess_cookie: requests.cookies.RequestsCookieJar,
                  created_after: date, replies: bool) -> List[dict]:
    """Pull the given user's statuses from Gab. Returns an empty list if not found."""

    params = {}
    all_posts = []
    while True:
        try:
            url = GAB_API_BASE_URL + f"/accounts/{id}/statuses"
            if not replies:
                url += "?exclude_replies=true"
            result = _get(url, params=params, cookies=sess_cookie).json()
        except json.JSONDecodeError as e:
            logger.error(f"Unable to pull user #{id}'s statuses': {e}")
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
        
        most_recent_date = date_parse(posts[-1]["created_at"]).replace(tzinfo=timezone.utc).date()
        if created_after and most_recent_date < created_after:
            # Current and all future batches are too old
            break

        for post in posts:
            post["_pulled"] = datetime.now().isoformat()
            date_created = date_parse(post["created_at"]).replace(tzinfo=timezone.utc).date()
            if created_after and date_created < created_after:
                continue

            all_posts.append(post)

    return all_posts


def pull_user_and_posts(id: int, pull_posts: bool,
                        sess_cookie: requests.cookies.RequestsCookieJar,
                        created_after: date, replies: bool) -> dict:
    """Pull both a user and their posts from Gab. Returns a tuple of (user, posts). Posts is an empty list if the user is not found (i.e., None)."""

    user = pull_user(id)
    posts = pull_statuses(id, sess_cookie, created_after, replies) if user is not None and pull_posts else []

    if user is None:
        logger.info(f"User #{id} does not exist.")
    else:
        logger.info(f"Pulled {len(posts)} posts from user #{id} (@{user['username']}).")

    return (user, posts)


def find_latest_user() -> int:
    """Binary search to find the approximate latest user."""

    lower_bound = 5318531  # Update this from time to time
    logger.debug("Finding upper bound for user search...")
    upper_bound = lower_bound
    while pull_user(upper_bound) != None:
        logger.debug(f"User {upper_bound} exists; bumping upper bound...")
        upper_bound = round(upper_bound * 1.2)

    logger.debug(f"Found upper bound for users at ID {upper_bound}")

    user = None
    while lower_bound <= upper_bound:
        middle = (lower_bound + upper_bound) // 2
        middle_user = pull_user(middle)
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
def get_sess_cookie(username, password):
    """Logs in to Gab account and returns the session cookie"""
    url = GAB_BASE_URL + "/auth/sign_in"
    try:
        login_req = _get(url)
        login_req.raise_for_status()

        login_page = BeautifulSoup(login_req.text, 'html.parser')
        csrf = login_page.find('meta', attrs={'name': 'csrf-token'})['content']
        if not csrf:
            logger.error("Unable to get csrf token from sign in page!")
            return None
        
        payload = {'user[email]': username, 'user[password]': password, 'authenticity_token': csrf}
        sess_req = requests.request("POST", url, params=payload, cookies=login_req.cookies, headers=headers)
        sess_req.raise_for_status()
    except requests.exceptions.HTTPError as e:
        logger.error(f"Failed request to login page: {str(e)}")
        return None

    if not sess_req.cookies.get('_session_id'):
        raise ValueError("Invalid gab.com credentials provided!")

    return sess_req.cookies


@click.command()
@click.option(
    "--threads",
    default=25,
    help="Number of threads to use in the pull (if unspecified, defaults to 25).",
    type=int,
)
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
@click.option("--created-after", default=None, help="Only pull posts created on or after the specified date, e.g. 2021-10-02 (defaults to none).",
              type=date.fromisoformat)
@click.option(
    "--posts/--no-posts", default=False, help="Pull posts (WIP; defaults to no posts)."
)
@click.option(
    "--replies/--no-replies", default=False, help="Include replies when pulling posts (defaults to no replies)"
)
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
def run(
        threads: int, users_file: str, posts_file: str, first: int, last: int, created_after: date,
        posts: bool, replies: bool, user: str, password: str,
):
    """Pull users and (optionally) posts from Gab."""

    if posts and (not user or not password):
        raise ValueError("To pull posts you must provide a Gab username and password!")

    sess_cookie = get_sess_cookie(user, password) if user and password else None

    if last is None:
        last = find_latest_user()["id"]

    users = iter(range(first, int(last) + 1))

    with open(users_file, "w") as user_file, open(posts_file, "w") as posts_file:
        with ThreadPoolExecutor(max_workers=threads) as ex, tqdm(
            total=int(last) + 1 - first
        ) as pbar:
            # Submit initial work
            futures = deque(
                ex.submit(pull_user_and_posts, user_id, posts, sess_cookie, created_after, replies)
                for user_id in islice(users, threads * 2)
            )

            while futures:
                pbar.update(1)
                try:
                    (user, found_posts) = futures.popleft().result() # Waits until complete

                    if user is not None:
                        print(json.dumps(user), file=user_file)
                        for post in found_posts:
                            print(json.dumps(post), file=posts_file)
                except Exception as e:
                    logger.warning(f"Encountered exception in thread pool: {str(e)}")
                    raise e

                # Schedule more work, if available
                try:
                    futures.append(ex.submit(pull_user_and_posts, next(users), posts, sess_cookie, created_after, replies))
                except StopIteration:
                    # No more unscheduled users to process
                    pass

if __name__ == "__main__":
    run()
