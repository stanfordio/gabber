from collections import deque
import os
import click
import requests
from itertools import islice
from datetime import datetime, timedelta, timezone
from loguru import logger
from tqdm import tqdm
from typing import Iterator, List
from tqdm.contrib.concurrent import process_map
import random
import json
from concurrent.futures import ThreadPoolExecutor
from dateutil.parser import parse as date_parse

# Setup loggers
logger.remove()


def write_tqdm(*args, **kwargs):
    return tqdm.write(*args, end="", **kwargs)


logger.add(write_tqdm)

# Setup proxies
proxies = {"http": os.getenv("HTTP_PROXY"), "https": os.getenv("HTTPS_PROXY")}
headers={
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36",
}
requests_kwargs = dict(proxies=proxies, headers=headers)

# Constants
GAB_API_BASE_URL = "https://gab.com/api/v1"


def pull_user(id: int) -> dict:
    """Pull the given user's information from Gab. Returns None if not found."""

    logger.info(f"Pulling user #{id}...")
    response = requests.get(GAB_API_BASE_URL + f"/accounts/{id}", **requests_kwargs)
    try:
        result = response.json()
    except json.JSONDecodeError as e:
        logger.error(f"Unable to pull user #{id}: {response.content.decode('utf-8')}")
        return None

    if result.get("error") == "Record not found":
        return None

    result["_pulled"] = datetime.now().isoformat()
    return result


def pull_statuses(id: int) -> List[dict]:
    """Pull the given user's statises from Gab. Returns an empty list if not found."""

    params = {}
    all_posts = []
    while True:
        response = requests.get(
            GAB_API_BASE_URL + f"/accounts/{id}/statuses",
            params=params,
            **requests_kwargs
        )
        try:
            result = response.json()
        except json.JSONDecodeError as e:
            logger.error(
                f"Unable to pull user #{id}'s statuses': {response.content.decode('utf-8')}"
            )
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

        for post in posts:
            post["_pulled"] = datetime.now().isoformat()
            all_posts.append(post)

    return all_posts


def pull_user_and_posts(id: int, pull_posts: bool) -> dict:
    """Pull both a user and their posts from Gab. Returns a tuple of (user, posts). Posts is an empty list if the user is not found (i.e., None)."""

    user = pull_user(id)
    posts = pull_statuses(id) if user is not None and pull_posts else []

    if user is None:
        logger.info(f"User #{id} does not exist.")
    else:
        logger.info(f"Pulled {len(posts)} posts from user #{id} (@{user['username']}).")

    return (user, posts)


def find_latest_user() -> int:
    """Binary search to find the approximate latest user."""

    lower_bound = 5300000  # Update this from time to time
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
@click.option('--posts/--no-posts', default=False, help="Pull posts (WIP; defaults to no posts).")
def run(threads: int, users_file: str, posts_file: str, first: int, last: int, posts: bool):
    """Pull all the users and (optionally) posts from Gab."""

    if last is None:
        last = find_latest_user()

    users = iter(range(first, int(last["id"]) + 1))

    with open(users_file, "w") as user_file, open(posts_file, "w") as posts_file:
        with ThreadPoolExecutor(max_workers=threads) as ex, tqdm(
            total=int(last["id"]) + 1 - first
        ) as pbar:
            # Submit initial work
            futures = deque(
                ex.submit(pull_user_and_posts, user_id, posts)
                for user_id in islice(users, threads * 2)
            )

            while futures:
                pbar.update(1)
                try:
                    (user, found_posts) = futures.popleft().result()

                    if user is not None:
                        print(json.dumps(user), file=user_file)
                        for post in found_posts:
                            print(json.dumps(post), file=posts_file)
                except Exception as e:
                    logger.warning(f"Encountered exception in thread pool: {str(e)}")
                    raise e

                # Schedule more work, if available
                try:
                    futures.append(ex.submit(pull_user_and_posts, next(users), posts))
                except StopIteration:
                    # No more unscheduled users to process
                    pass
