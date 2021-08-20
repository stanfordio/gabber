import click
import requests
from datetime import date, datetime, timedelta, timezone
from loguru import logger
from tqdm import tqdm
from tqdm.contrib.concurrent import process_map
import random
import json
from concurrent.futures import ThreadPoolExecutor
from dateutil.parser import parse as date_parse

logger.remove()
logger.add(tqdm.write)

GAB_API_BASE_URL = "https://gab.com/api/v1"

def pull_user(id: int) -> dict:
    logger.info(f"Pulling user #{id}...")
    response = requests.get(GAB_API_BASE_URL + f"/accounts/{id}")
    try:
        result = response.json()
    except json.JSONDecodeError as e:
        logger.error(f"Unable to pull user #{id}: {response.content.decode('utf-8')}" )

    if result.get("error") == "Record not found":
        return None

    result["_pulled"] = datetime.now().isoformat()
    return result


def pull_user_and_posts(id: int) -> dict:
    return (pull_user(id), [])


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
    if delta > timedelta(minutes=10):
        logger.error(
            f"The most recent user was created more than 10 minutes ago ({user['username']} @ {user['created_at']})... that doesn't seem right!"
        )
        raise RuntimeError("Unable to find plausibly most recent user")

    logger.info(
        f"The latest user on Gab is {user['username']} (ID {user['id']}), created at {user['created_at']} ({delta.total_seconds() / 60} minutes ago)"
    )

    return user


@click.command()
@click.option(
    "--threads",
    default=None,
    help="Number of threads to use in the pull (if unspecified, number of CPUs * 5).",
    type=int,
)
@click.option(
    "--users-file",
    default="gab_users.jsonl",
    help="Where to output the user file to",
)
@click.option(
    "--posts-file",
    default="gab_posts.jsonl",
    help="Where to output the posts file to",
)
@click.option("--first", default=0, help="The first user ID to pull", type=int)
@click.option("--last", default=None, help="The last user ID to pull", type=int)
def run(threads: int, users_file: str, posts_file: str, first: int, last: int):
    """Pull all the users and posts from Gab."""

    if last is None:
        last = find_latest_user()

    users = range(first, int(last["id"]) + 1)

    with open(users_file, "w") as user_file, open(posts_file, "w") as posts_file:
        with ThreadPoolExecutor(max_workers=threads) as pool:
            process_map()
            for (user, posts) in tqdm(
                pool.map(pull_user_and_posts, users ), total=last + 1 - first, desc="pulling users..."
            ):
                print(json.dumps(user), file=user_file)
                for post in posts:
                    print(json.dumps(post), file=posts_file)
