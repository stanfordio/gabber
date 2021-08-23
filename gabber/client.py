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
    response = requests.get(GAB_API_BASE_URL + f"/accounts/{id}", headers={
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36",
        "cookie": "cookie: remember_user_token=eyJfcmFpbHMiOnsibWVzc2FnZSI6IlcxczFNVGMyTVRNM1hTd2lNbGRxTm5oU04za3hURmwwZFV0WWVYaDZVM2dpTENJeE5qSTVORGMzTURrMUxqQTVPVGN4T0RNaVhRPT0iLCJleHAiOiIyMDIyLTA4LTIwVDE2OjMxOjM1LjA5OVoiLCJwdXIiOiJjb29raWUucmVtZW1iZXJfdXNlcl90b2tlbiJ9fQ%3D%3D--639921a904c825056231258db441ad690d769943; __cfruid=df66118de9f77d567f14f32dfd3a4aa7c5c3d2c9-1629490483; _session_id=eyJfcmFpbHMiOnsibWVzc2FnZSI6IklqYzJNMlJsT1RRd05HUTJPR1k0TUdNd05XSTFNbVprTXpkaE56Wm1NbVJpSWc9PSIsImV4cCI6IjIwMjItMDgtMjBUMjA6NDE6MDMuOTUzWiIsInB1ciI6ImNvb2tpZS5fc2Vzc2lvbl9pZCJ9fQ%3D%3D--21e49a0eb45bf49b2828b404417d41c666c48ad0; _gabsocial_session=gw2%2FyFmMmQnzrlaPvbcTz%2FoMirfsYCJVC4%2Bm3TCOxIRF0h3BvX%2FuP7Pepk29D9jQjCDolIOx1t1C0sdhVPj1GjYCAFHsW7Y4w7ugVJ%2Fr2buPMkW%2BPhpD95YfrMSFzSV31DyPh7Gl%2F4BVmaH%2FxRpZMpmFgVyiqfoapIsH1e3eH5JxAQitugArPNy2yCAwMRbmmplg64ZVEusLPnRziRw4PcwAydQKFQVHLHGloccXzzdFGYtaScMEqdRaS55VkPcA26kzIcELjMwI5WWt9wsaOa%2BPk3u3TaujFxd3ppvvfzw5SrabLICRekdy9YvZhWrHIQpPjrVB5FSI4lZQgPEuFBcEAT84iNlp4h9x2umcqRBhOvvQAY%2FEmh642gpZpQ6tSj0ibT5f2OoPBPhteWdsEzfC62TEO5l3w%2B78StaeamNi8pWjLkqBtCaOgDCnsBdZPfQIdWvRK4KeNsiEYVyrjxYxUMG8YiYaRU2s0cWkV6VxvDEk1wA%2FM0rtgPRbSTBSg5Z0MZNDUGA3g3zrI7siohqvJQCNDSzGMTKDgA%3D%3D--qmLxpAf0JI62E%2Bf6--nGGTBi%2FqG7cpLPNrGDcLHA%3D%3D"
    })
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

    with open(users_file, "w") as user_file_out, open(posts_file, "w") as posts_file_out:
        with ThreadPoolExecutor(max_workers=threads) as pool:
            for user_id in users:
                user, posts = pull_user_and_posts(user_id)
            for (user, posts) in tqdm(
                pool.map(pull_user_and_posts, users), total=last + 1 - first, desc="pulling users..."
            ):
                print(json.dumps(user), file=user_file_out)
                for post in posts:
                    print(json.dumps(post), file=posts_file_out)
