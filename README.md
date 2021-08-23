# Gabber
Simple tool to pull all posts and users from Gab.

## Usage

```text
Usage: gabber [OPTIONS]

  Pull all the users and (optionally) posts from Gab.

Options:
  --threads INTEGER     Number of threads to use in the pull (if unspecified,
                        defaults to 25).
  --users-file TEXT     Where to output the user file to.
  --posts-file TEXT     Where to output the posts file to.
  --first INTEGER       The first user ID to pull.
  --last INTEGER        The last user ID to pull.
  --posts / --no-posts  Pull posts (WIP; defaults to no posts).
  --help                Show this message and exit.
```

## Environment Variables

* `HTTP_PROXY` — route all traffic through this HTTP proxy (highly recommended given Gab's rate limiting)
* `HTTPS_PROXY` — route all traffic through this HTTPS proxy (highly recommended given Gab's rate limiting)

## Development

To run Gabber in a development environment, you'll need [Poetry](https://python-poetry.org). Install the dependencies by running `poetry install`, and then you're all set to work on Gabber locally.

To access the CLI, run `poetry run gabber`.
