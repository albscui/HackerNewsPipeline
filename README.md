# HackerNewsPipeline

Hacker News is a link aggregator webstie that users vote up stories that are interesting to them. It's similar to Reddit, but the community focuses on computer science and entrepreneurship posts.

We build a pipeline that runs some basic natural language processing tasks to find the top 100 keywords of Hacker News posts in a given year.

## How to run

```shell
export PYTHONPATH=<path to this repo>
export HN_PIPELINE__CACHE_PATH=<path to directory to save pickle files for each task>
python -m pipeline.run
```

## Source of data

The data we use comes from a Hacker News API that returns JSON data `http://hn.algolia.com/api/v1/items/:id`.

Important fields:

- `created_at`: A timestamp of the story's creation time.
- `created_at_i`: A unix epoch timestamp.
- `url`: The URL of the story link.
- `objectID`: The ID of the story.
- `author`: The story's author (username on HN).
- `points`: The number of upvotes the story had.
- `title`: The headline of the post.
- `num_comments`: The number of comments a post has.
