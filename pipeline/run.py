import csv
import io
import json
import string
from collections import Counter
from datetime import datetime
from pprint import pprint
from pytz import timezone

from pipeline.pipeline import Pipeline, build_csv
from pipeline.stop_words import stop_words

pipeline = Pipeline()


def __get_start_end_dates(year):
    # Given a year, return the start end end timestamps in unix epoch
    utc = timezone("UTC")
    start = utc.localize(datetime(year, 1, 1)).timestamp()
    end = utc.localize(datetime(year + 1, 1, 1)).timestamp()

    return start, end


# TODO currently we are only getting 1 page, need iterate through pages to get full dataset
@pipeline.task()
def get_data_from_hacker_news(year=2014):
    import requests
    url = "http://hn.algolia.com/api/v1/search_by_date"
    start, end = __get_start_end_dates(year)
    query = {
        "tags": "story",
        "numericFilters":
        "created_at_i>={},created_at_i<{}".format(start, end),
        "hitsPerPage": 100,
    }
    resp = requests.get(url, params=query)
    return resp.json()["hits"]


@pipeline.task(depends_on=get_data_from_hacker_news)
def filter_stories(stories):
    def is_popular(story):
        points = story["points"] > 50
        num_comments = story["num_comments"] > 1
        title = story["title"].startswith("Ask HN")
        return points and num_comments and not title

    return (s for s in stories if is_popular(s))


@pipeline.task(depends_on=filter_stories)
def json_to_csv(stories):
    header = ('objectID', 'created_at', 'url', 'points', 'title')
    lines = ((s['objectID'],
              datetime.strptime(s['created_at'], "%Y-%m-%dT%H:%M:%S.%fZ"),
              s['url'], s['points'], s['title']) for s in stories)
    return build_csv(lines, header, io.StringIO())


@pipeline.task(depends_on=json_to_csv)
def extract_titles(fp):
    reader = csv.reader(fp)
    header = next(reader)
    title_idx = header.index('title')
    return (l[title_idx] for l in reader)


@pipeline.task(depends_on=extract_titles)
def clean_titles(titles):
    return ("".join(c for c in t.lower() if c not in string.punctuation)
            for t in titles)


@pipeline.task(depends_on=clean_titles)
def build_keyword_dictionary(titles):
    c = Counter(w for t in titles for w in t.split()
                if w not in stop_words and w != "")
    return dict(c)


@pipeline.task(depends_on=build_keyword_dictionary)
def most_common_titles(word_freq):
    return [t for t in Counter(word_freq).most_common(100)]


def main():
    result = pipeline.run()
    pprint(result[most_common_titles])


if __name__ == "__main__":
    main()
