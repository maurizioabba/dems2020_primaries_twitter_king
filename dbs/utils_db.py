"""
Utility module to wrap connections to the db.

For now, we are using sqlite as backbone db.

Two tables are created:

- candidates: string name, string handle. This table is mostly static.
    It's used in read-only mode here.
- candidates_tweet.<handle>: int id, int ts, string tweet. This table
    stores the tweets of each candidate (corresponding to the related
    handle). The table is created in function create_tweets_storage.
    The id is unique and it works as primary key. An additional index is
    created on the ts for fast order. The tweet is stored in pickled
    format of a tweepy.Status object.

NOTE: from https://www.sqlite.org/cintro.html chapter 6, table names
can not be used as parametrized objects. We use string concatenation.
This is just done for testing purposes and should not be used in normal
conditions.
NOTE2: pickle (and successive unpickle) of data from internet is NOT
safe.
"""
import pickle
import sqlite3


class NoStorageError(Exception):
    """Exception raised when the table queried does not exist."""

class EmptyTableError(Exception):
    """Exception raised when the table queried is empty."""


def _get_sqlite_connection(db_path="./dbs/twitter_king.sqlite"):
    """
    Naive retrieval of sqlite connection.

    :param str db_path: the path to the sqlite db
    :returns: the connection and the cursor associated
    :rtype: tuple[sqlite3.Connection, sqlite3.Cursor]
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    return conn, cursor

def get_candidates_name_handle(table):
    """
    Get the list of candidates and handles associated from the db.

    For now, this is just a wrapper around an sqlite connection,
    returning a generator with a tuple (name, handle) for each
    candidate.

    :param str table: the name of the table
    :return: an Iterator over the tuple (name, handle)
    :rtype: Iterator[tuple[str, str]]
    """
    _, cursor = _get_sqlite_connection()
    cursor.execute("select name, handle from '%s'" % (table,))
    for line in cursor:
        yield line

def get_latest_tweet(table):
    """
    Naive approach to get the latest tweet stored in the db.

    Candidates db is created with an index on the timestamp. This
    function will return the tweet corresponding to the max ts retrieved
    in the db.

    :param str table: the name of the table
    :return: a tweet as instance of tweepy.Status
    :rtype: tweepy.Status
    """
    _, cursor = _get_sqlite_connection()
    try:
        cursor.execute("select max(ts), tweet from '%s'" % (table,))
    except sqlite3.OperationalError as e:
        raise NoStorageError(e)
    row = cursor.fetchone()
    if row[0] is None:
        raise EmptyTableError("Table {} is empty".format(table))
    return pickle.loads(row[1])

def create_tweets_storage(table):
    """
    Create a tweet table with the passed name.

    The table is created with the following attributes:
        id: integer. primary key. The tweet ID
        ts: integer. the timestamp of the tweet (no datetime support in
            sqlite)
        tweet: text. A tweepy.Status object, pickled

    In addition, we create an index on the timestamp to fast query (cfr.
    `get_latest_tweet`.

    :param str table: the name of the table
    """
    conn, cursor = _get_sqlite_connection()
    cursor.execute("create table %s (id integer PRIMARY KEY, ts integer, tweet text)" % (table,))
    cursor.execute("create index index_ts_%s on '%s' (ts)" % (table, table,))
    conn.commit()

def store_tweets(table, tweets):
    """
    Store tweets in the tweet table.

    Tweets are expected to have the following attributes:
        - id: integer unique ID for tweets
        - created_at: integer timestamp
    Tweets are stored by pickling the tweet in raw format.

    :param str table: the name of the table
    :param list[object] tweets: a list of tweets to insert into
        the table
    """
    conn, cursor = _get_sqlite_connection()
    # Create a list of values as id, ts, tweet
    list_values = [(t.id, t.created_at, pickle.dumps(t)) for t in tweets]
    cursor.executemany("insert into '%s' (id, ts, tweet) values (?, ?, ?)" % (table,), list_values)
    conn.commit()

def get_tweets_batch(table, ts_start=0, limit=None):
    """
    Get a batch of tweets starting from a specific timestamp or a limit.

    If ts_start is not passed, 0 is used (all records).
    If limit is not passed, query will be unbounded.

    :param str table: the name of the table
    :param int ts_start: minimum timestamp of the tweets retrieved.
        Default to 0 (all tweets will be returned)
    :param int limit: limit on the number of tweets.
        Default to 0 (no limit)
    :returns: a list of tweets
    :rtype: list[object]
    """
    _, cursor = _get_sqlite_connection()
    query = "select tweet from '%s' where ts >= ?" % (table,)
    if limit:
        query += " order by ts limit " + str(int(limit))
    cursor.execute(query, (ts_start,))
    return [pickle.loads(row[0]) for row in cursor.fetchall()]

