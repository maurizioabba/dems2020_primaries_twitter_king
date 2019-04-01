"""
Tweets module.

It provides primitives for authentication, fetch, analyze tweets.
"""
import datetime as dt
import dill
import tweepy

from dbs import utils_db

def _get_ts_from_tweet(tweet):
    """
    Helper function converting a tweet.created_at datetime in a timestamp.

    Perform simple transformation of datetime to timestamp

    :param tweepy.Status tweet: the parsed tweet
    :returns: the timestamp retrieved
    :rtype: int
    """
    return int((tweet.created_at - dt.datetime(1970, 1, 1)).total_seconds())


def get_auth_handler(
        task_instance,
        consumer_key,
        consumer_secret,
        access_token,
        access_secret_token,
        xcom_push_tweepy_hdl,
        **kwargs):
    """
    Get authentication handler.

    As we don't require any authorization process "live", we will just
    use a stored access_token and access_token_secret to access the tweet
    API. This will generate a static handler connected to my own account.

    The generated handler can not be pickled directly (because pickle
    is badly written). Our solution is to use dill to get an encoded
    version of the object. This can be pickled by airflow. Note that,
    by using dill, we will also be resistant toward airflow 2.0
    deprecation of pickle module in favour of json.

    :param str consumer_key: the main consumer API of the Twitter app
    :param str consumer_secret: the main consumer API secret from Twitter app
    :param str access_token: the temporary access token from app
    :param str access_secret_token: the temporary secret access token from app

    """
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret_token)

    api = tweepy.API(auth)

    task_instance.xcom_push(key=xcom_push_tweepy_hdl, value=dill.dumps(api))


def fetch_tweets(
        task_instance,
        candidate_handle,
        tweet_storage,
        xcom_pull_tweepy_hdl,
        new_tweets_fetch_size=100,
        **kwargs):
    """
    Fetch last 100 tweets from the candidates specified.

    The tweets can be found using the tweepy API.
    Each tweet will be stored using the tweet ID in the appropriate DB.
    """
    auth_handler = dill.loads(task_instance.xcom_pull(key=xcom_pull_tweepy_hdl))

    # kwargs for user_timeline
    kwargs_timeline = {
        "screen_name": candidate_handle,
        "count": new_tweets_fetch_size,
    }
    latest_tweet = None
    try:
        latest_tweet = utils_db.get_latest_tweet(tweet_storage)
        kwargs_timeline["since_id"] = latest_tweet.id
    except utils_db.NoStorageError:
        utils_db.create_tweets_storage(tweet_storage)
        # By not adding "since_id", It will just retrieve the latest 100 tweets
    except utils_db.EmptyTableError:
        # Just skip since_id parameter but do not recreate the table
        pass

    tweets = auth_handler.user_timeline(**kwargs_timeline)

    utils_db.store_tweets(tweet_storage, tweets)
    ret_s = "added {} tweets to handle {}".format(len(tweets), candidate_handle)
    if latest_tweet:
        ret_s += " starting from ts {}".format(latest_tweet.created_at)
    return ret_s


def analyze_tweets(
        task_instance,
        candidate_handle,
        tweet_storage,
        xcom_push_tweet_features,
        analysis_tweets_size=100,
        **kwargs):
    """
    Perform analysis on latest analysis_tweets_size tweets.

    The current workflow sums number of retweets and number of favorites.
    """
    task_instance.log.info("fetching tweets from %s", tweet_storage)
    tweets = utils_db.get_tweets_batch(tweet_storage, limit=analysis_tweets_size)

    features = {
        "tot_favorites": sum(t.favorite_count for t in tweets),
        "tot_retweets": sum(t.retweet_count for t in tweets),
    }

    task_instance.xcom_push(key=xcom_push_tweet_features, value=features)
    return ("handle {handle_id}: "
            "tweets {tweet_count}, "
            "favorites {tot_favorites}, "
            "retweet {tot_retweets}".format(
                handle_id=candidate_handle,
                tweet_count=len(tweets),
                tot_favorites=features["tot_favorites"],
                tot_retweets=features["tot_retweets"])
            )


def perform_ranking(
        task_instance, list_candidates, **kwargs):
    """Get all features and determine the winner."""
    curr_total = 0
    curr_top_candidate = None
    curr_top_features = None
    for name, handle in list_candidates:
        features = task_instance.xcom_pull(key=handle)
        cand_total = features["tot_favorites"] + features["tot_retweets"]
        # NOTE: no even allowed
        if cand_total > curr_total:
            curr_top_candidate = name
            curr_total = cand_total
            curr_top_features = features
    return "curr winner is %s with %s favorites and %s retweets" % (
        curr_top_candidate, curr_top_features["tot_favorites"], curr_top_features["tot_retweets"])
