"""
Fetch the list of candidates from wikipedia and check if every candidate
has its own associated twitter handle in the DB.

Fetch a single section from a wikipedia page is a 2 steps process:
1. fetch the list of sections from the page
   "2020_Democratic_Party_presidential_primaries"
2. fetch the section whose id corresponds to the anchor
   "Declared_candidates_and_exploratory_committees"

Once this section is obtained, use HTMLParser to extract the list of
candidates.
"""
import logging
import pandas as pd
import requests
import unidecode

from dbs import utils_db


def fetch_candidates_remote(
        task_instance,
        xcom_push_remote_candidates="candidates_remote",
        page="2020_Democratic_Party_presidential_primaries",
        anchor="Declared_candidates_and_exploratory_committees",
        **kwargs
    ):
    """
    Fetch a list of candidates from the selected page.anchor.

    :param str page: the target wikipedia page
    :param str anchor: the target anchor
    """
    logger = logging.getLogger()
    session = requests.Session()
    PARAMS_sections = {
        'action': "parse",
        'page': page,
        'prop': 'sections',
        'format': "json"
    }
    logging.debug("retrieval section list of %s", page)
    # get the list of sessions
    r = session.get(url="https://en.wikipedia.org/w/api.php", params=PARAMS_sections)
    r.raise_for_status()

    # once we have the list of sections, get the right anchor
    section_id = None
    for json_section in r.json()["parse"]["sections"]:
        if json_section["anchor"] == anchor:
            section_id = json_section["index"]
            break
    else:
        logger.debug(r.json())
        raise RuntimeError("no section {} found in page {}".format(anchor, page))

    # get the content at the appropriate section
    PARAMS = {
        'action': "parse",
        'page': page,
        # Get parsed text as html
        'prop': 'text',
        'section': str(section_id),
        'format': "json"
    }
    r = session.get(url="https://en.wikipedia.org/w/api.php", params=PARAMS)
    r.raise_for_status()
    text = r.json()['parse']['text']["*"]
    candidate_table = pd.io.html.read_html(text, attrs={'class':'wikitable'})[0]
    # NOTE: force ASCII conversion
    candidate_set = set(unidecode.unidecode(n) for n in candidate_table["Name"].values)
    task_instance.xcom_push(key=xcom_push_remote_candidates, value=candidate_set)
    return candidate_set


def fetch_candidates_local(task_instance,
                           table,
                           xcom_push_local_candidates="candidates_local",
                           **kwargs):
    """Get the list of candidates from the local sqlite db."""
    candidates_set = set([name for name, _ in utils_db.get_candidates_name_handle(table)])
    task_instance.xcom_push(key=xcom_push_local_candidates, value=candidates_set)
    return candidates_set


def compare_candidates_list(
        task_instance,
        xcom_pull_remote_candidates="candidates_remote",
        xcom_pull_local_candidates="candidates_local",
        **kwargs
    ):
    """
    Fetch two lists of candidates and compare them.

    Raise a ValueError if the two lists are different.

    :param str xcom_pull_remote_candidates: the key to retrieve the
        list of candidates from the remote task
    :param str xcom_pull_local_candidates_key: the key to retrieve the
        list of candidates from the local task
    :returns: a log string (anything that is returned will be logged)
    :rtype: str
    :raises: ValueError if the two lists are differents
    """
    remote_candidates_list = task_instance.xcom_pull(key=xcom_pull_remote_candidates)
    local_candidates_list = task_instance.xcom_pull(key=xcom_pull_local_candidates)
    remote_uniques = set(remote_candidates_list) - set(local_candidates_list)
    if remote_uniques:
        raise ValueError("local candidate list miss items {}".format(remote_uniques))
    return "same number of elements in both lists: {}".format(len(remote_candidates_list))


def main(task_instance, **kwargs):
    """Helper main to perform both operations in one task."""
    fetch_candidates_remote(task_instance, xcom_push_remote_candidates="remote_candidates")
    fetch_candidates_local(task_instance,
                           table="candidates",
                           xcom_push_local_candidates="local_candidates")
    return compare_candidates_list(task_instance,
                                   xcom_pull_remote_candidates="remote_candidates",
                                   xcom_pull_local_candidates="local_candidates")
