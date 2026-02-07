"""Read events from Elite Dangerous logfiles"""

import functools
import os
import logging
import pytz
import dateutil
import datetime
import ntpath
import hashlib

import json

import typing


# %% edjournal_event_t -----------------------------------------------
class EDJournalEvent(typing.NamedTuple):
    timestamp: str
    eventname: str
    event: dict


# %% salted_event_hasher ---------------------------------------------

def salted_event_hasher(salt: str) -> typing.Callable[[EDJournalEvent], hashlib._hashlib.HASH]:

    def _hash(event: EDJournalEvent):
        h = hashlib.md5(salt.encode("utf-8"))
        h.update(event.eventname.encode("utf-8"))
        h.update(str(event.timestamp).encode("utf-8"))
        h.update(json.dumps(event.event).encode("utf-8"))

        return h

    return _hash


syslog = logging.getLogger(__name__)

EDJournalEventIterator_T = typing.Iterator[EDJournalEvent]


# %% make_datetime ---------------------------------------------------

def _make_datetime(d):
    # use parser to convert from string representation
    if isinstance(d, str):
        return dateutil.parser.parse(d)
    # ints and floats are asumed to be unix timestamps (seconds sinds 1/1/1970)
    elif isinstance(d, int):
        return datetime.datetime.fromtimestamp(d)
    elif isinstance(d, float):
        return datetime.datetime.fromtimestamp(d)
    # for other cases use the object's string cast and parse that
    else:
        return dateutil.parser.parse(str(d))


def make_datetime(d, tz='UTC'):
    _d = _make_datetime(d)
    if _d.tzinfo is None or _d.tzinfo.utcoffset(_d) is None:
        return pytz.timezone(tz).localize(_d)

    return _d


# %% create_edjournal_event_from_logevent ----------------------------
def create_edjournal_event_from_logevent(logged_line: str) -> EDJournalEvent:
    logged_event = json.loads(logged_line)
    return EDJournalEvent(
        timestamp=make_datetime(logged_event.pop("timestamp")),
        eventname=logged_event.pop("event"),
        event=logged_event
    )


# %% read_journal ----------------------------------------------------

def read_journal(
    journal: str,    # path to journal file
    tail=True       # finish on end of input or wait for new events
) -> EDJournalEventIterator_T:
    """
        Returns a generator of journal events
        journal: path to journal
        tail:    finish on end of input or wait for new events (finishes on 'Shutdown' event)
    """

    last_timestamp = None

    try:
        syslog.debug(f"reading journal: {ntpath.basename(journal)}")

        with open(journal, encoding="utf-8") as journalfile:
            syslog.debug(f"Opening journal {journal}")

            while True:
                line = journalfile.readline()

                if not line:
                    if tail:
                        time.sleep(0.3)
                        continue
                    else:
                        break

                if len(line) < 5:
                    continue

                try:
                    event = create_edjournal_event_from_logevent(line)

                except json.decoder.JSONDecodeError as JX:
                    syslog.exception("Exception: %s", JX,
                                     exc_info=True, stack_info=True)
                    raise

                if last_timestamp is None:
                    yield EDJournalEvent(
                        eventname="JournalStart",
                        timestamp=event.timestamp,
                        event=dict(logfile=ntpath.basename(journal))
                    )

                last_timestamp = event.timestamp
                yield event

                if event.eventname == 'Shutdown':
                    syslog.debug(
                        f"SHUTDOWN {str(event.timestamp):22} {ntpath.basename(journal)}")
                    break

        syslog.debug(f"Done reading journal: {journal}")
        yield EDJournalEvent(
            eventname='JournalFinished',
            timestamp=last_timestamp,
            event=dict(logfile=f"{ntpath.basename(journal)}")
        )

    except Exception as err:
        syslog.exception(f"Exception reading {journal}")
        yield EDJournalEvent(
            eventname='JournalFinished',
            timestamp=last_timestamp,
            event=dict(logfile=f"{ntpath.basename(journal)}",
                       exception=type(err).__name__)
        )

    finally:
        syslog.debug(f"Exiting journal: {journal}")


# %% list_journals_unsorted ------------------------------------------

def list_journals_unsorted(journalpath: str) -> typing.Iterator[str]:
    'Generator for journal file paths'

    yield from (os.path.join(journalpath, f) for f in os.listdir(journalpath) if 'Journal' in f.split('.')[0] and '.log' in f)


# %% list_journals_sorted --------------------------------------------

def list_journals_sorted(journalpath: str) -> list[str]:

    return sorted(
        list_journals_unsorted(journalpath),
        key=lambda f: f.replace(
            '-', '').replace('Journal.20', 'Journal.').replace('T', '')
    )


# %% track_journals --------------------------------------------------

def track_journals(
    journalpath: str,
    backlog: int | str = 0
) -> EDJournalEventIterator_T:
    '''Iterable for Journal events spanning multiple log files'''

    try:

        logfiles = list_journals_sorted(journalpath)

        if isinstance(backlog, int):
            backlog = min(backlog, len(logfiles)-1)
            syslog.debug(f"Reading journals, backlog = {backlog}")
            for f in logfiles[-(1+backlog):]:
                yield from read_journal(f, tail=False)

        elif isinstance(backlog, str):
            syslog.debug(f"Reading journals, backlog = {backlog}")
            for f in functools.reduce(
                lambda t, j: t if not t and backlog not in j else t + [j],
                logfiles, []
            ):
                yield from read_journal(f, tail=False)

    except KeyboardInterrupt as kbi:
        syslog.info(f"Keyboard Interrupt {kbi.info()}")
