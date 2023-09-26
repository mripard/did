# coding: utf-8
"""
Public-Inbox stats about mailing lists threads

Config example::

    [wiki]
    type = public-inbox
    url = https://lore.kernel.org
"""

import copy
import datetime
import email.utils
import gzip
import mailbox
import tempfile
import urllib.parse

import requests

from did import utils
from did.base import Config, ConfigError, Date, ReportError, User
from did.stats import Stats, StatsGroup
from did.utils import item, log


class Message(object):
    def __init__(self, msg: mailbox.mboxMessage) -> None:
        self.msg = msg

    def __msg_id(self, keyid: str) -> str:
        msgid = self.msg[keyid]
        if msgid is None:
            return None

        return msgid.lstrip("<").rstrip(">")

    def id(self) -> str:
        return self.__msg_id("Message-Id")

    def parent_id(self) -> str:
        return self.__msg_id("In-Reply-To")

    def subject(self) -> str:
        subject = self.msg["Subject"]

        subject = " ".join(subject.splitlines())
        subject = " ".join(subject.split())

        return subject

    def date(self) -> datetime.datetime:
        return email.utils.parsedate_to_datetime(self.msg["Date"])

    def is_thread_root(self) -> bool:
        return self.parent_id() is None

    def is_from_user(self, user: str) -> bool:
        msg_from = email.utils.parseaddr(self.msg["From"])[1]

        return email.utils.parseaddr(user)[1] == msg_from

    def is_between_dates(self, since: Date, until: Date) -> bool:
        msg_date = self.date().date()

        return msg_date >= since.date and msg_date <= until.date


def _unique_messages(mbox: mailbox.mbox):
    msgs = dict()
    for msg in mbox.values():
        msg = Message(msg)
        id = msg.id()

        if id not in msgs:
            msgs[id] = msg
            yield msg


class PublicInbox(object):
    def __init__(self, user: User, url: str) -> None:
        self.url = url
        self.user = user

    def __get_url(self, path: str) -> str:
        return urllib.parse.urljoin(self.url, path)

    def _get_message_url(self, msg: Message) -> str:
        return self.__get_url("/r/%s/" % msg.id())

    def __get_mbox_from_content(self, content: bytes) -> mailbox.mbox:
        content = gzip.decompress(content)

        with tempfile.NamedTemporaryFile() as tmp:
            tmp.write(content)
            tmp.seek(0)

            return mailbox.mbox(tmp.name)

    def __get_thread_root(self, msg: Message) -> Message:
        url = self.__get_url("/all/%s/t.mbox.gz" % msg.id())
        resp = requests.get(url)
        mbox = self.__get_mbox_from_content(resp.content)
        for msg in mbox.values():
            msg = Message(msg)
            reply = msg.parent_id()
            if reply is None:
                return msg

    def get_all_threads(self, since: Date, until: Date):
        since_str = since.date.isoformat()
        until_str = until.date.isoformat()

        resp = requests.post(
            self.__get_url("/all/"),
            headers={"Content-Length": "0"},
            params={
                "q": "(f:%s AND d:%s..%s)"
                % (self.user.email, since_str, until_str),
                "x": "m",
            },
        )

        found = list()
        mbox = self.__get_mbox_from_content(resp.content)
        for msg in _unique_messages(mbox):
            msg_id = msg.id()
            if msg_id in found:
                continue

            if not msg.is_thread_root():
                root = self.__get_thread_root(msg)
                root_id = root.id()
                if root_id in found:
                    continue

                found.append(root_id)
                yield root
            else:
                found.append(msg_id)
                yield msg


class NewThreads(Stats):
    """Mails Threads Started"""

    def fetch(self):
        log.info(
            "Searching for new threads on {0} started by {1}".format(
                self.parent.url,
                self.user,
            )
        )

        self.stats = [
            msg
            for msg in self.parent.pi.get_all_threads(
                self.options.since, self.options.until
            )
            if msg.is_from_user(self.user.email)
            and msg.is_between_dates(self.options.since, self.options.until)
        ]

    def show(self):
        if not self._error and not self.stats:
            return

        self.header()
        for msg in self.stats:
            utils.item(msg.subject(), level=1, options=self.options)

            opt = copy.deepcopy(self.options)
            opt.width = 0
            utils.item(self.parent.pi._get_message_url(msg), level=2, options=opt)


class InvolvedThreads(Stats):
    """Mails Threads Involved In"""

    def fetch(self):
        log.info(
            "Searching for new threads on {0} started by {1}".format(
                self.parent.url,
                self.user,
            )
        )

        self.stats = [
            msg
            for msg in self.parent.pi.get_all_threads(
                self.options.since, self.options.until
            )
            if not msg.is_from_user(self.user.email)
            or not msg.is_between_dates(self.options.since, self.options.until)
        ]

    def show(self):
        if not self._error and not self.stats:
            return

        self.header()
        for msg in self.stats:
            utils.item(msg.subject(), level=1, options=self.options)

            opt = copy.deepcopy(self.options)
            opt.width = 0
            utils.item(self.parent.pi._get_message_url(msg), level=2, options=opt)


class PublicInboxStats(StatsGroup):
    """Public-Inbox Mailing List Archive"""

    order = 1000

    def __init__(self, option, name=None, parent=None, user=None):
        StatsGroup.__init__(self, option, name, parent, user)

        config = dict(Config().section(option))
        try:
            self.url = config["url"]
        except KeyError:
            raise ReportError("No url in the [{0}] section".format(option))

        self.pi = PublicInbox(self.user, self.url)
        self.stats = [
            InvolvedThreads(option=option + "-involved", parent=self),
            NewThreads(option=option + "-started", parent=self),
        ]
