#!/usr/bin/env python3

import csv
import itertools
import logging
import os
import sched
import smtplib
import textwrap
import threading
import time
from argparse import ArgumentParser
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Dict
from typing import List


class UniqueJournal:
    def __init__(self, filename: str):
        self._filename = filename
        if not os.path.exists(filename):
            open(self._filename, "w").close()

    def get_rows(self) -> List[str]:
        return self._rows()

    def add(self, row: str) -> None:
        with threading.Lock():
            rows = self._rows()
            if row in rows:
                return

            rows.append(row)
            self._save(rows)

    def _rows(self) -> List[str]:
        with open(self._filename) as f:
            return [i.rstrip("\n") for i in f.readlines() if i != "\n"]

    def _save(self, rows: List[str]) -> None:
        with open(self._filename, mode="w") as f:
            f.write("\n".join(rows))


def configure_loggers() -> None:
    formatter = logging.Formatter(fmt="%(asctime)s [%(levelname)s] - %(message)s")
    file_handler = logging.FileHandler(filename="mailer.log")
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(logging.INFO)
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)


def init_parser() -> ArgumentParser:
    parser = ArgumentParser()
    parser.add_argument(
        "-s",
        "--senders",
        type=str,
        default="senders.csv",
        help="senders CSV table filename",
    )
    parser.add_argument(
        "-r",
        "--recipients",
        type=str,
        default="recipients.csv",
        help="recipients CSV table filename",
    )
    return parser


def log_error(f):
    def wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.exception(repr(e))
            raise

    return wrapper


@dataclass
class Sender:
    email: str
    password: str

    def __hash__(self) -> int:
        return hash(self.email)


@dataclass
class Recipient:
    email: str
    variables: dict

    def __hash__(self) -> int:
        return hash(self.email)


def load_senders(filename: str) -> List[Sender]:
    result = []
    with open(filename) as f:
        csvreader = csv.reader(f, delimiter=";")
        for row in csvreader:
            result.append(Sender(email=row[0], password=row[1]))
    return result


def load_recipients(filename: str) -> List[Recipient]:
    result = []
    with open(filename) as f:
        csvreader = csv.reader(f, delimiter=";")
        for row in csvreader:
            result.append(
                Recipient(
                    email=row[0],
                    variables={
                        "conf_name": row[3],
                        "conf_acronym": row[1],
                        "conf_year": row[2],
                    },
                )
            )
    return result


def split_recipients_by_senders(
    senders: List[Sender], recipients: List[Recipient]
) -> Dict[Sender, List[Recipient]]:
    result = {i: [] for i in senders}
    senders_cycle = itertools.cycle(senders)
    recipients_queue = deque(recipients)
    while len(recipients_queue) != 0:
        for sender in senders_cycle:
            try:
                result[sender].append(recipients_queue.popleft())
            except IndexError:
                break
    return result


def make_email_message(sender_email: str, recipient: Recipient) -> MIMEMultipart:
    message = MIMEMultipart("alternative")
    message["Subject"] = make_message_subject(recipient)
    message["From"] = make_message_from(sender_email)
    message["To"] = recipient.email

    message.add_header(
        "List-Unsubscribe", f"<mailto:{sender_email}?subject=Unsubscribe>"
    )

    body = make_message_body(recipient)
    message.attach(MIMEText(body, "plain"))

    return message


def make_message_subject(recipient: Recipient) -> str:
    v = recipient.variables
    return f"Conference “{v['conf_name']} ({v['conf_acronym']})” Survey"


def make_message_from(sender_email: str) -> str:
    return f"Conference Committee <{sender_email}>"


def make_message_body(recipient: Recipient) -> str:
    v = recipient.variables
    return textwrap.dedent(
        f"""\
        Dear colleague!

        You participated in the “{v['conf_name']} ({v['conf_acronym']} {v['conf_year']}).”

        Please take a moment and answer a short conference survey. This will help us to continuously improve the service we provide to you.

        https://bit.ly/3mciBZE

        Thank you for your time. It is truly appreciated and we hope to see you soon!


        --

        Quality Assessment Team
        of Conference Planning Committee"""  # noqa: E501
    )


def send_email(
    sender_email: str, password: str, recipient_email: str, message: MIMEMultipart
) -> None:
    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.ehlo()
        server.starttls()
        server.login(sender_email, password)
        server.sendmail(sender_email, recipient_email, message.as_string())


def make_email_and_send(
    sender: Sender, recipient: Recipient, processed_emails: UniqueJournal
) -> None:
    logger = logging.getLogger(__name__)
    message = make_email_message(sender.email, recipient)

    try:
        send_email(sender.email, sender.password, recipient.email, message)

    except Exception as e:
        logger.error(f"fail sending {sender.email} > {recipient.email} ({repr(e)})")

    else:
        logger.info(f"successful sending {sender.email} > {recipient.email}")

        try:
            processed_emails.add(recipient.email)
        except Exception as e:
            logger.error(f"Can't save {recipient.email} as processed email ({repr(e)})")


@log_error
def main():
    configure_loggers()

    logger = logging.getLogger(__name__)
    logger.info("mailer starts working ...")

    parser = init_parser()
    args = parser.parse_args()

    senders = load_senders(args.senders)
    recipients = load_recipients(args.recipients)
    processed_emails = UniqueJournal("processed-emails.txt")
    scheduler = sched.scheduler(time.time, time.sleep)
    next_iteration_at = datetime.now()

    while True:
        remaining_recipients = [
            r for r in recipients if r.email not in processed_emails.get_rows()
        ]
        logger.info(f"{len(remaining_recipients)} recipients left")

        senders_recipients = split_recipients_by_senders(senders, remaining_recipients)
        for sender, recipients_part in senders_recipients.items():
            limit = min(len(recipients_part), 50)
            for i in range(limit):
                argument = (sender, recipients_part[i], processed_emails)
                scheduler.enter(i * 1560, 1, make_email_and_send, argument=argument)
        scheduler.run()

        if len(recipients) == len(processed_emails.get_rows()):
            logger.info("there aren't any recipients more, exit")
            exit()

        next_iteration_at += timedelta(days=1)

        logger.info(f"sleep until {next_iteration_at} ...")
        time.sleep((next_iteration_at - datetime.now()).total_seconds())


if __name__ == "__main__":
    main()
