import unittest
import smtpd
import threading
from scrapydd.mail import MailSender
from scrapydd.config import Config
import asyncore
import logging


class TestingSMTPServer(smtpd.SMTPServer, threading.Thread):
    def __init__(self, port=25):
        smtpd.SMTPServer.__init__(
            self,
            ('localhost', port),
            ('localhost', port),
        )
        threading.Thread.__init__(self)
        self.received_data=None

    def process_message(self, peer, mailfrom, rcpttos, data, **kwargs):
        logging.debug(data)
        self.received_peer = peer
        self.received_mailfrom = mailfrom
        self.received_rcpttos = rcpttos
        self.received_data = data

        # Import the email modules we'll need
        from email.parser import Parser

        headers = Parser().parsestr(data)

        logging.debug( 'To: %s' % headers['to'])
        logging.debug( 'From: %s' % headers['from'])
        logging.debug( 'Subject: %s' % headers['subject'])
        self.to_address = headers['to']
        self.from_address = headers['from']
        self.subject = headers['subject']
        if not headers.is_multipart():
            self.body = headers.get_payload()

    def run(self):
        asyncore.loop()

class MailSenderTest(unittest.TestCase):
    def test_send(self):
        smtp_server = TestingSMTPServer(26)
        smtp_server.start()

        smtp_from = 'from@address.com'
        config = Config(values={
                'smtp_port':'26',
                'smtp_server':'localhost',
                'smtp_user': 'test_user',
                'smtp_passwd': 'test_password',
                'smtp_from': smtp_from})
        target = MailSender(config)

        to_address = 'to@address.com'
        subject = 'some_subject'
        plain_text_content = 'some content'
        target.send(to_address, subject, plain_text_content)
        smtp_server.close()

        self.assertEqual(smtp_from, smtp_server.received_mailfrom)
        self.assertEqual(to_address, smtp_server.to_address)
        self.assertEqual(subject, smtp_server.subject)
        self.assertEqual(plain_text_content, smtp_server.body)



