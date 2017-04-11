import smtplib
from email.mime.text import MIMEText


class MailSender(object):
    def __init__(self, config):
        self.smtp_server = config.get('smtp_server')
        self.smtp_port = config.getint('smtp_port')
        self.smtp_user = config.get('smtp_user')
        self.smtp_passwd = config.get('smtp_passwd')
        self.smtp_from = config.get('smtp_from')

    def send(self, to_addresses, subject, content, content_as_html=False, attchements=None):
        msg = MIMEText(content)

        msg['Subject'] = subject
        msg['From'] = self.smtp_from
        msg['To'] = to_addresses

        smtp = smtplib.SMTP(self.smtp_server, self.smtp_port)
        if self.smtp_user and self.smtp_passwd:
            smtp.login(self.smtp_user, self.smtp_passwd)
        smtp.sendmail(self.smtp_from, [to_addresses], msg.as_string())
        smtp.quit()

