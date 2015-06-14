__author__ = 'qingluo'

import smtplib
# Import the email modules we'll need
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


class Gmail(object):
    def __init__(self, email, password):
        self.email = email
        self.password = password
        self.server = 'smtp.gmail.com'
        self.port = 587
        session = smtplib.SMTP(self.server, self.port)
        session.ehlo()
        session.starttls()
        session.ehlo
        session.login(self.email, self.password)
        self.session = session

    def send_message(self, subject, body, to):
        ''' This must be removed '''
        headers = [
            "From: " + self.email,
            "Subject: " + subject,
            "To: " + to,
            "MIME-Version: 1.0",
            "Content-Type: text/html"]
        headers = "\r\n".join(headers)
        self.session.sendmail(
            self.email,
            to,
            headers + "\r\n\r\n" + body)

    def send_text_attachment(self, subject, to, text_file, folder):
        msg = MIMEMultipart()
        msg['Subject'] = subject
        msg['To'] = to
        fp = open(folder + "/" +text_file, 'rb')
        attachment = MIMEText(fp.read())
        fp.close()
        attachment.add_header("Content-Disposition", "attachment", filename=text_file)
        msg.attach(attachment)

        self.session.sendmail(self.email, to, msg.as_string())

    # def send_email(text_file, date):
    # # Open a plain text file for reading.  For this example, assume that
    # #  the text file contains only ASCII characters.
    # msg = MIMEMultipart()
    # fp = open(text_file, 'rb')
    # # Create a text/plain message
    # attachment = MIMEText(fp.read())
    # fp.close()
    # me = "raki1978wmc6731@gmail.com"
    # you = "luoqing222@gmail.com"
    # msg['Subject'] = 'The analysis on of %s' % date
    # msg['From'] = me
    # msg['To'] = you
    # attachment.add_header("Content-Disposition", "attachment", filename=text_file)
    # msg.attach(attachment)
