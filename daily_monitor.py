import datetime
import globals
import smtplib

from crawler.facebook_crawler import FacebookCrawler
from crawler.tweet_crawler import TweetCrawler
from email import encoders
from email.mime.application import MIMEApplication
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


def daily_monitor():

    globals.data_configure.output_directory = "./"
    globals.data_configure.start_date = datetime.datetime.now().strftime('%Y-%m-%d')
    globals.data_configure.end_date = (datetime.datetime.now() + datetime.timedelta(hours=24)).strftime("%Y-%m-%d")

    fb_crawler = FacebookCrawler()
    fb_output_file = fb_crawler.output_content()

    tw_crawler = TweetCrawler()
    tt_output_file = tw_crawler.output_content()

    print(fb_output_file)
    print(tt_output_file)

    sender = 'ailabchenli@tencent.com'
    receiver = 'ailabchenli@tencent.com'
    # cc = ["abrahamzhan@tencent.com", "ailabchenli@tencent.com"]
    subject = "Output from FB and TT on " + globals.data_configure.start_date
    body = ""

    smtpserver = 'tsmtp.tencent.com'
    username = 'ailabchenli'
    password = globals.data_configure.email_sender_pwd

    message = MIMEMultipart()
    message["Subject"] = subject
    message["From"] = sender
    message["To"] = receiver
    # message["CC"] = ",".join(cc)

    # Add body to email
    message.attach(MIMEText(body, "plain"))

    for filename in [fb_output_file, tt_output_file]:
        attachment = MIMEApplication(open(filename, "rb").read(), _subtype="txt")
        if filename[0] == ".":
            filename = filename[1:]
        attachment.add_header('Content-Disposition', 'attachment', filename=filename)
        message.attach(attachment)

    smtp = smtplib.SMTP()
    smtp.connect(smtpserver)
    smtp.login(username, password)
    smtp.sendmail(sender, receiver, message.as_string())
    smtp.quit()
