import os

class DataConfigure(object):

    def __init__(self,
                 chrome_drive_path,
                 headless,
                 db_host,
                 db_port,
                 db_user,
                 db_pwd,
                 email_sender_pwd,
                 fb_db_name,
                 tw_db_name,
                 fb_account,
                 fb_pwd,
                 sleep_time,
                 facebook_url,
                 facebook_search_keywords,
                 tweet_url,
                 tweet_search_keywords,
                 ):

        self.chrome_drive_path = chrome_drive_path
        self.headless = headless
        self.db_host = db_host
        self.db_port = db_port
        self.db_user = db_user
        self.db_pwd = db_pwd if db_pwd else os.environ.get("DB_PWD")
        self.fb_db_name = fb_db_name
        self.tw_db_name = tw_db_name
        self.sleep_time = sleep_time
        self.facebook_url = facebook_url
        self.tweet_url = tweet_url
        self.output_directory = ""
        self.start_date = ""
        self.end_date = ""
        self.fb_account = fb_account if fb_account else os.environ.get("FB_USR")
        self.fb_pwd = fb_pwd if fb_pwd else os.environ.get("FB_PWD")
        self.facebook_search_keywords = facebook_search_keywords
        self.tweet_search_keywords = tweet_search_keywords
        self.email_sender_pwd = email_sender_pwd
