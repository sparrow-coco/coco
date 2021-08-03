import codecs
import datetime
import globals
import json
import pymysql
import pytz
import requests
import time
import traceback

from bs4 import BeautifulSoup
from utils.func import extract_number
from threadings.persistent_db import DBPool
from threadings.browser_pool import BrowserPool
from utils import network_utils
from dateutil import parser as date_parser
from selenium import webdriver


class TweetCrawler(object):

    def __init__(self):
        self.max_tweet_request_one_time = 10
        self.max_tweet_request_move_down = 5
        self.max_scroll_number = 2
        self.max_scroll_number_from_search_page = 20
        self.today_str = datetime.datetime.fromtimestamp(int(time.time()), pytz.timezone("America/Los_Angeles")).strftime('%Y-%m-%d')
        self.bearer_token = "AAAAAAAAAAAAAAAAAAAAABzUKQEAAAAAiObtqVd4r1K7tVu5ksxpitDD918%3Dm7Xd1IdBPlyKwTO4guCsgXMYHihHs6B7vKzkbup3oLcY43ISn2"
        self.language = "en"

        self.db_pool = DBPool(database=globals.data_configure.tw_db_name) # this should create connection on a per-Thread basis

        self.create_post_table()
        self.create_reply_table()
        return

    def create_post_table(self):

        sql_create_table = "CREATE TABLE IF NOT EXISTS " + "post" + \
                           "(`id` varchar(100) NOT NULL," \
                           "`time` datetime DEFAULT NULL," \
                           "`content` text," \
                           "`retweets` int DEFAULT 0," \
                           "`quotetweets` int DEFAULT 0,"\
                           "`likes` int DEFAULT 0," \
                           "`lang` varchar(10) NULL," \
                           "PRIMARY KEY (`id`)" \
                           "    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
        db, cursor = self.db_pool.get_conn_and_cursor()
        cursor.execute(sql_create_table)
        db.commit()

    def insert_post(self, post_id, publish_time, content, retweets, quotetweets, likes, lang='en'):
        find_sql = "select count(*) from post where id=%s"
        db, cursor = self.db_pool.get_conn_and_cursor()
        cursor.execute(find_sql, (post_id))
        count = cursor.fetchone()[0]
        
        if count != 0:
            try:
                update_sql = "update post set retweets=%s, quotetweets=%s, likes=%s where id=%s;"
                cursor.execute(update_sql,
                                    (str(retweets),
                                     str(quotetweets),
                                     str(likes),
                                     post_id))
                db.commit()
                # print("Successfully update a post: ", str(publish_time) + " " + content.replace("\n", ""), retweets, quotetweets, likes)
                print("Successfully update a post: ", str(publish_time), retweets, quotetweets, likes)
            except Exception as e:

                db.rollback()
                traceback.print_exc()
                print(u"update error!\n%s", e)
        else:
            try:
                sql_str = "insert into post" + \
                          " (id ,time, content, retweets, quotetweets, likes, lang)" \
                          " values (%s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE id=values(id)," \
                          "retweets=values(retweets), quotetweets=values(quotetweets), likes=values(likes);"
                cursor.execute(sql_str,
                                    (post_id,
                                     publish_time,
                                     content,
                                     str(retweets),
                                     str(quotetweets),
                                     str(likes),
                                     lang))
                db.commit()
                #print("Successfully insert a post: ", str(publish_time) + " " + content.replace("\n", ""), retweets, quotetweets, likes)
                print("Successfully insert a post: ", str(publish_time), retweets, quotetweets, likes)
                return True
            except Exception as e:

                db.rollback()
                traceback.print_exc()
                print(u"insert error!\n%s", e)
                return False

    def create_reply_table(self):
        db, cursor = self.db_pool.get_conn_and_cursor()
        sql_create_table = "CREATE TABLE IF NOT EXISTS " + "reply" + \
                           "(`time` datetime NOT NULL," \
                           "`user_name` varchar(100) NOT NULL," \
                           "`content` text," \
                           "`twitter_id` varchar(100) NOT NULL," \
                           "`lang` varchar(10) NULL," \
                           "PRIMARY KEY (`time`, `user_name`)" \
                           " ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
        cursor.execute(sql_create_table)
        db.commit()

    def insert_reply(self,
                     publish_time,
                     user_name,
                     content,
                     twitter_id,
                     lang='en'):
        try:
            db, cursor = self.db_pool.get_conn_and_cursor()
            sql_str = "insert into reply" + \
                      " (time ,user_name, content, twitter_id, lang)" \
                      " values (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE content=values(content);"
            cursor.execute(sql_str,
                                (publish_time,
                                 user_name,
                                 content,
                                 twitter_id,
                                 lang))
            db.commit()
            #print("Successfully insert a reply: ", publish_time + " " + content)
            print("Successfully insert a reply: ", publish_time)
            return True
        except Exception as e:

            db.rollback()
            traceback.print_exc()
            print(u"insert error!\n%s", e)
            return False

    def __execute(self, tweet_user_name, browser:webdriver.Chrome=None, browser_pool:BrowserPool=None):
        tweet_list = self.get_recent_tweet_ids(tweet_user_name)
        print("[%d] tweets from [%s]. " % (len(tweet_list), tweet_user_name))
        for one_tweet_info in tweet_list:
            self.get_reply_from_tweet_page(browser, tweet_user_name, one_tweet_info)
        return

    # you need to call join() outside of this function
    def execute(self, tweet_user_name, browser_pool:BrowserPool=None):
        browser_pool.execute_with_browser(self.__execute, tweet_user_name)
        # join() is called outside of this
        return 

    def transfer_time_to_utc(self, input_time_str):

        input_time_obj = datetime.datetime.strptime(input_time_str, "%Y-%m-%d %H:%M:%S")
        utc_time_obj = (input_time_obj + datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S")

        return str(utc_time_obj).strip()

    def create_url(self, tweet_user_name):
        # tweet_name = "LeagueOfLegends"
        max_count = self.max_tweet_request_one_time
        end_time = self.today_str + "T00:00:00Z"
        tweet_fields = "tweet.fields=created_at"
        url = "https://api.twitter.com/2/tweets/search/recent?query=from:%s&max_results=%s&end_time=%s&%s" % (
            tweet_user_name,
            max_count,
            end_time,
            tweet_fields)
        return url

    def get_recent_tweet_ids(self, tweet_user_name):

        tweet_list = []
        url = self.create_url(tweet_user_name)
        headers = {"Authorization": "Bearer {}".format(self.bearer_token)}
        response = requests.request("GET", url, headers=headers)
        print("Response status from Tweet API: %d" % int(response.status_code))
        if response.status_code != 200 or "data" not in response.json():
            print(response.status_code, response.text)
            return tweet_list

        for tweet_obj in response.json()["data"]:
            modified_id = tweet_user_name + "_" + tweet_obj["id"]
            tweet_list.append((modified_id,
                               tweet_obj["id"],
                               tweet_obj["created_at"].replace("T", " ")[:19],
                               tweet_obj["text"]))

        return tweet_list

    def save_post_into_db(self, tweet_info, chrome_browser):

        modified_id = tweet_info[0]
        tweet_id = tweet_info[1]
        time_str = tweet_info[2]
        tweet_text = tweet_info[3]

        html_obj = BeautifulSoup(chrome_browser.page_source, 'lxml')
        all_a_blocks = html_obj.find_all("a", attrs={"dir":"auto", "role":"link"})
        retweets = 0
        quotetweets = 0
        likes = 0
        for one_a_block in all_a_blocks:
            if "href" in one_a_block.attrs and one_a_block.attrs["href"].find(tweet_id + "/retweets/with_comments") >= 0:
                quotetweets = extract_number(one_a_block.text, "Quote Tweets")
            elif "href" in one_a_block.attrs and one_a_block.attrs["href"].find(tweet_id + "/retweets") >= 0:
                retweets = extract_number(one_a_block.text, "Retweets")
            elif "href" in one_a_block.attrs and one_a_block.attrs["href"].find(tweet_id + "/likes") >= 0:
                likes = extract_number(one_a_block.text, "Likes")

        self.insert_post(modified_id, time_str, tweet_text, retweets, quotetweets, likes)

    def __find_all_reply_data(self, chrome:webdriver.Chrome):
        def twitter_request_api_filter(log:dict, resp_url:str, resp_body:str):
            try:
                quicktest = resp_url is not None and "twitter.com/i/api/2/timeline/conversation/" in resp_url
                if not quicktest:
                    return False
                actual_body = json.loads(resp_body["body"])
            except:
                return False
            return actual_body.get("globalObjects") is not None \
                and actual_body["globalObjects"].get("tweets") is not None
        
        def twitter_api_data_extractor(resp_body:str):
            ret = set()
            uid_to_username_map = {}

            resp_body = json.loads(resp_body["body"])
            user_edges = resp_body["globalObjects"]["users"]
            for uid, node in user_edges.items():
                try:
                    uid_to_username_map[uid] = node["screen_name"]
                except:
                    pass
            
            tweet_edges = resp_body["globalObjects"]["tweets"]
            for tid, node in tweet_edges.items():
                try:
                    user_name = uid_to_username_map[node["user_id_str"]]
                    publish_time = node["created_at"]
                    publish_time = date_parser.parse(publish_time)
                    content = node["full_text"]
                    data = (publish_time, user_name, content)
                    ret.add(data)
                except:
                    pass
            return ret

        return network_utils.get_data_from_browser_network(chrome, twitter_request_api_filter, twitter_api_data_extractor)


    def get_reply_from_tweet_page(self, chrome_browser, tweet_user_name, tweet_info):
        try_count = 0
        added_replies = 0

        tweet_url = "https://twitter.com/" + tweet_user_name + "/status/" + tweet_info[1]
        chrome_browser.get(tweet_url)
        time.sleep(int(globals.data_configure.sleep_time / 10))

        self.save_post_into_db(tweet_info, chrome_browser)
        time.sleep(int(globals.data_configure.sleep_time))
        
        last_height = chrome_browser.execute_script("return document.body.scrollHeight")
        while True:
            replies = self.__find_all_reply_data(chrome_browser)
            for reply in replies:
                publish_time, user_name, content_str = reply
                self.insert_reply(publish_time, user_name, content_str, tweet_info[0])
                added_replies += 1

            if added_replies > 0:
                print("# of current added replies at current round: %d" % added_replies)
                try_count = 0
                added_replies = 0
            else:
                print("All comments have been saved at current turn!")
                if try_count > self.max_scroll_number:
                    break

            chrome_browser.execute_script("window.scrollBy(0, document.body.scrollHeight)")
            time.sleep(int(globals.data_configure.sleep_time))
            new_height = chrome_browser.execute_script("return document.body.scrollHeight")
            # all post processed
            if last_height == new_height:
                time.sleep(int(globals.data_configure.sleep_time)) # give it a second try
                if last_height == chrome_browser.execute_script("return document.body.scrollHeight"):
                    print("Scrolled to the end")
                    break
            try_count += 1
        return

    def __find_all_post_data(self, chrome:webdriver.Chrome):
        def twitter_request_api_filter(log:dict, resp_url:str, resp_body:str):
            try:
                quicktest = resp_url is not None and "twitter.com/i/api/2/search/adaptive.json" in resp_url
                if not quicktest:
                    return False
                actual_body = json.loads(resp_body["body"])
            except:
                return False
            return actual_body.get("globalObjects") is not None \
                and actual_body["globalObjects"].get("tweets") is not None
        
        def twitter_api_data_extractor(resp_body:str):
            ret = set()
            uid_to_username_map = {}

            resp_body = json.loads(resp_body["body"])
            user_edges = resp_body["globalObjects"]["users"]
            for uid, node in user_edges.items():
                try:
                    # uid is the str version of node["id"]
                    uid_to_username_map[node["id"]] = node["screen_name"]
                except:
                    pass
            
            tweet_edges = resp_body["globalObjects"]["tweets"]
            for tid, node in tweet_edges.items():
                try:
                    user_name = uid_to_username_map[node["user_id"]]
                    tweet_id = tid
                    modified_id = user_name + "_" + tweet_id
                    publish_time = node["created_at"]
                    publish_time = date_parser.parse(publish_time)
                    content = node["full_text"]
                    retweets = node["retweet_count"]
                    replies = node["reply_count"]
                    likes = node["favorite_count"]
                    language = node["lang"]
                    data = (modified_id, publish_time, content, retweets, replies, likes, language)
                    ret.add(data)
                except:
                    pass
            return ret

        return network_utils.get_data_from_browser_network(chrome, twitter_request_api_filter, twitter_api_data_extractor)

    def __execute_search(self, language, keyword, browser:webdriver.Chrome=None, browser_pool:BrowserPool=None):
        self.language = language
        query_url = "https://twitter.com/search?q=" + keyword
        browser.get(query_url)
        time.sleep(globals.data_configure.sleep_time)

        try_count = 0
        collected = set()

        last_height = browser.execute_script("return document.body.scrollHeight")
        while True:
            added = 0
            entries = self.__find_all_post_data(browser)
            for entry in entries:
                modified_id, publish_time, content, retweets, replies, likes, language = entry
                self.insert_post(modified_id, publish_time, content, retweets, replies, likes, language)
                if modified_id not in collected:
                    collected.add(modified_id)
                    # print("Adding post: ", modified_id, publish_time, content, retweets, replies, likes)
                    print("Adding post: ", publish_time, retweets, replies, likes)
                    added += 1

            if added == 0:
                if try_count == 3:
                    break
                else:
                    try_count += 1
            else:
                try_count = 0

            browser.execute_script("window.scrollBy(0, document.body.scrollHeight)")
            time.sleep(int(globals.data_configure.sleep_time))
            new_height = browser.execute_script("return document.body.scrollHeight")
            # all post processed
            if last_height == new_height:
                time.sleep(int(globals.data_configure.sleep_time)) # give it a second try
                if last_height == browser.execute_script("return document.body.scrollHeight"):
                    print("Scrolled to the end")
                    break
        return

    # join() is called from outside
    def execute_search(self, language, keyword, browser_pool:BrowserPool=None):
        browser_pool.execute_with_browser(self.__execute_search, language, keyword)
        return
        

    def update_post_info(self, chrome_browser):
        db, cursor = self.db_pool.get_conn_and_cursor()
        find_posts = "select * from post;"
        cursor.execute(find_posts)
        all_entries = cursor.fetchall()
        for one_entry in all_entries:
            elems = one_entry[0].split("_")
            tweet_id = elems[1]
            tweet_url = "https://www.twitter.com/" + elems[0] + "/status/" + elems[1]
            chrome_browser.get(tweet_url)
            time.sleep(int(globals.data_configure.sleep_time / 10))
            html_obj = BeautifulSoup(chrome_browser.page_source, 'lxml')
            all_a_blocks = html_obj.find_all("a", attrs={"dir": "auto", "role": "link"})
            retweets = 0
            quotetweets = 0
            likes = 0
            for one_a_block in all_a_blocks:
                if "href" in one_a_block.attrs and one_a_block.attrs["href"].find(
                        tweet_id + "/retweets/with_comments") >= 0:
                    quotetweets = extract_number(one_a_block.text, "Quote Tweets")
                elif "href" in one_a_block.attrs and one_a_block.attrs["href"].find(tweet_id + "/retweets") >= 0:
                    retweets = extract_number(one_a_block.text, "Retweets")
                elif "href" in one_a_block.attrs and one_a_block.attrs["href"].find(tweet_id + "/likes") >= 0:
                    likes = extract_number(one_a_block.text, "Likes")

            print(retweets, quotetweets, likes)
            self.insert_post(one_entry[0], one_entry[1], one_entry[2], retweets, quotetweets, likes)
            time.sleep(int(globals.data_configure.sleep_time))

    def __execute_without_account_info(self, tweet_name, browser:webdriver.Chrome=None, browser_pool:BrowserPool=None):
        tweet_list = self.get_tweet_list_from_page(tweet_name, browser)
        print("[%d] tweets from [%s]. " % (len(tweet_list), tweet_name))
        for item in tweet_list:
            print(item)
        for one_tweet_info in tweet_list:
            self.get_reply_from_tweet_page(browser, tweet_name, one_tweet_info)
        return

    # join() has to be called outside of this function
    def execute_without_account_info(self, tweet_name, browser_pool:BrowserPool=None):
        browser_pool.execute_with_browser(self.__execute_without_account_info, tweet_name)
        return

    def get_tweet_list_from_page(self, tweet_name, chrome_browser):

        query_url = "https://twitter.com/" + tweet_name
        chrome_browser.get(query_url)
        time.sleep(globals.data_configure.sleep_time)

        ret_list = []
        move_down = 0
        saved_post = 0
        collected = set()

        while saved_post < self.max_tweet_request_one_time and move_down < self.max_tweet_request_move_down:

            html_obj = BeautifulSoup(chrome_browser.page_source, 'lxml')
            all_article_obj = html_obj.find_all("article", attrs={"role": "article", "data-focusable": "true"})

            for one_article in all_article_obj:

                publish_time = ""
                user_name = ""
                tweet_id = ""

                all_a_blocks = one_article.find_all("a", attrs={"role": "link"})
                for one_block in all_a_blocks:
                    time_block = one_block.find("time")
                    if time_block is None:
                        continue

                    publish_time = time_block["datetime"].replace("T", " ")[:19]
                    publish_time = self.transfer_time_to_utc(publish_time)

                    href = one_block.attrs["href"]
                    elems = [item for item in href.split("/") if len(item.strip()) > 0]
                    user_name = elems[0]
                    tweet_id = elems[-1]
                    break

                if publish_time == "" or user_name == "" or tweet_id == "":
                    continue

                content_div_blocks = one_article.find("div", attrs={"lang": "en", "dir": "auto"})
                if content_div_blocks is None:
                    continue
                content = content_div_blocks.text.strip()

                replies = 0
                likes = 0
                retweets = 0
                group_blocks = one_article.find_all("div", attrs={"role": "group"})
                for one_block in group_blocks:
                    if one_block.attrs is not None and "aria-label" in one_block.attrs:
                        attrs_content = one_block.attrs["aria-label"]
                        elems = attrs_content.split(",")
                        for item in elems:
                            if item.find("repl") >= 0:
                                replies = extract_number(item, "replies")
                            elif item.find("lik") >= 0:
                                likes = extract_number(item, "likes")
                            elif item.find("retwe") >= 0:
                                retweets = extract_number(item, "retweets")

                modified_id = user_name + "_" + tweet_id
                self.insert_post(modified_id, publish_time, content, retweets, replies, likes)

                if modified_id not in collected:
                    collected.add(modified_id)
                    ret_list.append((modified_id,
                                     tweet_id,
                                     publish_time,
                                     content))
                    print("Adding post: ", modified_id, publish_time, content, retweets, replies, likes)
                    saved_post += 1

            chrome_browser.execute_script("window.scrollBy(0, 850)")
            move_down += 1
            time.sleep(int(globals.data_configure.sleep_time))

        return ret_list

    def output_content(self):

        start_date_str = globals.data_configure.start_date
        end_date_str = globals.data_configure.end_date

        output_list = []

        post_sql_query = "select * from %s.post where time>='%s 00:00:00' and time<'%s 00:00:00';" % (
            globals.data_configure.tw_db_name, start_date_str, str(end_date_str))
        db, cursor = self.db_pool.get_conn_and_cursor()
        cursor.execute(post_sql_query)
        all_post = cursor.fetchall()
        for one_post in all_post:
            post_info = one_post[0]
            elems = post_info.split("_")
            entry_dict = {"type": "review",
                          "id": one_post[0],
                          "user_id": elems[0],
                          "user_name": elems[0],
                          "text": one_post[2],
                          "time": str(one_post[1]),
                          "retweets": int(one_post[3]),
                          "quotetweets": int(one_post[4]),
                          "likes": int(one_post[5]),
                          "source": "twitter",
                          "url": "https://www.twitter.com/" + elems[0] + "/status/" + elems[1],
                          }
            output_list.append(entry_dict)

        comment_sql_query = "select * from %s.reply where time>='%s 00:00:00' and time<'%s 00:00:00';" % (
            globals.data_configure.tw_db_name, start_date_str, end_date_str)
        cursor.execute(comment_sql_query)
        all_comment = cursor.fetchall()
        for one_comment in all_comment:
            # print(one_comment)
            comment_id = str(one_comment[0]) + "_" + one_comment[1]
            elems = one_comment[-2].split("_")
            entry_dict = {
                "type": "comment",
                "id": comment_id,
                "user_id": one_comment[1],
                "user_name": one_comment[1],
                "text": one_comment[2],
                "time": str(one_comment[0]),
                "post_id": one_comment[-1],
                "source": "twitter",
                "url": "https://www.twitter.com/" + elems[0] + "/status/" + elems[1],
            }
            output_list.append(entry_dict)

        output_filename = globals.data_configure.output_directory + "/twitter_" + start_date_str + "_" + end_date_str + ".txt"
        f = codecs.open(output_filename, 'w', 'utf-8')
        for one_line in output_list:
            f.write(json.dumps(one_line) + "\n")
        f.close()

        return output_filename