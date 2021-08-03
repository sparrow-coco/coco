import codecs

from selenium import webdriver
import globals
import json
import time
import traceback

from bs4 import BeautifulSoup
from xdata_parser.facebook_login_parser import FacebookParser
from typing import Callable
from threadings.browser_pool import BrowserPool
from threadings.persistent_db import DBPool
from utils import network_utils

POST_TOO_OLD=100

class FacebookCrawler(object):

    def __init__(self):
        self.parser = FacebookParser()
        self.max_post_number = 10
        self.language = "en"
        self.max_scroll_number_from_search_page = 30
        self.max_post_collected_number_from_search_page = 50

        self.db_pool = DBPool(database=globals.data_configure.fb_db_name) # this should create connection on a per-Thread basis

        self.create_post_table()
        self.create_comment_table()
        self.create_reply_table()
        return

    def create_post_table(self):

        sql_create_table = "CREATE TABLE IF NOT EXISTS " + "post" + \
                           "(`id` varchar(100) NOT NULL," \
                           "`time` datetime DEFAULT NULL," \
                           "`content` text," \
                           "`lang` varchar(10) NULL," \
                           "PRIMARY KEY (`id`)" \
                           "    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
        db, cursor = self.db_pool.get_conn_and_cursor()
        cursor.execute(sql_create_table)
        db.commit()

    def insert_post(self, post_id, publish_time, content, lang="en"):
        try:
            db, cursor = self.db_pool.get_conn_and_cursor()
            sql_str = "insert into post" + \
                      " (id ,time, content, lang)" \
                      " values (%s, %s, %s, %s) ON DUPLICATE KEY UPDATE id=values(id);"
            cursor.execute(sql_str,
                                (post_id,
                                 publish_time,
                                 content,
                                 lang))
            db.commit()
            #print("Successfully Insert A Post: ", post_id, publish_time, content)
            print("Successfully Insert A Post: ", post_id, publish_time)
            return True
        except Exception as e:

            db.rollback()
            traceback.print_exc()
            print(u"post insert error!\n%s", e)
            return False

    def create_comment_table(self):
        db, cursor = self.db_pool.get_conn_and_cursor()
        sql_create_table = "CREATE TABLE IF NOT EXISTS comment" + \
                           "(`time` datetime NOT NULL," \
                           "`username_content_md5` varchar(50) NOT NULL," \
                           "`content` text," \
                           "`user_name` varchar(50) NULL," \
                           "`post_id` varchar(50) NULL," \
                           "`lang` varchar(10) NULL," \
                           "PRIMARY KEY (`time`, `username_content_md5`)" \
                           " ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
        cursor.execute(sql_create_table)
        db.commit()

    def insert_comment(self,
                       publish_time,
                       username_content_md5,
                       user_name,
                       content,
                       post_id,
                       lang="en"):
        try:
            db, cursor = self.db_pool.get_conn_and_cursor()
            sql_str = "insert into comment" + \
                      " (time ,username_content_md5, content, user_name, post_id, lang)" \
                      " values (%s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE content=values(content);"
            cursor.execute(sql_str,
                                (publish_time,
                                 username_content_md5,
                                 content,
                                 user_name,
                                 post_id,
                                 lang))
            db.commit()
            #print("Successfully Insert A Comment: ",
            #      publish_time, username_content_md5, user_name, content, post_id)
            print("Successfully Insert A Comment: ",
                  publish_time, username_content_md5)
            return True
        except Exception as e:

            db.rollback()
            traceback.print_exc()
            print(u"comment insert error!\n%s", e)
            return False

    def find_comment(self,
                     publish_time,
                     username_content_md5,
                     user_name,
                     content,
                     post_id):
        try:
            db, cursor = self.db_pool.get_conn_and_cursor()
            sql_str = "select count(*) from comment where" + \
                      " time=%s and username_content_md5=%s and user_name=%s and post_id=%s;"
            cursor.execute(sql_str,
                                (publish_time,
                                 username_content_md5,
                                 user_name,
                                 post_id))
            count = cursor.fetchone()[0]
            if count >= 1:
                return True
            else:
                return False
        except Exception as e:

            db.rollback()
            traceback.print_exc()
            print(u"find comment error!\n%s", e)
            return False

    def create_reply_table(self):
        db, cursor = self.db_pool.get_conn_and_cursor()
        sql_create_table = "CREATE TABLE IF NOT EXISTS " + "reply" + \
                           "(`time` datetime NOT NULL," \
                           "`username_content_md5` varchar(50) NOT NULL," \
                           "`content` text," \
                           "`user_name` varchar(50) NULL," \
                           "`comment_time` datetime NOT NULL," \
                           "`comment_username_content_md5` varchar(50) NOT NULL," \
                           "PRIMARY KEY (`time`, `username_content_md5`)" \
                           " ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
        cursor.execute(sql_create_table)
        db.commit()

    def insert_reply(self,
                     publish_time,
                     username_content_md5,
                     user_name,
                     content,
                     comment_time,
                     comment_username_content_md5):
        try:
            db, cursor = self.db_pool.get_conn_and_cursor()
            sql_str = "insert into reply" + \
                      " (time ,username_content_md5, content, user_name, comment_time, comment_username_content_md5)" \
                      " values (%s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE content=values(content);"
            cursor.execute(sql_str,
                                (publish_time,
                                 username_content_md5,
                                 content,
                                 user_name,
                                 comment_time,
                                 comment_username_content_md5))
            db.commit()
            #print("Successfully Insert A Reply: ", publish_time, username_content_md5,
            #      user_name, content, comment_time, comment_username_content_md5)
            print("Successfully Insert A Reply: ", publish_time, username_content_md5,
                comment_time, comment_username_content_md5)
            return True
        except Exception as e:

            db.rollback()
            traceback.print_exc()
            print(u"reply insert error!\n%s", e)
            return False

    def execute(self, post_main_page, page_name, browser_pool:BrowserPool, start_batch_size=10):
        browser_pool.execute_with_browser(self.__execute, post_main_page, page_name, start_batch_size, nohang=False)
        browser_pool.join()
        return

    """
    BrowserPool will assign a browser
    [2021-06-19] Facebook works like this:
        1. the main page that is loaded at start up has data preloaded into the html.
           Therefore, at this stage, we need to use BeautifulSoup for extraction
        2. when you scroll down, more data is needed. This time Facebook HAS TO make a request. Facebook
           does this by requesting /api/graphql path, and data is inside the JSON which is inside the response
           Therefore, at this stage, we can use json_data_extraction for optimization
    """
    def __execute(self, post_main_page, page_name, start_batch_size, browser:webdriver.Chrome=None, browser_pool:BrowserPool=None):
        post_url_list = []
        url_processed = 0
        attempt_url = []
        need_soup = True

        browser.get(post_main_page)
        self.parser.close_windows(browser)
        time.sleep(globals.data_configure.sleep_time)

        # used when switched to request only mode for data extraction
        def facebook_post_api_filter(log:dict, resp_url, resp_body):
            try:
                quicktest = resp_url is not None and "www.facebook.com/api/graphql/" in resp_url
                if not quicktest:
                    return False
                test = resp_body["body"].split("\n") # safer so that linux platform also works. If windows, use \r\n
                # facebook sometimes returns TWO lines (i.e. two Json objects). We only need the first line.
                actual_body = json.loads(resp_body["body"].split("\n")[0]) # safer so that linux platform also works
            except:
                # traceback.print_exc()
                return False
            return (actual_body["data"].get("node") is not None
                and actual_body["data"]["node"].get("timeline_feed_units") is not None
            )

        def facebook_api_data_extractor(resp_body):
            ret = set()
            resp_body = json.loads(resp_body["body"].split("\n")[0])
            edges = resp_body["data"]["node"]["timeline_feed_units"]["edges"]
            for node in edges:
                try:
                    url = node["node"]["comet_sections"]["feedback"]["story"]["url"]
                    ret.add(url)
                except:
                    pass
            return ret
        
        # curr height
        # it would be faster to scroll to the bottom first and process all
        last_height = browser.execute_script("return document.body.scrollHeight")
        while True:
            if need_soup:
                post_blocks = self.parser.get_post_block_list_from_main_page(browser)
                if post_blocks is None:
                    break
                for post_block in post_blocks:
                    post_url_list += self.parser.get_post_url_from_post_block_list(post_block, page_name, browser)
                attempt_url += network_utils.get_data_from_browser_network(browser, facebook_post_api_filter, facebook_api_data_extractor)
                # if no overlap, continue using soup.
                # otherwise, switch to resp json mode for extraction
                all_urls = set(post_url_list) | set(attempt_url)
                need_soup = len(set(post_url_list) & set(attempt_url)) == 0 or len(all_urls) == len(post_url_list)
                post_url_list = list(all_urls)[url_processed:]
            else:
                # use the request from network_utils for performance
                post_url_list += network_utils.get_data_from_browser_network(browser, facebook_post_api_filter, facebook_api_data_extractor)
            
            if len(post_url_list) >= start_batch_size:
                batch = post_url_list.copy()
                browser_pool.execute_with_browser(self.process_url_lists, batch, page_name)
                url_processed += len(post_url_list) #adjust if using beautiful soup mode above
                post_url_list = [] # reset
            # browse more data
            browser.execute_script("window.scrollBy(0, document.body.scrollHeight)")
            time.sleep(int(globals.data_configure.sleep_time))
            new_height = browser.execute_script("return document.body.scrollHeight")
            # all post processed
            if last_height == new_height:
                time.sleep(int(globals.data_configure.sleep_time)) # give it a second try
                if last_height == browser.execute_script("return document.body.scrollHeight"):
                    print("Scrolled to the end")
                    break
        # done
        return 0
        
    def process_url_lists(self, post_url_list, page_name, browser:webdriver.Chrome=None, browser_pool:BrowserPool=None):
        print("There are %d post to process!" % len(post_url_list))
        for index, one_post_url in enumerate(post_url_list):
            exe_url = one_post_url
            elems = exe_url.split("/")
            post_id = elems[-1]
            print("Start Processing %d post: %s" % (index, exe_url))
            self.process_post_page(page_name, post_id, exe_url, browser)
        # done
        return 0

    def _expand_see_more(self, chrome_browser):

        html_obj = BeautifulSoup(chrome_browser.page_source, 'lxml')
        block1 = html_obj.find("a", attrs={"class": "uiMorePagerPrimary", "role": "button"})
        if block1 is not None and block1.text.strip() == "See More":
            try:
                time.sleep(globals.data_configure.sleep_time)
                chrome_browser.find_element_by_class_name("uiMorePagerPrimary").click()
            except Exception as e:
                print(e)
                chrome_browser.execute_script("window.scrollBy(0, 100)")

        self.parser.close_windows(chrome_browser)

    def process_post_page(self, page_name, post_id, post_url, browser):
        browser.get(post_url)
        time.sleep(globals.data_configure.sleep_time)

        post_publish_time = self.parser.get_publish_time_from_post_page(browser, page_name, post_id)
        if post_publish_time == "":
            print("Do not find publish time for post %s" % post_id)
            return

        post_content = self.parser.get_content_from_post_page(browser)
        self.insert_post(post_url.replace("https://www.facebook.com/", ""), post_publish_time, post_content, self.language)
        self.parser.process_comments_on_post_page(browser, post_url.replace("https://www.facebook.com/", ""), self)

    def process_post_pages(self, posts, browser:webdriver.Chrome=None, browser_pool:BrowserPool=None):
        print("There are %d post to process!" % len(posts))
        for index, one_post_url in enumerate(posts):
            exe_url = one_post_url
            if exe_url[-1] == "/":
                exe_url = exe_url[:-1]
            elems = exe_url.split("/")
            post_id = elems[-1]
            page_name = elems[-3]
            print("Start Processing %d post: %s" % (index, exe_url))
            self.process_post_page(page_name, post_id, exe_url, browser)
        # done
        return

    def __execute_search(self, language, keyword, start_batch_size, browser:webdriver.Chrome=None, browser_pool:BrowserPool=None):
        self.language = language
        query_url = "https://www.facebook.com/search/posts/?q=" + keyword
        browser.get(query_url)
        time.sleep(globals.data_configure.sleep_time)

        move_down = 0
        try_count = 0
        url_processed = 0
        collected_post = []
        attempt_url = []
        need_soup = True

        def facebook_post_api_filter(log:dict, resp_url, resp_body):
            try:
                quicktest = resp_url is not None and "www.facebook.com/api/graphql/" in resp_url
                if not quicktest:
                    return False
                # facebook sometimes returns TWO lines (i.e. two Json objects). We only need the first line.
                actual_body = json.loads(resp_body["body"].split("\n")[0]) # safer so that linux platform also works
            except:
                # traceback.print_exc()
                return False
            return (actual_body["data"].get("serpResponse") is not None
                and actual_body["data"]["serpResponse"].get("results") is not None
            )
        
        def facebook_api_data_extractor(resp_body):
            ret = set()
            resp_body = json.loads(resp_body["body"].split("\n")[0])
            edges = resp_body["data"]["serpResponse"]["results"]["edges"]
            for node in edges:
                try:
                    url = node["relay_rendering_strategy"]["view_model"]["click_model"]["permalink"]
                    ret.add(url)
                except:
                    pass
            return ret

        last_height = browser.execute_script("return document.body.scrollHeight")
        while move_down < self.max_scroll_number_from_search_page and \
                 len(collected_post) < self.max_post_collected_number_from_search_page:
            
            added = 0
            if need_soup:
                html_obj = BeautifulSoup(browser.page_source, 'lxml')
                all_possible_link_obj = html_obj.find_all("a", attrs={"role": "link", "tabindex": "0"})

                for one_link_obj in all_possible_link_obj:

                    if "href" not in one_link_obj.attrs:
                        continue

                    href_url = one_link_obj.attrs["href"]
                    if href_url.find("/www.facebook.com/") >= 0 and href_url.find("/posts/") >= 0:
                        if href_url not in collected_post:
                            collected_post.append(href_url)
                            added += 1
                # decide when to switch to "soupless" mode
                attempt_url += network_utils.get_data_from_browser_network(browser, facebook_post_api_filter, facebook_api_data_extractor)
                # if no overlap, continue using soup.
                # otherwise, switch to resp json mode for extraction
                all_urls = set(collected_post) | set(attempt_url)
                need_soup = len(set(collected_post) & set(attempt_url)) == 0 or len(all_urls) == len(collected_post)
                collected_post = list(all_urls)[url_processed:]
            else:
                collected_post += network_utils.get_data_from_browser_network(browser, facebook_post_api_filter, facebook_api_data_extractor)

            if added == 0:
                if try_count == 3:
                    break
                else:
                    try_count += 1
            else:
                try_count = 0
            
            if len(collected_post) >= start_batch_size:
                batch = collected_post.copy()
                browser_pool.execute_with_browser(self.process_post_pages, batch)
                url_processed += len(collected_post) #adjust if using beautiful soup mode above
                collected_post = [] # reset
            
            # browse more data
            browser.execute_script("window.scrollBy(0, document.body.scrollHeight)")
            time.sleep(int(globals.data_configure.sleep_time))
            new_height = browser.execute_script("return document.body.scrollHeight")
            # all post processed
            if last_height == new_height:
                time.sleep(int(globals.data_configure.sleep_time)) # give it a second try
                if last_height == browser.execute_script("return document.body.scrollHeight"):
                    print("Scrolled to the end")
                    break
        # done
        return

    def execute_search(self, language, keyword, browser_pool:BrowserPool, start_batch_size=10):
        browser_pool.execute_with_browser(self.__execute_search, language, keyword, start_batch_size)
        return

    def output_content(self):

        start_date_str = globals.data_configure.start_date
        end_date_str = globals.data_configure.end_date

        output_list = []

        post_id = set()
        db, cursor = self.db_pool.get_conn_and_cursor()
        post_sql_query = "select * from %s.post where time>='%s 00:00:00' and time<'%s 00:00:00';" % (
            globals.data_configure.fb_db_name, start_date_str, end_date_str)
        cursor.execute(post_sql_query)
        all_post = cursor.fetchall()
        for one_post in all_post:
            post_info = one_post[0]
            post_id.add(post_info)
            elems = post_info.split("/")
            entry_dict = {"type": "review",
                          "id": elems[-1],
                          "user_id": elems[0],
                          "user_name": elems[0],
                          "text":one_post[-1],
                          "time": str(one_post[1]),
                          "source":"facebook",
                          "url":"https://www.facebook.com/" + post_info,
                          }
            output_list.append(entry_dict)

        comment2post = {}
        comment_sql_query = "select * from %s.comment where time>='%s 00:00:00' and time<'%s 00:00:00';" % (
            globals.data_configure.fb_db_name, start_date_str, end_date_str)
        cursor.execute(comment_sql_query)
        all_comment = cursor.fetchall()
        for one_comment in all_comment:
            comment_id = str(one_comment[0]) + "_" + one_comment[1]
            comment2post[comment_id] = one_comment[-1]
            entry_dict = {
                "type":"comment",
                "id": comment_id,
                "user_id": one_comment[3],
                "user_name": one_comment[3],
                "text": one_comment[2],
                "time": str(one_comment[0]),
                "post_id": one_comment[-1],
                "source": "facebook",
                "url": "https://www.facebook.com/" + one_comment[-1],
            }
            output_list.append(entry_dict)

        output_filename = globals.data_configure.output_directory + "/facebook_" + start_date_str + "_" + end_date_str + ".txt"
        f = codecs.open(output_filename, 'w', 'utf-8')
        for one_line in output_list:
            f.write(json.dumps(one_line) + "\n")
        f.close()

        return

    def login_facebook(self, browser):

        browser.get("https://www.facebook.com/")
        time.sleep(globals.data_configure.sleep_time / 10)

        #context = browser.find_element_by_css_selector("#email")
        context = browser.find_element_by_name("email")
        context.send_keys(globals.data_configure.fb_account)
        time.sleep(1)

        context = browser.find_element_by_css_selector("#pass")
        context.send_keys(globals.data_configure.fb_pwd)
        time.sleep(1)

        #context = browser.find_element_by_xpath('//button[text()="Log In"]')
        context = browser.find_element_by_name("login")
        context.click()

        time.sleep(globals.data_configure.sleep_time / 10)
