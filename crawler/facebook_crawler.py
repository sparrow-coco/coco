import codecs
import globals
import json
import pymysql
import re
import time
import traceback
import sys

from bs4 import BeautifulSoup
from selenium import webdriver
from xdata_parser.facebook_parser import FacebookParser
from threadings.persistent_db import DBPool


class FacebookCrawler(object):

    def __init__(self):
        self.parser = FacebookParser()
        self.max_post_number = 10

        self.db_pool = DBPool(database=globals.data_configure.fb_db_name)

        self.create_post_table()
        self.create_comment_table()
        self.create_reply_table()
        return

    def create_post_table(self):
        db, cursor = self.db_pool.get_conn_and_cursor()
        sql_create_table = "CREATE TABLE IF NOT EXISTS " + "post" + \
                           "(`id` varchar(100) NOT NULL," \
                           "`time` datetime DEFAULT NULL," \
                           "`content` text," \
                           "PRIMARY KEY (`id`)" \
                           "    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
        cursor.execute(sql_create_table)
        db.commit()

    def insert_post(self, post_id, publish_time, content):
        try:
            db, cursor = self.db_pool.get_conn_and_cursor()
            sql_str = "insert into post" + \
                      " (id ,time, content)" \
                      " values (%s, %s, %s) ON DUPLICATE KEY UPDATE id=values(id);"
            cursor.execute(sql_str,
                                (post_id,
                                 publish_time,
                                 content))
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
                           "PRIMARY KEY (`time`, `username_content_md5`)" \
                           " ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
        cursor.execute(sql_create_table)
        db.commit()

    def insert_comment(self,
                       publish_time,
                       username_content_md5,
                       user_name,
                       content,
                       post_id):
        try:
            db, cursor = self.db_pool.get_conn_and_cursor()
            sql_str = "insert into comment" + \
                      " (time ,username_content_md5, content, user_name, post_id)" \
                      " values (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE content=values(content);"
            cursor.execute(sql_str,
                                (publish_time,
                                 username_content_md5,
                                 content,
                                 user_name,
                                 post_id))
            db.commit()
            #print("Successfully Insert A Comment: ",
            #      publish_time, username_content_md5, user_name, content, post_id)
            print("Successfully Insert A Comment: ",
                  publish_time, username_content_md5, post_id)
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

    def execute(self, post_main_page, page_name, browser):

        browser.get(post_main_page)
        self.parser.close_windows(browser)

        post_url_list = []
        while True:
            finds = [m.start() for m in re.finditer('You must log in to continue', browser.page_source)]
            if len(finds) == 2:
                browser.get(post_main_page)
                self.parser.close_windows(browser)

            post_block_list = self.parser.get_post_block_list_from_main_page(browser)
            if len(post_block_list) == 0:
                break
            post_url_list = self.get_post_url_list(post_block_list)
            if len(post_url_list) >= self.max_post_number:
                break
            self._expand_see_more(browser)

        print("There are %d post to process!" % len(post_url_list))
        for index, one_post_url in enumerate(post_url_list):
            exe_url = one_post_url
            if one_post_url.find("photos") >= 0:
                elems = one_post_url.split("/")
                if elems[-2].isdigit():
                    exe_url = "https://www.facebook.com/leagueoflegends/posts/" + elems[-2]
                    post_id = elems[-2]
                else:
                    continue
            else:
                elems = one_post_url.split("/")
                post_id = elems[-1]

            print("Start Processing %d post: %s" % (index, exe_url))
            self.process_post_page(page_name, post_id, exe_url, browser)

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

    def get_post_url_list(self, post_block_list):

        post_url_list = []
        for one_post_block in post_block_list:
            post_url = self.parser.get_post_url_from_main_page(one_post_block)
            if len(post_url) > 0:
                post_url_list.append(post_url)

        return post_url_list

    def process_post_page(self, page_name, post_id, post_url, browser):

        try_count = 0
        success = False
        while try_count < 10:
            browser.get(post_url)
            try:
                post_publish_time = self.parser.get_publish_time_from_post_page(browser)
                success = True
                break
            except Exception as e:
                print(e)
                try_count += 1
                continue

        if success:
            post_content = self.parser.get_content_from_post_page(browser)
            self.insert_post(post_url.replace("https://www.facebook.com/", ""), post_publish_time, post_content)
            self.parser.process_comments_on_post_page(browser, post_url.replace("https://www.facebook.com/", ""), self)
        else:
            sys.stderr.write("Fail to process post page: %s\n"%post_url)

    def output_content(self):

        start_date_str = globals.data_configure.start_date
        end_date_str = globals.data_configure.end_date

        output_list = []

        post_id = set()
        post_sql_query = "select * from %s.post where time>='%s 00:00:00' and time<'%s 00:00:00';" % (
            globals.data_configure.fb_db_name, start_date_str, end_date_str)
        db, cursor = self.db_pool.get_conn_and_cursor()
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

        reply_sql_query = "select * from %s.reply where time>='%s 00:00:00' and time<'%s 00:00:00';" % (
            globals.data_configure.fb_db_name, start_date_str, end_date_str)
        cursor.execute(reply_sql_query)
        all_reply = cursor.fetchall()
        for one_reply in all_reply:
            reply_id = str(one_reply[0]) + "_" + one_reply[1]
            comment_id = str(one_reply[-2]) + "_" + one_reply[-1]
            entry_dict = {
                "type":"comment",
                "id": reply_id,
                "user_id": one_reply[3],
                "user_name": one_reply[3],
                "text": one_reply[2],
                "time": str(one_reply[0]),
                "post_id": comment_id,
                "source": "facebook",
            }
            output_list.append(entry_dict)

        output_filename = globals.data_configure.output_directory + "/facebook_" + start_date_str + "_" + end_date_str + ".txt"
        f = codecs.open(output_filename, 'w', 'utf-8')
        for one_line in output_list:
            f.write(json.dumps(one_line) + "\n")
        f.close()

        return output_filename

    sql_create_table = "CREATE TABLE IF NOT EXISTS " + "reply" + \
                       "(`time` datetime NOT NULL," \
                       "`username_content_md5` varchar(50) NOT NULL," \
                       "`content` text," \
                       "`user_name` varchar(50) NULL," \
                       "`comment_time` datetime NOT NULL," \
                       "`comment_username_content_md5` varchar(50) NOT NULL," \
                       "PRIMARY KEY (`time`, `username_content_md5`)" \
                       " ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"