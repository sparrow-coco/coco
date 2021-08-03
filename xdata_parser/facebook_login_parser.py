import datetime
from typing import Callable

from selenium import webdriver
import globals
import hashlib
import json
import time
import sys

from bs4 import BeautifulSoup
from selenium.webdriver.common.action_chains import ActionChains
from utils.func import xpath_soup
from utils import network_utils


class FacebookParser(object):

    def __init__(self):
        self.max_comment_click_try = 3
        self.max_try_on_view_more_comment = 5
        return

    def transfer_time_format(self, publish_time):

        try:
            elems = [item.strip() for item in publish_time.replace("at", ",").replace("AM", ",AM").replace("PM", ",PM").split(",")
                     if len(item.strip()) > 0]
            transferred_time = \
                str(datetime.datetime.strptime(elems[-3].strip() + " " + elems[-4].strip() + " " + elems[-2].strip() + " " + elems[-1],
                                               '%Y %B %d %I:%M %p'))

            transferred_time_obj = datetime.datetime.strptime(transferred_time, "%Y-%m-%d %H:%M:%S")
            utc_time_obj = (transferred_time_obj + datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S")

            return str(utc_time_obj).strip()
        except Exception as e:
            print(e)
            print("error in transfer_time_format [%s]"%publish_time)
            return ""

    def close_windows(self, chrome_browser):

        html_obj = BeautifulSoup(chrome_browser.page_source, 'lxml')
        block1 = html_obj.find("a", attrs={"class": "autofocus", "action": "cancel", "role": "button"})
        if block1 is not None and block1.text.strip() == "Close":
            try:
                chrome_browser.find_element_by_class_name("autofocus").click()
            except Exception as e:
                print(e)

        html_obj = BeautifulSoup(chrome_browser.page_source, 'lxml')
        block2 = html_obj.find("a", attrs={"id":"expanding_cta_close_button", "role":"button"})
        if block2 is not None and block2.text.strip() == "Not Now":
            try:
                chrome_browser.find_element_by_id("expanding_cta_close_button").click()
            except Exception as e:
                print(e)

        html_obj = BeautifulSoup(chrome_browser.page_source, 'lxml')
        block3 = html_obj.find("label", attrs={"id": "uiButton"})
        if block3 is not None:
            input_block = block3.find("input", attrs={"name": "cancel"})
            xpath = xpath_soup(input_block)
            chrome_block = chrome_browser.find_element_by_xpath(xpath)
            try:
                chrome_block.click()
            except Exception as e:
                print(e)

    def get_post_block_list_from_main_page(self, chrome_browser):

        html_obj = BeautifulSoup(chrome_browser.page_source, 'lxml')
        post_blocks = html_obj.find_all("div", attrs={"role": "feed"})
        if len(post_blocks) == 0:
            return html_obj

        #return post_blocks[-1] #this would have skipped the Pinned Posts
        return post_blocks

    def get_post_url_from_post_block_list(self, post_block_list, page_name, browser):

        post_and_time_urls = set()
        post_blocks = post_block_list.find_all("div", attrs={"role": "article"})
        for one_post_block in post_blocks:
            all_a_block = one_post_block.find_all("a", attrs={"role": "link"})
            for one_a_block in all_a_block:
                if "tabindex" in one_a_block.attrs and one_a_block.attrs["tabindex"] == "0" and "href" in one_a_block.attrs:

                    if one_a_block.attrs["href"].lower().startswith("https://www.facebook.com/" + page_name.lower() + "/posts/"):
                        url_str = one_a_block.attrs["href"]
                    elif one_a_block.attrs["href"].lower().startswith("https://www.facebook.com/" + page_name.lower() + "/photos/"):
                        url_str = one_a_block.attrs["href"]
                        pos1 = url_str.find("?")
                        url_str = url_str[:pos1]
                        if url_str[-1] == "/":
                            url_str = url_str[:-1]
                        elems = url_str.split("/")
                        url_str = "https://www.facebook.com/" + page_name.lower() + "/posts/" + elems[-1]
                    else:
                        continue

                    pos1 = url_str.find("?")
                    if pos1 > 0:
                        post_and_time_urls.add(url_str[:pos1])
                    else:
                        post_and_time_urls.add(url_str)

        return list(post_and_time_urls)

    def get_publish_time_from_post_page(self, chrome_browser, page_name, post_id):

        html_obj = BeautifulSoup(chrome_browser.page_source, 'lxml')
        post_block = html_obj.find("div", attrs={"role": "article"})

        publish_time = ""
        all_a_block = post_block.find_all("a", attrs={"role": "link"})
        for one_a_block in all_a_block:
            if "tabindex" in one_a_block.attrs and one_a_block.attrs["tabindex"] == "0" and \
                    "href" in one_a_block.attrs and one_a_block.attrs["href"].lower().startswith(
                "https://www.facebook.com/" + page_name.lower() + "/posts/" + post_id):

                xpath = xpath_soup(one_a_block)
                chrome_block = chrome_browser.find_element_by_xpath(xpath)
                ActionChains(chrome_browser).move_to_element(chrome_block).perform()
                time.sleep(int(globals.data_configure.sleep_time))
                dark_modes = chrome_browser.find_elements_by_class_name("__fb-dark-mode")
                publish_time = ""
                if len(dark_modes) == 1:
                    publish_time = dark_modes[0].text.strip()
                    if publish_time != "":
                        publish_time = self.transfer_time_format(publish_time)
                        break

        return publish_time

    def get_content_from_post_page(self, chrome_browser):

        html_obj = BeautifulSoup(chrome_browser.page_source, 'lxml')
        post_block = html_obj.find("div", attrs={"role": "article"})

        post_content = ""
        all_div_blocks = post_block.find_all("div")
        for one_div_block in all_div_blocks:
            if "data-ad-comet-preview" in one_div_block.attrs and one_div_block.attrs["data-ad-comet-preview"] == "message":
                post_content = one_div_block.text.strip()
                break

        return post_content

    def process_comments_on_post_page(self, chrome_browser, post_id, crawler):

        # click view more comments until the end
        try_count = 0
        while try_count <= 10:

            html_obj = BeautifulSoup(chrome_browser.page_source, 'lxml')
            post_block = html_obj.find("div", attrs={"role": "article"})
            possible_view_more_comments_block = post_block.find_all("div", attrs={"role": "button"})

            view_more_comments_block = None
            for one_block in possible_view_more_comments_block:
                if one_block.text.strip() == "View more comments":
                    view_more_comments_block = one_block
                    break
            
            # no view more comments link
            if view_more_comments_block is None:
                print("Did not find <view more comments> block under post block [%s]"%post_id)
                break

            try:
                soup_path = xpath_soup(view_more_comments_block)
                chrome_block = chrome_browser.find_element_by_xpath(soup_path)
                chrome_block.click()
                time.sleep(globals.data_configure.sleep_time)
                continue
            except Exception as e:
                print(e)
                log_str = "Error in clicking [View more comments] url"
                print(log_str)
                sys.stderr.write(log_str + "\n")
                self.close_windows(chrome_browser)
                try_count += 1

        added_comments, exist_comments = self.process_current_comment(chrome_browser, post_id, crawler)
        print("[%d] more comments are added and [%s] comments are already in db" % (added_comments, exist_comments))

        return

    def __get_comments_from_api(self, chrome:webdriver.Chrome):
        def facebook_comment_api_filter(log:dict, resp_url, resp_body):
            try:
                quicktest = resp_url is not None and "www.facebook.com/api/graphql/" in resp_url
                if not quicktest:
                    return False
                # facebook sometimes returns TWO lines (i.e. two Json objects). We only need the first line.
                actual_body = json.loads(resp_body["body"].split("\n")[0]) # safer so that linux platform also works
            except:
                # traceback.print_exc()
                return False
            return (actual_body["data"].get("feedback") is not None
                and actual_body["data"]["feedback"].get("display_comments") is not None
            )

        found_md5 = set()
        def facebook_comment_api_data_extractor(resp_body):
            ret = set()
            base_url = "https://www.facebook.com/"
            resp_body = json.loads(resp_body["body"].split("\n")[0])
            edges = resp_body["data"]["feedback"]["display_comments"]["edges"]
            for node in edges:
                try:
                    comment_publish_time = node["node"]["created_time"]
                    comment_publish_time = datetime.datetime.fromtimestamp(int(comment_publish_time))
                    user_url = node["node"]["author"]["url"]
                    # process username
                    comment_user_raw_name = node["node"]["author"]["name"]
                    comment_user_name = FacebookParser.get_user_name_from_user_url(user_url)
                    if comment_user_name == "":
                        start = user_url.find(base_url)
                        comment_user_name = user_url[start+len(base_url):] if start >= 0 else comment_user_raw_name
                    comment_content = node["node"]["preferred_body"]["text"]
                    comment_username_content_md5 = hashlib.md5((comment_user_name + " " + comment_content).encode()).hexdigest()
                    # add data
                    data = (comment_publish_time, comment_username_content_md5, comment_user_name, comment_content)
                    ret.add(data)
                    found_md5.add(comment_username_content_md5)
                except:
                    pass
            return ret
        return found_md5, network_utils.get_data_from_browser_network(chrome, facebook_comment_api_filter, facebook_comment_api_data_extractor)

    def __get_comment_blocks_from_html(self, chrome:webdriver.Chrome):
        html_obj = BeautifulSoup(chrome.page_source, 'lxml')
        post_block = html_obj.find("div", attrs={"role": "article"})
        all_ul_blocks = post_block.find_all("ul")

        comments_ul = None
        for one_ul_block in all_ul_blocks:
            ul_text = one_ul_block.text.replace("·", "").replace(" ", "")
            if ul_text == "" or (len(one_ul_block) <= 5 and len(ul_text) < 20 and ul_text.startswith("LikeReply")):
                continue
            comments_ul = one_ul_block
            break
        return comments_ul

    def __get_comments_from_comment_ul(self, chrome_browser:webdriver.Chrome, comments_ul, found_md5):
        ret = set()
        for one_comment in comments_ul.contents:
            user_url = ""
            comment_publish_time = ""
            comment_user_raw_name = ""
            all_a_blocks = one_comment.find_all("a", attrs={"role": "link"})
            for one_a_block in all_a_blocks:
                if user_url != "" and comment_publish_time != "":
                    break

                if "href" in one_a_block.attrs and "tabindex" in one_a_block.attrs and one_a_block.attrs["tabindex"] == '0':
                    url = one_a_block.attrs['href']
                    if url.find("/posts/") >= 0:
                        try:
                            xpath = xpath_soup(one_a_block)
                            chrome_block = chrome_browser.find_element_by_xpath(xpath)
                            ActionChains(chrome_browser).move_to_element(chrome_block).perform()
                            time.sleep(int(globals.data_configure.sleep_time))
                            dark_modes = chrome_browser.find_elements_by_class_name("__fb-dark-mode")
                            if len(dark_modes) == 1:
                                comment_publish_time = dark_modes[0].text.strip()
                                comment_publish_time = self.transfer_time_format(comment_publish_time)
                        except Exception as e:
                            comment_publish_time = self.get_abs_time_from_rel_time(one_a_block.text.strip())
                    elif url.find("?comment_id") >= 0 or url.find("profile.php?id") >= 0:
                        user_url = url
                        comment_user_raw_name = one_a_block.text.strip()

            if comment_publish_time == "":
                continue

            comment_user_name = self.get_user_name_from_user_url(user_url)
            if comment_user_name == "":
                comment_user_name = comment_user_raw_name
            if comment_user_name == "":
                continue

            comment_content = ""
            all_div_blocks = one_comment.find_all("div", attrs={"dir": "auto"})
            for one_div_block in all_div_blocks:
                if "style" in one_div_block.attrs and one_div_block.attrs['style'] == "text-align: start;":
                    comment_content = one_div_block.text.strip()
                    break

            comment_username_content_md5 = hashlib.md5((comment_user_name + " " + comment_content).encode()).hexdigest()
            if comment_username_content_md5 in found_md5:
                print("Overlapping content from API. No need to process from html")
                break
            data = (comment_publish_time, comment_username_content_md5, comment_user_name, comment_content)
            ret.add(data)
        return list(ret)

    def __insert_new_comments(self, crawler, data_entries, post_id):
        added_comments, exist_comments = 0, 0
        for entry in data_entries:
            comment_publish_time, comment_username_content_md5, comment_user_name, comment_content = entry
            if crawler.find_comment(comment_publish_time,
                                        comment_username_content_md5,
                                        comment_user_name, # obtained from user_url_link
                                        comment_content, #comment
                                        post_id): # original post
                    exist_comments += 1
                    continue

            crawler.insert_comment(comment_publish_time,
                                comment_username_content_md5,
                                comment_user_name,
                                comment_content,
                                post_id,
                                crawler.language)
            added_comments += 1
        return exist_comments, added_comments

    def process_current_comment(self, chrome_browser, post_id, crawler):
        added_comments = 0
        exist_comments = 0

        found_md5, comment_entries = self.__get_comments_from_api(chrome_browser)
        comments_ul = self.__get_comment_blocks_from_html(chrome_browser)

        if comments_ul is None and len(comment_entries) == 0:
            print("Do not find comment ul block from post [%s]" % post_id)
            return added_comments, exist_comments
        
        comments_from_html = self.__get_comments_from_comment_ul(chrome_browser, comments_ul, found_md5)
        comment_entries += comments_from_html
        added_comments, exist_comments = self.__insert_new_comments(crawler, comment_entries, post_id)
        return added_comments, exist_comments

    @staticmethod
    def get_user_name_from_user_url(user_url):

        if user_url.find("profile.php?id") >= 0:
            pos1 = user_url.find("?id=")
            pos2 = user_url.find("&")
            return user_url[pos1+4:pos2]
        elif user_url.find("?comment_id=") >= 0:
            pos1 = user_url.find("?comment_id")
            update_url = user_url[:pos1]
            return update_url.replace("https://www.facebook.com/", "")
        else:
            return ""

    @staticmethod
    def get_abs_time_from_rel_time(rel_time):

        rel_time = rel_time.strip()
        current_time_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        current_date_obj = datetime.datetime.strptime(current_time_str, "%Y-%m-%d %H:%M:%S")

        if rel_time[-1] == "h":
            second = int(rel_time[:-1]) * 3600
        elif rel_time[-1] == "m":
            second = int(rel_time[:-1]) * 60
        elif rel_time[-1] == "d":
            second = int(rel_time[:-1]) * 24 * 3600
        elif rel_time[-1] == "w":
            second = int(rel_time[:-1]) * 7 * 24 * 3600
        else:
            return ""

        delta_obj = (current_date_obj - datetime.timedelta(seconds=int(second))).strftime("%Y-%m-%d")
        return str(delta_obj).strip() + " 00:00:00"
