import datetime
import globals
import hashlib
import re
import time
import sys

from bs4 import BeautifulSoup
from selenium.webdriver.common.action_chains import ActionChains
from utils.func import xpath_soup


class FacebookParser(object):

    def __init__(self):
        self.max_comment_click_try = 3
        self.max_try_on_view_more_comment = 5
        return

    def transfer_time_format(self, publish_time):

        elems = [item.strip() for item in publish_time.replace("at", ",").replace("AM", ",AM").replace("PM", ",PM").split(",")
                 if len(item.strip()) > 0]
        transferred_time = \
            str(datetime.datetime.strptime(elems[-3].strip() + " " + elems[-4].strip() + " " + elems[-2].strip() + " " + elems[-1],
                                           '%Y %B %d %I:%M %p'))

        transferred_time_obj = datetime.datetime.strptime(transferred_time, "%Y-%m-%d %H:%M:%S")
        utc_time_obj = (transferred_time_obj + datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S")

        return str(utc_time_obj).strip()

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
        post_blocks = html_obj.find_all("div", attrs={"class": "userContentWrapper"})

        return post_blocks

    def get_post_url_from_main_page(self, post_block):

        comment_form_block = post_block.find("form", attrs={"class": "commentable_item"})
        comment_a_blocks = comment_form_block.find_all("a", attrs={"role": "button"})
        for one_a_block in comment_a_blocks:
            if one_a_block is not None and re.match(r'[\d]+[.]?[\d]?[MK]? Comments',
                                                    one_a_block.text.strip()) is not None:
                if "href" in one_a_block.attrs:
                    return one_a_block.attrs["href"]

        return ""

    def get_publish_time_from_post_page(self, chrome_browser):

        html_obj = BeautifulSoup(chrome_browser.page_source, 'lxml')
        post_block = html_obj.find("div", attrs={"class": "userContentWrapper"})

        publish_time = ""
        publish_time_block = post_block.find("abbr")
        if publish_time_block is not None and "title" in publish_time_block.attrs:
            publish_time = publish_time_block.attrs["title"]

        if len(publish_time) == 0:
            return publish_time

        return self.transfer_time_format(publish_time)

    def get_content_from_post_page(self, chrome_browser):

        html_obj = BeautifulSoup(chrome_browser.page_source, 'lxml')
        post_block = html_obj.find("div", attrs={"class": "userContentWrapper"})

        post_content = ""
        user_content_block = post_block.find("div", attrs={"class": "userContent"})
        see_more_block = user_content_block.find("a", attrs={"class": "see_more_link"})
        if see_more_block is not None:
            try:
                xpath = xpath_soup(see_more_block)
                chrome_block = chrome_browser.find_element_by_xpath(xpath)
                if chrome_block is not None:
                    chrome_block = chrome_browser.find_element_by_xpath(xpath)
                    action = ActionChains(chrome_browser)
                    action.move_to_element(chrome_block).perform()
                    chrome_block.click()
                    time.sleep(globals.data_configure.sleep_time)
                    self.close_windows(chrome_browser)
                    update_html_obj = BeautifulSoup(chrome_browser.page_source, 'lxml')
                    update_post_block = update_html_obj.find("div", attrs=post_block.attrs)
                    update_user_content_block = update_post_block.find("div", attrs={"class": "userContent"})
                    post_content = update_user_content_block.text.strip()
                    if post_content[-8:] == "See More":
                        post_content = post_content[:-8]
            except Exception as e:
                print(e)
                print("Error in clicking [See More] url")
                self.close_windows(chrome_browser)
                chrome_browser.execute_script("window.scrollBy(0, 10)")
        else:
            post_content = user_content_block.text.strip()

        return post_content.replace("'", "")

    def process_comments_on_post_page(self, chrome_browser, post_id, crawler):

        self.close_windows(chrome_browser)
        html_obj = BeautifulSoup(chrome_browser.page_source, 'lxml')
        post_block = html_obj.find("div", attrs={"class": "userContentWrapper"})

        # expand comment
        comment_form_block = post_block.find("form", attrs={"class": "commentable_item"})
        comment_a_blocks = comment_form_block.find_all("a", attrs={"role": "button"})
        success_expand_comment = False
        try_count = 0
        for one_a_block in comment_a_blocks:
            if one_a_block is not None and re.match(r'[\d]+[.]?[\d]?[MK]? Comments', one_a_block.text.strip()) is not None:
                while try_count <= self.max_comment_click_try:
                    try:
                        xpath = xpath_soup(one_a_block)
                        comment_number_chrome_block = chrome_browser.find_element_by_xpath(xpath)
                        if comment_number_chrome_block is not None:
                            print("Comment Box Text: ", one_a_block.text.strip())
                            action = ActionChains(chrome_browser)
                            action.move_to_element(comment_number_chrome_block).perform()
                            comment_number_chrome_block.click()
                            print("sleep to wait [Number Comments] expand")
                            time.sleep(globals.data_configure.sleep_time)
                            success_expand_comment = True
                            break
                    except Exception as e:
                        print(e)
                        print("Error in clicking [Comments] url")
                        self.close_windows(chrome_browser)
                        chrome_browser.execute_script("window.scrollBy(0, 100)")
                        self.close_windows(chrome_browser)
                        try_count += 1

                break

        if success_expand_comment is False:
            log_str = "Fail to expand Comments button under post %s" % post_id
            print(log_str)
            sys.stderr.write(log_str + "\n")
            return

        # click view more comments until the end
        click_count = 0
        max_click = 100
        all_processed_comment = set()
        all_processed_reply = set()
        while True:

            html_obj = BeautifulSoup(chrome_browser.page_source, 'lxml')
            update_post_block = html_obj.find("div", attrs=post_block.attrs)
            if update_post_block is None:
                print("Do not find update_post_block when finding view more comments")
                return []

            comment_form_block = update_post_block.find("form", attrs={"class": "commentable_item"})
            comment_a_blocks = comment_form_block.find_all("a", attrs={"role": "button"})
            view_more_comments_soup_block = None
            for one_a_block in comment_a_blocks:
                if one_a_block is not None and one_a_block.text.strip() == "View more comments":
                    view_more_comments_soup_block = one_a_block
                    break

            # no view more comments link
            if view_more_comments_soup_block is None:
                print("Do not find [view more comments] block")
                break

            try_count = 0
            while try_count < self.max_try_on_view_more_comment:
                try:
                    xpath = xpath_soup(view_more_comments_soup_block)
                    view_more_comments_chrome_block = chrome_browser.find_element_by_xpath(xpath)
                    if view_more_comments_chrome_block is None:
                        break
                    action = ActionChains(chrome_browser)
                    action.move_to_element(view_more_comments_chrome_block).perform()
                    view_more_comments_chrome_block.click()
                    time.sleep(globals.data_configure.sleep_time)
                    click_count += 1
                    break
                except Exception as e:
                    print(e)
                    log_str = "Error in clicking [View more comments] url"
                    print(log_str)
                    sys.stderr.write(log_str + "\n")
                    self.close_windows(chrome_browser)
                    chrome_browser.execute_script("window.scrollBy(0, 100)")
                    self.close_windows(chrome_browser)
                    try_count += 1

            # reach the max click time or error number reaches the max try count
            if click_count >= max_click or try_count >= self.max_try_on_view_more_comment:
                break

            need_continue = self.process_current_comment(chrome_browser,
                                                         post_block,
                                                         post_id,
                                                         crawler,
                                                         all_processed_comment,
                                                         all_processed_reply)

            if not need_continue:
                print("Early stop at post page %s and # of processed comment is [%d] and # of processed reply is [%d]" %
                      (post_id, len(all_processed_comment), len(all_processed_reply)))
                break

            print("[%d] comments and [%d] replies have been processed under current post page" %
                  (len(all_processed_comment), len(all_processed_reply)))

        print("Stop at post page [%s] and # of processed comment is [%d] and # of processed reply is [%d]"%
              (post_id, len(all_processed_comment), len(all_processed_reply)))

    def process_current_comment(self, chrome_browser, post_block, post_id, crawler, uniq_comment, uniq_reply):

        html_obj = BeautifulSoup(chrome_browser.page_source, 'lxml')
        update_post_block = html_obj.find("div", attrs=post_block.attrs)
        if update_post_block is None:
            return []

        comment_form_block = update_post_block.find("form", attrs={"class": "commentable_item"})
        all_comment_blocks = comment_form_block.find_all("li")

        already_exist = 0
        legal_comment = 0
        for one_comment in all_comment_blocks:

            time_block = one_comment.find("abbr")
            if time_block is None:
                continue
            if "data-tooltip-content" in time_block.attrs:
                comment_publish_time = time_block.attrs["data-tooltip-content"]
                comment_publish_time = self.transfer_time_format(comment_publish_time)
            else:
                continue

            content_block = one_comment.find("span", attrs={"dir": "ltr"})
            if content_block is None:
                continue

            see_more_block = content_block.find("a", attrs={"role": "action"})
            if see_more_block is not None and see_more_block.text.strip() == "See More":
                try:
                    xpath = xpath_soup(see_more_block)
                    see_more_chrome_block = chrome_browser.find_element_by_xpath(xpath)
                    if see_more_chrome_block is not None:
                        while True:
                            try:
                                action = ActionChains(chrome_browser)
                                action.move_to_element(see_more_chrome_block).perform()
                                see_more_chrome_block.click()
                                print("sleep to wait comment expand")
                                time.sleep(globals.data_configure.sleep_time)
                                break
                            except Exception as e:
                                print(e)
                                print("Error in clicking [Comment See More] url")
                                self.close_windows(chrome_browser)
                                chrome_browser.execute_script("window.scrollBy(0, 100)")
                                see_more_chrome_block = chrome_browser.find_element_by_xpath(xpath)
                                self.close_windows(chrome_browser)
                        self.close_windows(chrome_browser)
                    xpath = xpath_soup(content_block)
                    content_chrome_block = chrome_browser.find_element_by_xpath(xpath)
                    comment_content = content_chrome_block.text.strip().replace("'", "").strip()
                except Exception as e:
                    continue
            else:
                comment_content = content_block.text.strip().replace("'", "").strip()

            if len(comment_content) == 0:
                continue

            if content_block.parent is None or content_block.parent.contents is None or \
                    len(content_block.parent.contents) == 0:
                continue
            comment_user_name = content_block.parent.contents[0].text.strip().replace("'", "").strip()

            # get comment info
            comment_username_content_md5 = hashlib.md5((comment_user_name + " " + comment_content).encode()).hexdigest()

            legal_comment += 1
            uniq_comment.add((comment_publish_time,
                              comment_username_content_md5,
                              comment_user_name,
                              comment_content,
                              post_id))

            if crawler.find_comment(comment_publish_time, comment_username_content_md5, comment_user_name,
                                    comment_content, post_id):
                already_exist += 1
                continue

            crawler.insert_comment(comment_publish_time, comment_username_content_md5, comment_user_name,
                                   comment_content, post_id)

            # now start to get the replies for each comments
            all_a_blocks = one_comment.find_all("a", attrs={"role": "button"})
            reply_block = None
            for item in all_a_blocks:
                if re.search(r'[\d]+ Repl', item.text.strip()) is not None:
                    reply_block = item
                    break

            if reply_block is not None:
                try:
                    xpath = xpath_soup(reply_block)
                    reply_chrome_block = chrome_browser.find_element_by_xpath(xpath)
                    # click only once for expanding reply list
                    if reply_chrome_block is not None:
                        while True:
                            try:
                                time.sleep(globals.data_configure.sleep_time)
                                action = ActionChains(chrome_browser)
                                action.move_to_element(reply_chrome_block).perform()
                                reply_chrome_block.click()
                                time.sleep(globals.data_configure.sleep_time)
                                break
                            except Exception as e:
                                print(e)
                                print("Error in clicking [Reply] url")
                                self.close_windows(chrome_browser)
                                chrome_browser.execute_script("window.scrollBy(0, 150)")
                                reply_chrome_block = chrome_browser.find_element_by_xpath(xpath)
                                self.close_windows(chrome_browser)

                        comment_chrome_block = chrome_browser.find_element_by_xpath(xpath_soup(one_comment))
                        comment_bs_block = BeautifulSoup(comment_chrome_block.get_attribute("innerHTML"), 'lxml')

                        # get all replies under this comment
                        for one_reply_li_block in comment_bs_block.find_all("li"):
                            time_block = one_reply_li_block.find("abbr")
                            if time_block is None:
                                continue
                            if "data-tooltip-content" in time_block.attrs:
                                reply_publish_time = time_block.attrs["data-tooltip-content"]
                                reply_publish_time = self.transfer_time_format(reply_publish_time)
                            else:
                                continue

                            content_block = one_reply_li_block.find("span", attrs={"dir": "ltr"})
                            if content_block is None:
                                continue
                            reply_content = content_block.text.strip().replace("'", "")
                            if len(reply_content) == 0:
                                continue

                            if content_block.parent is None or content_block.parent.contents is None or len(
                                    content_block.parent.contents) == 0:
                                continue
                            reply_user_name = content_block.parent.contents[0].text.strip().replace("'", "").strip()

                            reply_username_content_md5 = hashlib.md5((reply_user_name + " " + reply_content).encode()). \
                                hexdigest()
                            crawler.insert_reply(reply_publish_time, reply_username_content_md5, reply_user_name,
                                                 reply_content, comment_publish_time, comment_username_content_md5)
                            uniq_reply.add((reply_publish_time, reply_username_content_md5, reply_user_name,
                                            reply_content, comment_publish_time, comment_username_content_md5))
                except Exception as e:
                    continue

        if (0 < legal_comment == already_exist) or legal_comment == 0:
            return False

        return True
