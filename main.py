import json
import traceback
import globals
import sys

from configures import *
from daily_monitor import daily_monitor
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver import DesiredCapabilities
from threadings.browser_pool import BrowserPool
from threadings.utils import timeout

browser_list = []
browser_pool = None

def create_web_browsers(max_workers, options):
    # this is necessary to view network requests in selenium webdriver
    capabilities = DesiredCapabilities.CHROME
    capabilities["goog:loggingPrefs"] = {"performance": "ALL"}
    browsers = []
    for i in range(max_workers):
        if globals.data_configure.headless:
                options.headless = True
        browsers.append(webdriver.Chrome(
            globals.data_configure.chrome_drive_path, 
            options=options,
            desired_capabilities=capabilities))
    return browsers

def cleanup(wait_for_subprocesses=True):
    """clean up function called when app with browser pool exits

    Args:
        wait_for_subprocesses (bool, optional): whether if browser_pool.join() should be called. Defaults to True.
    """
    global browser_pool
    if browser_pool is None:
        print("Browser pool did not initiate properly. Did you call 'crawling_init()'?")
        exit(-1)
    if wait_for_subprocesses:
        browser_pool.join()
    browser_pool.close()
    print("Cleaned up")
    return

def config_init():
    with open(sys.argv[1], 'r', encoding="utf-8") as f:
        configure_obj = json.load(f)

    data_configure = DataConfigure(**configure_obj["data_configure"])
    globals.data_configure = data_configure
    globals.max_workers = configure_obj["optimization"]["max_workers"]
    globals.timeout_sec = configure_obj["optimization"]["timeout"]
    return

def crawling_init():
    global browser_pool
    global browser_list
    if sys.argv[2] != "crawler":
        print("Given 3 parameters, the task should be set as crawler. But it is [%s]"%sys.argv[2])
        sys.exit()

    options = Options()
    options.add_argument("--disable-notifications")

    try:
        browser_list = create_web_browsers(globals.max_workers, options)
        browser_pool = BrowserPool(browser_list)
    except:
        traceback.print_exc()
        print("Failed to create {} browser instances".format(globals.max_workers))
        exit(-1)
    return

@timeout(cleanup_function=cleanup, wait_for_subprocesses=False)
def main():
    print("Started main")
    if len(sys.argv) == 3:
        if sys.argv[2] == "monitor":
            daily_monitor()

    if len(sys.argv) == 4:
        if sys.argv[3] == "facebook":
            from crawler.facebook_crawler_after_login import FacebookCrawler
            fb_crawler = FacebookCrawler()
            for browser in browser_list:
                fb_crawler.login_facebook(browser)
            for one_facebook_url in globals.data_configure.facebook_url:
                # for each keyword, best to have AT MINIMUM 1 Thread for getting the urls + 1 Thread for processing the post
                if globals.max_workers < 2 * 1:
                    print("[ WARNING ] to maximize multithreading, please specify max_workers >= {}".format(2*1))
                fb_crawler.execute(one_facebook_url[0], one_facebook_url[1], browser_pool) #TODO: I could pool from this level
        elif sys.argv[3] == "facebook_search":
            from crawler.facebook_crawler_after_login import FacebookCrawler
            fb_crawler = FacebookCrawler()
            for browser in browser_list:
                fb_crawler.login_facebook(browser)
            for language, keyword_list in globals.data_configure.facebook_search_keywords.items():
                # for each keyword, best to have AT MINIMUM 1 Thread for getting the urls + 1 Thread for processing the post
                if globals.max_workers < 2 * len(keyword_list):
                    print("[ WARNING ] to maximize multithreading, please specify max_workers >= {}".format(2*len(keyword_list)))
                for one_keyword in keyword_list:
                    fb_crawler.execute_search(language, one_keyword, browser_pool)
                browser_pool.join()
        elif sys.argv[3] == "twitter":
            from crawler.tweet_crawler import TweetCrawler
            tw_crawler = TweetCrawler()
            if globals.max_workers < 2 * len(globals.data_configure.tweet_url):
                print("[ WARNING ] to maximize multithreading, please specify max_workers >= {}".format(2*len(globals.data_configure.tweet_url)))
            for one_tweet_name in globals.data_configure.tweet_url:
                tw_crawler.execute(one_tweet_name, browser_pool)
            browser_pool.join()
            
            if globals.max_workers < 2 * len(globals.data_configure.tweet_url):
                print("[ WARNING ] to maximize multithreading, please specify max_workers >= {}".format(2*len(globals.data_configure.tweet_url)))
            for one_tweet_name in globals.data_configure.tweet_url:
                tw_crawler.execute_without_account_info(one_tweet_name, browser_pool)
            browser_pool.join()
        elif sys.argv[3] == "twitter_search":                
            from crawler.tweet_crawler import TweetCrawler
            tw_crawler = TweetCrawler()
            for language, keyword_list in globals.data_configure.tweet_search_keywords.items():
                if globals.max_workers < 2 * len(keyword_list):
                    print("[ WARNING ] to maximize multithreading, please specify max_workers >= {}".format(2*len(keyword_list)))
                for one_keyword in keyword_list:
                    tw_crawler.execute_search(language, one_keyword, browser_pool)
                browser_pool.join()
        else:
            print("Currently only support facebook, twitter. But [%s] is given!"%sys.argv[3])
            sys.exit()
        cleanup(wait_for_subprocesses=True)

    if len(sys.argv) == 7:
        if sys.argv[2] != "output":
            print("Given 6 parameters, the task should be set as output. But it is [%s]"%sys.argv[2])
            sys.exit()

        globals.data_configure.output_directory = sys.argv[-1] + "/"
        globals.data_configure.start_date = sys.argv[-3]
        globals.data_configure.end_date = sys.argv[-2]
        if globals.data_configure.start_date >= globals.data_configure.end_date:
            print("Start date [%s] is large or equal to end date [%s]"%(sys.argv[-3], sys.argv[-2]))
            sys.exit()

        if sys.argv[3] == "facebook":
            from crawler.facebook_crawler import FacebookCrawler

            fb_crawler = FacebookCrawler()
            fb_crawler.output_content()
        elif sys.argv[3] == "twitter":
            from crawler.tweet_crawler import TweetCrawler

            tw_crawler = TweetCrawler()
            tw_crawler.output_content()
        else:
            print("Currently only support facebook, twitter. But [%s] is given!"%sys.argv[3])
            sys.exit()
    return

if __name__ == "__main__":
    if len(sys.argv) not in [3, 4, 7]:
        sys.stdout.write("Incorrect parameter numbers!\n")
        sys.stdout.write("Usage: main.py configure_online.txt task source start_date end_date out_directory")
        sys.exit()
    config_init()
    if len(sys.argv) == 4:
        crawling_init()
    main()



