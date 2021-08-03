import json
import traceback

from typing import Callable
from selenium import webdriver

""" 
REQUIRES the following setup at minimum
# make chrome log requests
capabilities = DesiredCapabilities.CHROME
capabilities["goog:loggingPrefs"] = {"performance": "ALL"}
driver = webdriver.Chrome(
    desired_capabilities=capabilities, executable_path="./chromedriver"
)
"""

def __get_current_requests(chrome:webdriver.Chrome):
    logs_raw = chrome.get_log("performance")
    return [json.loads(lr["message"])["message"] for lr in logs_raw]

def get_current_responses(chrome:webdriver.Chrome, resp_filter:Callable=None):
    """
    Args:
        chrome (webdriver.Chrome): the webdriver (assumed chrome for now) from which we extract request data
        resp_filter (Callable, optional): filter function that decides if a log/request/resp data is preserved, 
            by returning True/False on keeping the data/throw away the data, respectively. Note that by default,
            ONLY NEW Requests/responses (relative to last call of this method + driver combo) will be returned
            
            You should expect three parameters being passed in for filtering
                log: full log from chrome. basically all information you need
                req_url: request url (without path)
                resp_body: raw response body
            For example, request_id can be extracted by: request_id = log["params"]["requestId"]
    Returns:
        list: list of response_body that returns True from the @resp_filter function
    """
    # extract requests from logs
    reqs = __get_current_requests(chrome)

    def simple_filter(log:dict, resp_url, resp_body):
        try:
            actual_body = json.loads(resp_body["body"])
        except:
            return False
        return (
            # is an actual response, and I want to filter to the important onces
            log["method"] == "Network.responseReceived"
            and log["params"]["response"]
            and "text/javascript" in log["params"]["response"]["mimeType"]
            and log["params"]["response"]["requestHeaders"][":path"] == "/api/graphql/"
            and actual_body["data"]["node"]["timeline_feed_units"] is not None
        )
    
    resp_filter = resp_filter or simple_filter
    ret = []
    for log in reqs:
        # skip the responses net yet received
        if "Network.responseReceived" != log["method"]:
            continue
        request_id = log["params"]["requestId"]
        req_url = log["params"]["response"].get("url")
        try:
            resp_body = chrome.execute_cdp_cmd("Network.getResponseBody", {"requestId": request_id})
        except:
            #print("[ WARNING ] request_id={} has empty body".format(request_id))
            continue
        if resp_filter(log, req_url, resp_body):
            print(f"Caught {req_url}")
            ret.append(resp_body)            
    return ret

def get_data_from_browser_network(browser, filter_function:Callable, extract_function:Callable):
    """Generic Function that uses @filter_function to get the desired requests you want, and then use
    @extract_function to extract information needed from each corresponding resp_body

    Args:
        browser (a webdriver): currrent browser instance from which we want to get network data
        filter_function (Callable): function used to filter through the requests of @browser 
        extract_function (Callable): function used to extract needed data from each resp_body of the corresponding filtered request

    Returns:
        list: collection of UNIQUE extracted data from @extract_function
    """
    ret = set()
    resp_bodies = get_current_responses(browser, filter_function)
    for resp_body in resp_bodies:
        data = extract_function(resp_body)
        ret |= set(data)
    return list(ret)