import sys
import subprocess
import concurrent.futures
import datetime

FLAG_DEV=False

class Task(object):
    def __init__(self, script_path) -> None:
        self.__script_path = script_path

    def get_script(self):
        return self.__script_path
    
    def run(self):
        p = subprocess.Popen(["powershell.exe", self.__script_path], 
            stdout=sys.stdout)
        p.communicate()


def start_all_spider():
    Tasks = [
        Task("scripts\\windows\\start_facebook.ps1"),
        Task("scripts\\windows\\start_facebook_search.ps1"),
        Task("scripts\\windows\\start_twitter.ps1"),
        Task("scripts\\windows\\start_twitter_search.ps1")
    ]
    futures = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        for task in Tasks:
            print("Starting task {} at {}".format(task.get_script(), datetime.datetime.now()))
            sys.stdout.flush()
            futures.append(executor.submit(task.run))
        for future in concurrent.futures.as_completed(futures):
            try:
                ret = future.result()
                print(ret)
            except Exception as e:
                print(e)
            sys.stdout.flush()
    return

if __name__ == "__main__":
    start_all_spider()
    """
    if FLAG_DEV:
        start_all_spider()
    else:
        # 每日定时执行
        trigger_time = '06:00'
        print("爬虫任务将在每日 %s 触发执行。" % trigger_time)
        schedule.every().day.at(trigger_time).do(start_all_spider)
        while True:
            schedule.run_pending()
    """
