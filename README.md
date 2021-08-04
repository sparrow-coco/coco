# oversea_crawler

## 海外facebook和twitter爬虫

### 功能

- 爬取facebook和twitter上给定账户下的发帖和相关回复
- 爬取facebook和twitter上给定关键词的搜索结果
- 将爬取数据存入数据库
- 输出数据库数据到本地


### 准备
本项目需要python3, 安装chrome和下载相应版本的chrome drive.

- 拉取项目到本地：

  ```bash
  $ cd $HOME
  $ git clone https://git.woa.com/ailabchenli/oversea_crawler.git
  $ cd oversea_crawler
  ```

- 安装python依赖包：
  
    `$ pip install -r requirements.txt`


### 运行

此程序有两种模式:

1. 爬取
2. 读取

**爬取:**

```python
python main.py <path_to_configure_file> crawler <source_and_way>
```

其中:

- `path_to_configure_file` 为配置文件路径

- `source_and_way` 为 facebook， facebook_search， twitter， twitter_search 四选一

例：

- `python main.py ./configures_dev.json crawler facebook`

**读取：**

```python
python main.py <path_to_configure_file> output <source> <start_date> <end_date> <output_directory>
```

其中:

- `path_to_configure_file`为配置文件路径

- `source` 为 facebook 或 twitter
- `start_date` 为需要读取数据的开始日期。yyyy-mm-dd格式
- `end_date` 为结束日期
- `output_directory` 为输出文件的存储目录

#### 配置文件参数说明

运行的所需参数全部存储在`./configures_dev.json`里，运行前可以进行配置。逐一解释如下：

`data_configure`:

- "db_host":数据库ip地址
- "db_port":数据库端口
- "db_user":数据库用户名
- "db_pwd":数据库密码
- "fb_db_name":数据库用于存储脸书数据的数据库名
- "tw_db_name":数据库用于存储推特数据的数据库名
- "chrome_drive_path":chrome drive的路径
- "headless":是否设定chrome drive运行是为headless，调试时可以设置成false
- "sleep_time":每次请求的时间间隔，单位为秒
- "fb_account":脸书账户
- "fb_pwd":脸书账户密码, 脸书这个必须要设置，否则无法爬取。目前都是在登录情况下爬取的。
- "facebook_url":需要爬取的脸书目标账户的url和名称。
- "facebook_search_keywords": 在脸书上需要爬取的关键词
- "tweet_url":需要爬取的推特目标账户的名称。这个和facebook_url不太一样，推特只要提供名称就可以，脸书需要提供url和名称
- "tweet_search_keywords":在脸书上需要爬取的关键词。这个可以和facebook_search_keywords一样

`optimization`:

- "max_workers":多线程时最大线程数。建议最少为2。
  - 如最大线程数太少，此程序会根据配置print推荐的线程数
  - 详情请见程序内相关代码的comment
- "timeout":程序timeout时间。此时间将不包括多线程启动chrome driver的时间。

#### 运行示例

Windows：

- “一键运行”所有爬虫可使用`superviser.py`。例：`python superviser.py`

- 在`scripts/windows`目录下有`.ps1`和 `.cmd`脚本，可参考使用
测试使
