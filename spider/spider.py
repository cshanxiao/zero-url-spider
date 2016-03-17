# -*- coding: utf8 -*-
u'''
@summary:
@author: Administrator
@date: 2016年3月15日
'''
import os
import re
import sys
import threading
import time
import traceback
import urlparse

import requests


class Spider(object):
    def __init__(self, url, deep, save_path, total_time=300, timeout=30, start_from_index=True):
        self.origin_url = url
        self.scheme, self.base_url = self.split_url(self.origin_url)
        self.start_url = "://".join([self.scheme, self.base_url])
        if self.scheme is None or self.base_url is None:
            raise RuntimeError("Url error! Not a valid url! url: %s"
                               % url)

        self.deep = deep
        self.save_path = os.path.abspath(save_path)
        self.file_lock = threading.Lock()
        self._mkfile()

        self.total_time = total_time
        self.timeout = timeout
        self.start_from_index = start_from_index

        self.end_flag = False

        self.sess = requests.Session()
        self.uncrawled_urls = set()
        self.urls = set()

    def _mkfile(self):
        if os.path.exists(self.save_path):
            os.remove(self.save_path)
        with open(self.save_path, "w") as fd:
            fd.write("")

        if os.path.exists(self.save_path + ".temp"):
            os.remove(self.save_path + ".temp")
        with open(self.save_path + ".temp", "w") as fd:
            fd.write("")

        if not os.path.isfile(self.save_path):
            raise RuntimeError("Save_path error! Not a file path! save_path: %s"
                               % self.save_path)

    def split_url(self, url):
        url_info = urlparse.urlsplit(url)
        if url_info.scheme is None or url_info.hostname is None:
            url_info = urlparse.urlsplit("://".join(["http", url]))
        return url_info.scheme, url_info.netloc

    def is_url(self, url):
        regex = re.compile(
            r'^(?:http|ftp)s?://' # http:// or https://
            r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' #domain...
            r'localhost|' # localhost...
            r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # ...or ip
            r'(?::\d+)?' # optional port
            r'(?:/?|[/?]\S+)$', re.IGNORECASE)
        return False if regex.match(url) is None else True

    def _get_urls(self, url):
        try:
            urls = set()
            content = self.sess.get(url, timeout=self.timeout).content
            url_regular = r'["\']((%s|/).*?)["\']' % self.scheme
            tmp_urls = re.findall(url_regular, content, re.MULTILINE)
            urls.update(set([item[0] for item in tmp_urls if item]))

            tmp_urls = re.findall(r'src.*?=.*?["\'](.*?)["\']', content, re.MULTILINE)
            urls.update(set([item for item in tmp_urls if item]))

            tmp_urls = re.findall(r'href.*?=.*?["\'](.*?)["\']', content, re.MULTILINE)
            urls.update(set([item for item in tmp_urls if item]))

            tmp_urls = re.findall(r'url.*?=.*?["\'](.*?)["\']', content, re.MULTILINE)
            urls.update(set([item for item in tmp_urls if item]))

            return True, urls
        except:
#             traceback.print_exc()
            return False, []

    def get_index(self):
        if self.start_from_index:
            state, urls = self._get_urls(self.start_url)
        else:
            state, urls = self._get_urls(self.origin_url)
        if not state:
            raise RuntimeError("Start url can not reachable! start_url: %s"
                               % self.start_url)

        self.uncrawled_urls.update(urls)

    def get_urls(self, url):
        state, urls = self._get_urls(url)
        if not state:
            return
        self.uncrawled_urls.update(urls)
        self.urls.add(url)
        with self.file_lock:
            with open(self.save_path, "a") as fd:
                fd.write(url + "\n")

    def _time_thread(self):
        while self.total_time > 0 and not self.end_flag:
            time.sleep(2)
            self.total_time -= 2
        self.end_flag = True

    def stop(self):
        if os.path.exists(self.save_path + ".temp"):
            os.remove(self.save_path + ".temp")
        self.end_flag = True
        sys.exit(0)

    def crawl_url(self):
        time_thread = threading.Thread(target=self._time_thread)
        time_thread.setDaemon(True)
        time_thread.start()

        self.get_index()
        wait_times = 12
        while not self.end_flag and wait_times > 0:
            if len(self.uncrawled_urls) == 0:
                time.sleep(10)
                wait_times -= 1
                continue

            url = self.uncrawled_urls.pop()
            if not url.startswith(self.scheme):
                if url.startswith("/"):
                    url = "".join([self.start_url, url])
                else:
                    url = "/".join([self.start_url, url])
            else:
                _scheme, netloc = self.split_url(url)
                if netloc != self.base_url:
                    continue

            tmp_url = url.replace("%s://" % self.scheme, "")
            if tmp_url.count("/") > self.deep:
                continue

            if url in self.urls:
                continue

            if not self.is_url(url):
                continue

            crawl_thread = threading.Thread(target=self.get_urls, args=(url,))
            crawl_thread.setDaemon(True)
            crawl_thread.start()
            time.sleep(0.2)
        self.stop()

def parse_argv():
    import optparse
    parser = optparse.OptionParser()
    parser.add_option("--url", "--url", dest="url", default=None)
    parser.add_option("--deep", "--deep", dest="deep", default=3)
    parser.add_option("--save_path", "--save_path", dest="save_path", default='./urls.txt')
    parser.add_option("--total_time", "--total_time", dest="total_time", default=300)
    parser.add_option("--time_out", "--time_out", dest="time_out", default=30)
    parser.add_option("--start_from_index", "--start_from_index",
                      dest="start_from_index", default=1)

    try:
        _options, _args = parser.parse_args()
    except:
        parser.print_help()
        raise
    return _options

def test():
#     spider = Spider("http://m7lrv.com", 3, "./m7lrv.txt")
    spider = Spider("http://www.aizhan.com/", 2, "./aizhan.txt", 60 * 30)
    try:
        spider.crawl_url()
    finally:
        spider.stop()

def main():
#     test()
    options = parse_argv()
    url = unicode(options.url, sys.stdin.encoding)
    deep = int(options.deep)
    save_path = os.path.abspath(unicode(options.save_path, sys.stdin.encoding))
    total_time = int(options.total_time)
    time_out = int(options.deep)
    start_from_index = int(options.start_from_index)

    spider = Spider(url, deep, save_path, total_time, time_out, start_from_index)
    try:
        spider.crawl_url()
    finally:
        spider.stop()

if __name__ == '__main__':
    import sys
    reload(sys)
    sys.setdefaultencoding('utf8')
    main()

