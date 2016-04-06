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
        all_express = [
                       (r'^(?:http|ftp)s?://' # http:// or https://
                        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' #domain...
                        r'localhost|' # localhost...
                        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # ...or ip
                        r'(?::\d+)?' # optional port
                        r'(?:/?|[/?]\S+)$'),

                    ("^(http|https|ftp)\\://([a-zA-Z0-9\\.\\-]+(\\:[a-zA-"
                      + "Z0-9\\.&%\\$\\-]+)*@)?((25[0-5]|2[0-4][0-9]|[0-1]{1}[0-9]{"
                      + "2}|[1-9]{1}[0-9]{1}|[1-9])\\.(25[0-5]|2[0-4][0-9]|[0-1]{1}"
                      + "[0-9]{2}|[1-9]{1}[0-9]{1}|[1-9]|0)\\.(25[0-5]|2[0-4][0-9]|"
                      + "[0-1]{1}[0-9]{2}|[1-9]{1}[0-9]{1}|[1-9]|0)\\.(25[0-5]|2[0-"
                      + "4][0-9]|[0-1]{1}[0-9]{2}|[1-9]{1}[0-9]{1}|[0-9])|([a-zA-Z0"
                      + "-9\\-]+\\.)*[a-zA-Z0-9\\-]+\\.[a-zA-Z]{2,4})(\\:[0-9]+)?(/"
                      + "[^/][a-zA-Z0-9\\.\\,\\?\\'\\\\/\\+&%\\$\\=~_\\-@]*)*$")
        ]
        for express in all_express:
            regex = re.compile(express, re.IGNORECASE)
            if regex.match(url) is None:
                return False
        return True

    def _get_urls(self, url):
        u'''
        @note: 由于大量网页代码不规范，只能尽量解析提取页面里面url
        '''
        try:
            urls = set()
            resp = self.sess.get(url, timeout=self.timeout)
            if resp.status_code != 200:
                if resp.status_code == 404:
                    return False, []
            if "text" not in resp.headers["content-type"]:
                return False, []

            if "css" in resp.headers["content-type"]:
                return False, []

            content = resp.content

            # 解析引号内可能存在的url
            url_regular = r'["\']((%s|/).*?)["\']' % self.scheme
            tmp_urls = re.findall(url_regular, content, re.MULTILINE)
            urls.update(set([item[0] for item in tmp_urls if item]))

            # 解析特定标签属性内可能存在的url
            tmp_urls = re.findall(r'src.*?=.*?["\'](.*?)["\']', content, re.MULTILINE)
            urls.update(set([item for item in tmp_urls if item]))

            tmp_urls = re.findall(r'href.*?=.*?["\'](.*?)["\']', content, re.MULTILINE)
            urls.update(set([item for item in tmp_urls if item]))

            tmp_urls = re.findall(r'url.*?=.*?["\'](.*?)["\']', content, re.MULTILINE)
            urls.update(set([item for item in tmp_urls if item]))

            # 解析js里面可能存在的url
#             js_url_re = 'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
#             tmp_urls = set(re.findall(js_url_re, content, re.MULTILINE))
#             urls.update(set([item for item in tmp_urls if item]))

            ignore_urls = set()
            ignore_exts = ["0.001", "0.907", ".acp", ".aif", ".aiff", ".au", ".awf", ".bmp", ".c4t", ".cal", ".cdf", ".cel", ".cg4", ".cit", ".cmx", ".crl", ".csi", ".cut", ".dbm", ".der", ".dib", ".doc", ".drw", ".dwf", ".dwg", ".dxf", ".emf", ".eps", ".etd", ".fax", ".fif", ".frm", ".gbr", ".gif", ".gp4", ".hmr", ".hpl", ".hrf", ".ico", ".iff", ".igs", ".img", ".isp", ".java", ".jpe", ".jpeg", ".jpg", ".lar", ".lavs", ".lmsff", ".ltr", ".m2v", ".m4e", ".man", ".mdb", ".mfp", ".mhtml", ".mid", ".mil", ".mnd", ".mocha", ".mp1", ".mp2v", ".mp4", ".mpd", ".mpeg", ".mpga", ".mps", ".mpv", ".mpw", ".net", ".nws", ".out", ".p12", ".p7c", ".p7r", ".pc5", ".pcl", ".pdf", ".pdx", ".pgl", ".pko", ".plt", ".png", ".ppa", ".pps", ".ppt", ".prf", ".prt", ".ps", ".pwz", ".ra", ".ras", ".red", ".rjs", ".rlc", ".rm", ".rmi", ".rmm", ".rms", ".rmx", ".rp", ".rsml", ".rtf", ".rv", ".sat", ".sdw", ".slb", ".slk", ".smil", ".snd", ".spl", ".ssm", ".stl", ".sty", ".swf", ".tg4", ".tif", ".tiff", ".top", ".uin", ".vdx", ".vpg", ".vsd", ".vst", ".vsw", ".vtx", ".wav", ".wb1", ".wb3", ".wiz", ".wk4", ".wks", ".wma", ".wmf", ".wmv", ".wmz", ".wpd", ".wpl", ".wr1", ".wrk", ".ws2", ".xdp", ".xfd", ".xls", ".xwd", ".sis", ".x_t", ".apk", ".tif", ".301", ".906", ".a11", ".ai", ".aifc", ".anv", ".asf", ".asx", ".avi", ".bot", ".c90", ".cat", ".cdr", ".cer", ".cgm", ".class", ".cmp", ".cot", ".crt", ".css", ".dbf", ".dbx", ".dcx", ".dgn", ".dll", ".dot", ".dwf", ".dxb", ".edn", ".eml", ".epi", ".eps", ".exe", ".fdf", ".g4", ".", ".gl2", ".hgl", ".hpg", ".hqx", ".hta", ".icb", ".ico", ".ig4", ".iii", ".ins", ".IVF", ".jfif", ".jpe", ".jpg", ".js", ".la1", ".latex", ".lbm", ".ls", ".m1v", ".m3u", ".mac", ".mdb", ".mht", ".mi", ".midi", ".mns", ".movie", ".mp2", ".mp3", ".mpa", ".mpe", ".mpg", ".mpp", ".mpt", ".mpv2", ".mpx", ".mxp", ".nrf", ".p10", ".p7b", ".p7m", ".p7s", ".pci", ".pcx", ".pdf", ".pfx", ".pic", ".pl", ".pls", ".png", ".pot", ".ppm", ".ppt", ".pr", ".prn", ".ps", ".ptn", ".ram", ".rat", ".rec", ".rgb", ".rjt", ".rle", ".rmf", ".rmj", ".rmp", ".rmvb", ".rnx", ".rpm", ".rtf", ".sam", ".sdp", ".sit", ".sld", ".smi", ".smk", ".spc", ".sst", ".tdf", ".tga", ".tif", ".torrent", ".vda", ".vsd", ".vss", ".vst", ".vsx", ".wax", ".wb2", ".wbmp", ".wk3", ".wkq", ".wm", ".wmd", ".wmx", ".wp6", ".wpg", ".wq1", ".wri", ".ws", ".wvx", ".xfdf", ".xls", ".xlw", ".xpl", ".x_b", ".sisx", ".ipa", ".xap"]
            for url in urls:
                for ext in ignore_exts:
                    tmp_url = url.lower()
                    if tmp_url.endswith(ext):
                        ignore_urls.add(url)
                        break
            return True, urls.difference(ignore_urls)
        except:
#             traceback.print_exc()
            return False, []

    def get_index(self):
        if self.start_from_index:
            state, urls = self._get_urls(self.start_url)
        else:
            state, urls = self._get_urls(self.origin_url)
        if not state:
            raise RuntimeError("Start url is not available! start_url: %s"
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
    u'''
    1:访问url出现错误的时候代码报错，并且temp临时文件不会删除（url在NET里面是可以访问的，但是到了py里面就报错了）
            案例网站：
        http://fang.taobao.com
        http://www.0791bao.cn
        http://www.jlgjqz.com

    2：最好开启一个debug参数，用来记录错误信息，这样出现问题你也能快速的摘到问题代码位置（看你自己愿意加不加了）

    py 2016/3/30 16:57:31
    3：个别网站采集出来的url不完整 案例网站：http://www.zhiyiwang.com
    '''

#     spider = Spider("http://m7lrv.com", 10, "./m7lrv.txt")
    spider = Spider("www.ejuhotel.com", 3, "./aizhan.txt", 60 * 3)

#     spider = Spider("http://tieba.baidu.com/f?kw=c%23&fr=ala0&tpl=5", 10, "./tieba.txt", 60 * 10)
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

