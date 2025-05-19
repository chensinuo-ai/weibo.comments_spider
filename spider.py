#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import json
import logging
import logging.config
import os
import random
import shutil
import sys
import time
from datetime import date, datetime, timedelta
from time import sleep

from absl import app, flags
from tqdm import tqdm
import requests
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
import warnings
import csv
from fake_useragent import UserAgent

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)
from . import config_util, datetime_util
from .downloader import AvatarPictureDownloader
from .parser import AlbumParser, IndexParser, PageParser, PhotoParser
from .user import User

FLAGS = flags.FLAGS

flags.DEFINE_string('config_path', None, 'The path to config.json.')
flags.DEFINE_string('u', None, 'The user_id we want to input.')
flags.DEFINE_string('user_id_list', None, 'The path to user_id_list.txt.')
flags.DEFINE_string('output_dir', None, 'The dir path to store results.')

logging_path = os.path.split(
    os.path.realpath(__file__))[0] + os.sep + 'logging.conf'
logging.config.fileConfig(logging_path)
logger = logging.getLogger('spider')

def get_total_pages(url, headers):
    """获取评论总页数"""
    try:
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, 'lxml')
        pagination = soup.find('div', id='pagelist')
        if pagination:
            input_tag = pagination.find('input', {'name': 'mp'})
            return int(input_tag['value']) if input_tag else 1
        return 1
    except Exception as e:
        logger.error(f"获取总页数失败: {str(e)}")
        return 1

def clean_header_value(value: str) -> str:
    """清理请求头中的非法字符"""
    return value.strip().replace('\ufeff', '').replace('\xa0', ' ')

class Spider:
    def __init__(self, config):
        """Weibo类初始化"""
        self.filter = config['filter']
        since_date = config['since_date']
        if isinstance(since_date, int):
            since_date = date.today() - timedelta(since_date)
        self.since_date = str(since_date)
        self.end_date = config['end_date']
        random_wait_pages = config['random_wait_pages']
        self.random_wait_pages = [min(random_wait_pages), max(random_wait_pages)]
        random_wait_seconds = config['random_wait_seconds']
        self.random_wait_seconds = [min(random_wait_seconds), max(random_wait_seconds)]
        self.global_wait = config['global_wait']
        self.page_count = 0
        self.write_mode = config['write_mode']
        self.pic_download = config['pic_download']
        self.video_download = config['video_download']
        self.file_download_timeout = config.get('file_download_timeout', [5, 5, 10])
        self.result_dir_name = config.get('result_dir_name', 0)
        self.cookie = config['cookie']
        self.mysql_config = config.get('mysql_config')
        self.sqlite_config = config.get('sqlite_config')
        self.kafka_config = config.get('kafka_config')
        self.mongo_config = config.get('mongo_config')
        self.max_weibo_pages = config.get('max_weibo_pages', 2)  # 获取要爬取的微博页数
        self.max_comment_pages = config.get('max_comment_pages', 2)  # 获取每条微博要爬取的评论页数
        self.user_config_file_path = ''
        user_id_list = config['user_id_list']
        if FLAGS.user_id_list:
            user_id_list = FLAGS.user_id_list
        if not isinstance(user_id_list, list):
            if not os.path.isabs(user_id_list):
                user_id_list = os.getcwd() + os.sep + user_id_list
            if not os.path.isfile(user_id_list):
                logger.warning('不存在%s文件', user_id_list)
                sys.exit()
            self.user_config_file_path = user_id_list
        if FLAGS.u:
            user_id_list = FLAGS.u.split(',')
        if isinstance(user_id_list, list):
            user_config_list = list(
                map(
                    lambda x: {
                        'user_uri': x['id'],
                        'since_date': x.get('since_date', self.since_date),
                        'end_date': x.get('end_date', self.end_date),
                    }, [
                        user_id for user_id in user_id_list
                        if isinstance(user_id, dict)
                    ])) + list(
                        map(
                            lambda x: {
                                'user_uri': x,
                                'since_date': self.since_date,
                                'end_date': self.end_date
                            },
                            set([
                                user_id for user_id in user_id_list
                                if not isinstance(user_id, dict)
                            ])))
            if FLAGS.u:
                config_util.add_user_uri_list(self.user_config_file_path,
                                              user_id_list)
        else:
            user_config_list = config_util.get_user_config_list(
                user_id_list, self.since_date)
            for user_config in user_config_list:
                user_config['end_date'] = self.end_date
        self.user_config_list = user_config_list
        self.user_config = {}
        self.new_since_date = ''
        self.user = User()
        self.got_num = 0
        self.weibo_id_list = []
        self.weibo_counter = 0  # 添加微博计数器
        
        # 初始化评论爬虫相关属性
        self.ua = UserAgent()
        self.comment_headers = {
            'cookie': clean_header_value(self.cookie),
            'referer': 'https://weibo.cn/u/2803301701',
            'user-agent': self.ua.random,
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'accept-encoding': 'gzip, deflate, br',
            'connection': 'keep-alive',
            'upgrade-insecure-requests': '1',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-user': '?1',
            'cache-control': 'max-age=0'
        }
        # 使用代理池
        from .proxy_pool import proxy_pool
        self.proxy_pool = proxy_pool
        
        # 添加随机延时
        self.min_delay = 5
        self.max_delay = 15
        
        # 添加请求失败计数器
        self.fail_count = 0
        self.max_fails = 3
        
        # 添加会话管理
        self.session = requests.Session()
        self.session.headers.update(self.comment_headers)

    def _random_delay(self):
        """随机延时"""
        delay = random.uniform(self.min_delay, self.max_delay)
        logger.info(f"随机延时 {delay:.2f} 秒")
        sleep(delay)

    def _rotate_user_agent(self):
        """轮换User-Agent"""
        self.comment_headers['user-agent'] = self.ua.random
        self.session.headers.update(self.comment_headers)

    def _handle_request_error(self, e):
        """处理请求错误"""
        self.fail_count += 1
        logger.error(f"请求失败 ({self.fail_count}/{self.max_fails}): {str(e)}")
        
        if self.fail_count >= self.max_fails:
            logger.warning("连续失败次数过多，等待较长时间后重试")
            sleep(random.uniform(300, 600))  # 等待5-10分钟
            self.fail_count = 0
        else:
            self._random_delay()
            self._rotate_user_agent()

    def crawl_comments(self, weibo_url, weibo_id, weibo_number):
        """爬取单条微博的评论"""
        try:
            # 准备保存评论的文件
            comment_file = os.path.join(os.path.dirname(self._get_filepath('csv')), f'comments_{weibo_id}.csv')
            logger.info(f"评论将保存到文件: {comment_file}")
            
            # 如果文件已存在，先删除
            if os.path.exists(comment_file):
                os.remove(comment_file)
            
            with open(comment_file, mode='w', encoding='utf-8-sig', newline='') as f:
                csv_writer = csv.writer(f)
                csv_writer.writerow(['评论编号', '用户ID', '昵称', '内容', '点赞数', '发布时间', '设备', 'IP属地'])

                # 获取总页数
                logger.info(f"正在获取微博 {weibo_number} 的评论总页数...")
                try:
                    response = self.session.get(f"{weibo_url}?page=1", 
                                             headers=self.comment_headers,
                                             timeout=15)
                    response.raise_for_status()
                    soup = BeautifulSoup(response.text, 'lxml')
                    pagination = soup.find('div', id='pagelist')
                    if pagination:
                        input_tag = pagination.find('input', {'name': 'mp'})
                        total_pages = int(input_tag['value']) if input_tag else 1
                    else:
                        total_pages = 1
                    logger.info(f"获取到总页数: {total_pages}")
                except Exception as e:
                    logger.error(f"获取总页数失败: {str(e)}")
                    total_pages = 1

                # 使用配置的评论页数
                total_pages = min(self.max_comment_pages, total_pages)
                logger.info(f"微博 {weibo_number} 将爬取前 {total_pages} 页评论")

                comment_counter = 0  # 评论计数器
                for page in range(1, total_pages + 1):
                    url = f"{weibo_url}?page={page}"
                    logger.info(f"正在爬取微博 {weibo_number} 的第 {page}/{total_pages} 页评论")

                    try:
                        # 随机延时
                        self._random_delay()
                        
                        # 轮换User-Agent
                        self._rotate_user_agent()
                        logger.info(f"使用User-Agent: {self.comment_headers['user-agent']}")

                        logger.info(f"发送请求: {url}")
                        response = self.session.get(url,
                                                headers=self.comment_headers,
                                                timeout=15)
                        response.raise_for_status()
                        logger.info(f"请求成功，状态码: {response.status_code}")

                        # 解析评论
                        soup = BeautifulSoup(response.text, 'lxml')
                        comments = soup.find_all('div', class_='c', id=lambda x: x and x.startswith('C_'))
                        logger.info(f"找到 {len(comments)} 条评论")

                        if not comments:
                            logger.warning(f"微博 {weibo_number} 第 {page} 页未找到评论，可能触发反爬！")
                            self._handle_request_error(Exception("未找到评论"))
                            continue

                        for comment in comments:
                            try:
                                comment_counter += 1
                                comment_number = f"{weibo_number}-{comment_counter}"  # 生成评论编号
                                
                                # 用户信息
                                user_link = comment.find('a', href=lambda x: x and '/u/' in x)
                                user_id = user_link['href'].split('/')[-1] if user_link else 'N/A'
                                screen_name = user_link.text if user_link else 'N/A'

                                # 评论内容
                                content = comment.find('span', class_='ctt').text.strip() if comment.find('span',
                                                                                                      class_='ctt') else 'N/A'

                                # 元数据解析
                                info_text = comment.find('span', class_='ct').text if comment.find('span', class_='ct') else ''
                                like_info = comment.find('a', string=lambda t: t and '赞[' in t)
                                likes = like_info.text.replace('赞[', '').replace(']', '') if like_info else '0'

                                # 分离时间和设备信息
                                time_source = info_text.split('\xa0')[0] if info_text else 'N/A'
                                device = info_text.split('来自')[-1].split('\xa0')[0] if '来自' in info_text else 'N/A'
                                ip_location = info_text.split('\xa0')[-1] if len(info_text.split('\xa0')) > 1 else 'N/A'

                                csv_writer.writerow([comment_number, user_id, screen_name, content, likes, time_source, device, ip_location])
                                logger.info(f"已保存评论 {comment_number}")
                            except Exception as e:
                                logger.error(f"解析评论时出错: {str(e)}")
                                continue
                        
                        logger.info(f"成功保存 {comment_counter} 条评论")

                        # 重置失败计数
                        self.fail_count = 0

                    except Exception as e:
                        self._handle_request_error(e)
                        continue

            logger.info(f"微博 {weibo_number} 的评论爬取完成，共爬取 {total_pages} 页")

        except Exception as e:
            logger.error(f"爬取微博 {weibo_number} 的评论时出错: {str(e)}")

    def write_weibo(self, weibos):
        """将爬取到的信息写入文件或数据库"""
        for writer in self.writers:
            writer.write_weibo(weibos)
        for downloader in self.downloaders:
            downloader.download_files(weibos)
            
        # 爬取每条微博的评论
        for weibo in weibos:
            try:
                # 构造评论URL
                comment_url = f'https://weibo.cn/comment/{weibo.id}'
                logger.info(f"开始爬取微博 {weibo.weibo_number} 的评论")
                logger.info(f"评论URL: {comment_url}")
                
                # 爬取评论
                self.crawl_comments(comment_url, weibo.id, weibo.weibo_number)
                
                # 随机延时
                sleep_time = random.uniform(3, 8)
                logger.info(f"评论爬取完成，等待 {sleep_time:.2f} 秒后继续")
                time.sleep(sleep_time)
                
            except Exception as e:
                logger.error(f"爬取微博 {weibo.weibo_number} 的评论时出错: {str(e)}")
                continue

    def write_user(self, user):
        """将用户信息写入数据库"""
        for writer in self.writers:
            writer.write_user(user)

    def get_user_info(self, user_uri):
        """获取用户信息"""
        self.user = IndexParser(self.cookie, user_uri).get_user()
        self.page_count += 1

    def download_user_avatar(self, user_uri):
        """下载用户头像"""
        avatar_album_url = PhotoParser(self.cookie,
                                       user_uri).extract_avatar_album_url()
        pic_urls = AlbumParser(self.cookie,
                               avatar_album_url).extract_pic_urls()
        AvatarPictureDownloader(
            self._get_filepath('img'),
            self.file_download_timeout).handle_download(pic_urls)

    def get_weibo_info(self):
        """获取微博信息"""
        try:
            since_date = datetime_util.str_to_time(
                self.user_config['since_date'])
            now = datetime.now()
            if since_date <= now:
                page_num = IndexParser(
                    self.cookie,
                    self.user_config['user_uri']).get_page_num()  # 获取微博总页数
                self.page_count += 1
                if self.page_count > 2 and (self.page_count +
                                            page_num) > self.global_wait[0][0]:
                    wait_seconds = int(
                        self.global_wait[0][1] *
                        min(1, self.page_count / self.global_wait[0][0]))
                    logger.info(u'即将进入全局等待时间，%d秒后程序继续执行' % wait_seconds)
                    for i in tqdm(range(wait_seconds)):
                        sleep(1)
                    self.page_count = 0
                    self.global_wait.append(self.global_wait.pop(0))
                page1 = 0
                random_pages = random.randint(*self.random_wait_pages)
                # 使用配置的微博页数
                max_pages = min(self.max_weibo_pages, page_num)
                for page in tqdm(range(1, max_pages + 1), desc='Progress'):
                    weibos, self.weibo_id_list, to_continue = PageParser(
                        self.cookie,
                        self.user_config, page, self.filter).get_one_page(
                            self.weibo_id_list)  # 获取第page页的全部微博
                    logger.info(
                        u'%s已获取%s(%s)的第%d页微博%s',
                        '-' * 30,
                        self.user.nickname,
                        self.user.id,
                        page,
                        '-' * 30,
                    )
                    self.page_count += 1
                    if weibos:
                        # 为每条微博添加编号
                        for weibo in weibos:
                            self.weibo_counter += 1
                            weibo.weibo_number = self.weibo_counter
                        yield weibos
                    if not to_continue:
                        break

                    # 通过加入随机等待避免被限制
                    if (page - page1) % random_pages == 0 and page < page_num:
                        sleep(random.randint(*self.random_wait_seconds))
                        page1 = page
                        random_pages = random.randint(*self.random_wait_pages)

                    if self.page_count >= self.global_wait[0][0]:
                        logger.info(u'即将进入全局等待时间，%d秒后程序继续执行' %
                                    self.global_wait[0][1])
                        for i in tqdm(range(self.global_wait[0][1])):
                            sleep(1)
                        self.page_count = 0
                        self.global_wait.append(self.global_wait.pop(0))

                # 更新用户user_id_list.txt中的since_date
                if self.user_config_file_path or FLAGS.u:
                    config_util.update_user_config_file(
                        self.user_config_file_path,
                        self.user_config['user_uri'],
                        self.user.nickname,
                        self.new_since_date,
                    )
        except Exception as e:
            logger.exception(e)

    def _get_filepath(self, type):
        """获取结果文件路径"""
        try:
            dir_name = self.user.nickname
            if self.result_dir_name:
                dir_name = self.user.id
            if FLAGS.output_dir is not None:
                file_dir = FLAGS.output_dir + os.sep + dir_name
            else:
                file_dir = (os.getcwd() + os.sep + 'weibo' + os.sep + dir_name)
            if type == 'img' or type == 'video':
                file_dir = file_dir + os.sep + type
            if not os.path.isdir(file_dir):
                os.makedirs(file_dir)
            if type == 'img' or type == 'video':
                return file_dir
            file_path = file_dir + os.sep + self.user.id + '.' + type
            # 如果文件已存在，先删除
            if os.path.exists(file_path):
                os.remove(file_path)
            return file_path
        except Exception as e:
            logger.exception(e)

    def initialize_info(self, user_config):
        """初始化爬虫信息"""
        self.got_num = 0
        self.user_config = user_config
        self.weibo_id_list = []
        if self.end_date == 'now':
            self.new_since_date = datetime.now().strftime('%Y-%m-%d %H:%M')
        else:
            self.new_since_date = self.end_date
        self.writers = []
        if 'csv' in self.write_mode:
            from .writer import CsvWriter

            self.writers.append(
                CsvWriter(self._get_filepath('csv'), self.filter))
        if 'txt' in self.write_mode:
            from .writer import TxtWriter

            self.writers.append(
                TxtWriter(self._get_filepath('txt'), self.filter))
        if 'json' in self.write_mode:
            from .writer import JsonWriter

            self.writers.append(JsonWriter(self._get_filepath('json')))
        if 'mysql' in self.write_mode:
            from .writer import MySqlWriter

            self.writers.append(MySqlWriter(self.mysql_config))
        if 'mongo' in self.write_mode:
            from .writer import MongoWriter

            self.writers.append(MongoWriter(self.mongo_config))
        if 'sqlite' in self.write_mode:
            from .writer import SqliteWriter

            self.writers.append(SqliteWriter(self.sqlite_config))

        if 'kafka' in self.write_mode:
            from .writer import KafkaWriter

            self.writers.append(KafkaWriter(self.kafka_config))

        self.downloaders = []
        if self.pic_download == 1:
            from .downloader import (OriginPictureDownloader,
                                     RetweetPictureDownloader)

            self.downloaders.append(
                OriginPictureDownloader(self._get_filepath('img'),
                                        self.file_download_timeout))
        if self.pic_download and not self.filter:
            self.downloaders.append(
                RetweetPictureDownloader(self._get_filepath('img'),
                                         self.file_download_timeout))
        if self.video_download == 1:
            from .downloader import VideoDownloader

            self.downloaders.append(
                VideoDownloader(self._get_filepath('video'),
                                self.file_download_timeout))

    def get_one_user(self, user_config):
        """获取一个用户的微博"""
        try:
            self.get_user_info(user_config['user_uri'])
            logger.info(self.user)
            logger.info('*' * 100)

            self.initialize_info(user_config)
            self.write_user(self.user)
            logger.info('*' * 100)

            # 下载用户头像相册中的图片。
            if self.pic_download:
                self.download_user_avatar(user_config['user_uri'])

            for weibos in self.get_weibo_info():
                self.write_weibo(weibos)
                self.got_num += len(weibos)
            if not self.filter:
                logger.info(u'共爬取' + str(self.got_num) + u'条微博')
            else:
                logger.info(u'共爬取' + str(self.got_num) + u'条原创微博')
            logger.info(u'信息抓取完毕')
            logger.info('*' * 100)
        except Exception as e:
            logger.exception(e)

    def start(self):
        """运行爬虫"""
        try:
            if not self.user_config_list:
                logger.info(
                    u'没有配置有效的user_id，请通过config.json或user_id_list.txt配置user_id')
                return
            user_count = 0
            user_count1 = random.randint(*self.random_wait_pages)
            random_users = random.randint(*self.random_wait_pages)
            for user_config in self.user_config_list:
                if (user_count - user_count1) % random_users == 0:
                    sleep(random.randint(*self.random_wait_seconds))
                    user_count1 = user_count
                    random_users = random.randint(*self.random_wait_pages)
                user_count += 1
                self.get_one_user(user_config)
        except Exception as e:
            logger.exception(e)


def _get_config():
    """获取config.json数据"""
    src = os.path.split(
        os.path.realpath(__file__))[0] + os.sep + 'config_sample.json'
    config_path = os.getcwd() + os.sep + 'config.json'
    if FLAGS.config_path:
        config_path = FLAGS.config_path
    elif not os.path.isfile(config_path):
        shutil.copy(src, config_path)
        logger.info(u'请先配置当前目录(%s)下的config.json文件，'
                    u'如果想了解config.json参数的具体意义及配置方法，请访问\n'
                    u'https://github.com/dataabc/weiboSpider#2程序设置' %
                    os.getcwd())
        sys.exit()
    try:
        with open(config_path) as f:
            config = json.loads(f.read())
            return config
    except ValueError:
        logger.error(u'config.json 格式不正确，请访问 '
                     u'https://github.com/dataabc/weiboSpider#2程序设置')
        sys.exit()


def main(_):
    try:
        config = _get_config()
        config_util.validate_config(config)
        wb = Spider(config)
        wb.start()  # 爬取微博信息
    except Exception as e:
        logger.exception(e)


if __name__ == '__main__':
    app.run(main)
