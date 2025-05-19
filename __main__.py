import os
import sys

from absl import app
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from weibo_spider.spider import main
import requests
from bs4 import XMLParsedAsHTMLWarning
import warnings
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
from bs4 import BeautifulSoup
import csv
import time
import random
from fake_useragent import UserAgent


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
        print(f"获取总页数失败: {str(e)}")
        return 1


def clean_header_value(value: str) -> str:
    """清理请求头中的非法字符"""
    return value.strip().replace('\ufeff', '').replace('\xa0', ' ')


def test_proxy(proxy, url, headers):
    """测试代理是否可用"""
    try:
        response = requests.get(url, headers=headers, proxies=proxy, timeout=5)
        return response.status_code == 200
    except:
        return False


def get_working_proxies(proxies, url, headers):
    """获取可用的代理列表"""
    working_proxies = []
    print("正在测试代理...")
    for proxy in proxies:
        if test_proxy(proxy, url, headers):
            working_proxies.append(proxy)
            print(f"代理可用: {proxy}")
    print(f"找到 {len(working_proxies)} 个可用代理")
    return working_proxies


def weibo_comment_spider(max_pages=1):
    """
    爬取微博内容和评论
    :param max_pages: 要爬取的微博页数
    """
    ua = UserAgent()
    user_id = '2803301701'  # 用户ID
    base_url = f'https://weibo.cn/u/{user_id}'

    # 动态请求头
    headers = {
        'cookie': clean_header_value('_T_WM=9c39d290bfa64125e019384e85963e85; SCF=AvU7eNNEzGbZUzN43rulHcmv3fiZUC4jI0u4YrrSnKbY_7gzPKxQ_7GhzAqgCE6ZqA0MVgyamxhBXolUQMV9Kac.; SUB=_2A25FLaO1DeRhGeFH6lUX9S3MzTWIHXVmQrl9rDV6PUJbktAYLUXakW1Ne8WUR4AasxIi6Wto-sM99U45ZCg12Wlb; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9W53edGQQErfkvqsoe8jXkxH5JpX5KMhUgL.FoM4eKMcSKe7So.2dJLoIp7LxKML1KBLBKnLxKqL1hnLBoMN1K2NSo-0ehq4; SSOLoginState=1747571685; ALF=1750163685'),
        'referer': f'https://weibo.cn/u/{user_id}',
        'user-agent': ua.random
    }

    # 扩展代理池
    proxies = [
        # HTTP代理
        {'http': 'http://45.173.132.1:9292'},
        {'http': 'http://201.71.2.129:999'},
        {'http': 'http://181.65.200.53:80'},
        {'http': 'http://45.81.146.7:8080'},
        {'http': 'http://128.0.179.234:41258'},
        {'http': 'http://41.65.67.167:1976'},
        {'http': 'http://211.219.51.199:3128'},
        {'http': 'http://177.93.37.35:999'},
        {'http': 'http://12.88.29.66:9080'},
        {'http': 'http://112.196.112.243:80'},
        {'http': 'http://103.62.237.102:8080'},
        {'http': 'http://103.169.254.164:8061'},
        {'http': 'http://103.130.175.169:8080'},
        {'http': 'http://45.225.207.180:999'},
        {'http': 'http://194.186.35.70:3128'}
    ]

    # 测试并获取可用代理
    working_proxies = get_working_proxies(proxies, 'https://weibo.cn', headers)
    if not working_proxies:
        print("没有可用的代理，将使用直接连接")
        working_proxies = [None]

    # 创建微博内容文件
    with open('weibo_contents.csv', mode='w', encoding='utf-8-sig', newline='') as weibo_file:
        weibo_writer = csv.writer(weibo_file)
        weibo_writer.writerow(['微博ID', '微博编号', '内容', '发布时间', '转发数', '评论数', '点赞数'])

        # 创建评论文件
        with open('weibo_comments.csv', mode='w', encoding='utf-8-sig', newline='') as comment_file:
            comment_writer = csv.writer(comment_file)
            comment_writer.writerow(['评论ID', '微博编号', '评论编号', '用户ID', '昵称', '内容', '点赞数', '发布时间', '设备', 'IP属地'])

            # 获取微博列表
            page = 1
            weibo_count = 1  # 微博编号从1开始
            
            while page <= max_pages:
                try:
                    # 获取微博列表页
                    url = f"{base_url}?page={page}"
                    print(f"正在获取第 {page} 页微博列表")
                    
                    # 随机选择代理
                    proxy = random.choice(working_proxies)
                    print(f"使用代理: {proxy}")
                    
                    response = requests.get(url, headers=headers, proxies=proxy, timeout=15)
                    print(f"请求状态码: {response.status_code}")
                    
                    if response.status_code != 200:
                        print(f"请求失败，状态码: {response.status_code}")
                        continue
                        
                    soup = BeautifulSoup(response.text, 'lxml')
                    
                    # 获取当前页的所有微博
                    weibo_list = soup.find_all('div', class_='c', id=lambda x: x and x.startswith('M_'))
                    
                    if not weibo_list:
                        print("没有更多微博了")
                        break

                    print(f"找到 {len(weibo_list)} 条微博")
                    
                    for weibo in weibo_list:
                        try:
                            # 获取微博ID
                            weibo_id = weibo.get('id', '').replace('M_', '')
                            print(f"正在处理微博 {weibo_id}")
                            
                            # 获取微博内容
                            weibo_text = weibo.find('span', class_='ctt').text.strip() if weibo.find('span', class_='ctt') else 'N/A'
                            
                            # 获取发布时间
                            info_text = weibo.find('span', class_='ct').text if weibo.find('span', class_='ct') else ''
                            time_source = info_text.split('\xa0')[0] if info_text else 'N/A'
                            
                            # 获取转发、评论、点赞数
                            stats = weibo.find_all('a', href=lambda x: x and ('repost' in x or 'comment' in x or 'like' in x))
                            repost_count = stats[0].text.replace('转发[', '').replace(']', '') if len(stats) > 0 else '0'
                            comment_count = stats[1].text.replace('评论[', '').replace(']', '') if len(stats) > 1 else '0'
                            like_count = stats[2].text.replace('赞[', '').replace(']', '') if len(stats) > 2 else '0'
                            
                            # 写入微博内容
                            weibo_writer.writerow([
                                weibo_id,
                                weibo_count,
                                weibo_text,
                                time_source,
                                repost_count,
                                comment_count,
                                like_count
                            ])
                            
                            # 获取该微博的评论
                            comment_url = f'https://weibo.cn/comment/{weibo_id}'
                            comment_page = 1
                            comment_count = 1  # 评论编号从1开始
                            
                            # 获取评论总页数
                            total_comment_pages = get_total_pages(comment_url, headers)
                            print(f"微博 {weibo_id} 共有 {total_comment_pages} 页评论")
                            
                            while comment_page <= total_comment_pages:
                                try:
                                    print(f"正在获取微博 {weibo_count} 的第 {comment_page} 页评论")
                                    comment_response = requests.get(
                                        f"{comment_url}?page={comment_page}",
                                        headers=headers,
                                        proxies=random.choice(working_proxies),
                                        timeout=15
                                    )
                                    
                                    if comment_response.status_code != 200:
                                        print(f"获取评论失败，状态码: {comment_response.status_code}")
                                        continue
                                        
                                    comment_soup = BeautifulSoup(comment_response.text, 'lxml')
                                    comments = comment_soup.find_all('div', class_='c', id=lambda x: x and x.startswith('C_'))
                                    
                                    if not comments:
                                        print(f"微博 {weibo_count} 的评论获取完成")
                                        break
                                    
                                    print(f"找到 {len(comments)} 条评论")
                                    
                                    for comment in comments:
                                        try:
                                            # 用户信息
                                            user_link = comment.find('a', href=lambda x: x and '/u/' in x)
                                            user_id = user_link['href'].split('/')[-1] if user_link else 'N/A'
                                            screen_name = user_link.text if user_link else 'N/A'
                                            
                                            # 评论内容
                                            content = comment.find('span', class_='ctt').text.strip() if comment.find('span', class_='ctt') else 'N/A'
                                            
                                            # 元数据解析
                                            info_text = comment.find('span', class_='ct').text if comment.find('span', class_='ct') else ''
                                            like_info = comment.find('a', string=lambda t: t and '赞[' in t)
                                            likes = like_info.text.replace('赞[', '').replace(']', '') if like_info else '0'
                                            
                                            # 分离时间和设备信息
                                            time_source = info_text.split('\xa0')[0] if info_text else 'N/A'
                                            device = info_text.split('来自')[-1].split('\xa0')[0] if '来自' in info_text else 'N/A'
                                            ip_location = info_text.split('\xa0')[-1] if len(info_text.split('\xa0')) > 1 else 'N/A'
                                            
                                            # 写入评论
                                            comment_writer.writerow([
                                                comment.get('id', 'N/A'),
                                                weibo_count,
                                                comment_count,
                                                user_id,
                                                screen_name,
                                                content,
                                                likes,
                                                time_source,
                                                device,
                                                ip_location
                                            ])
                                            comment_count += 1
                                        except Exception as e:
                                            print(f"解析评论时出错: {str(e)}")
                                            continue
                                    
                                    # 评论页延时
                                    sleep_time = random.uniform(3, 8)
                                    print(f"等待 {sleep_time:.2f} 秒后继续...")
                                    time.sleep(sleep_time)
                                    comment_page += 1
                                    
                                except Exception as e:
                                    print(f"获取评论失败: {str(e)}")
                                    # 切换UserAgent和代理
                                    headers['user-agent'] = ua.random
                                    continue
                            
                            weibo_count += 1
                            
                        except Exception as e:
                            print(f"解析微博时出错: {str(e)}")
                            continue
                    
                    # 微博列表页延时
                    sleep_time = random.uniform(3, 8)
                    print(f"等待 {sleep_time:.2f} 秒后继续...")
                    time.sleep(sleep_time)
                    page += 1
                    
                except Exception as e:
                    print(f"获取微博列表失败: {str(e)}")
                    # 切换UserAgent和代理
                    headers['user-agent'] = ua.random
                    continue


if __name__ == '__main__':
    # 设置要爬取的微博页数
    max_pages = 1  # 可以根据需要修改这个数字
    weibo_comment_spider(max_pages)
    print("爬取完成！结果已保存到 weibo_contents.csv 和 weibo_comments.csv")
app.run(main)
