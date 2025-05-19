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


def weibo_comment_spider():
    ua = UserAgent()
    base_url = 'https://weibo.cn/comment/PoUl5taJ0'

    # 动态请求头
    headers = {
        'cookie': clean_header_value('_T_WM=f015bcca5926222038b9468e871d7599; SCF=AsT4HpwbDNh--yNyJpth6yy21afWGTE2rD4LgHMnxC7daQ0BV8FFreFCIPm-k3OSIPkfp2K5kiutEj1X5QwPGpI.; SUB=_2A25FANInDeRhGeFH6lUX9S3MzTWIHXVmfGvvrDV6PUJbktAYLUffkW1Ne8WURz2eDzXjNE03a1QOs6_2RY0G_hQW; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9W53edGQQErfkvqsoe8jXkxH5NHD95QN1K2NSo-0ehq4Ws4DqcjMi--NiK.Xi-2Ri--ciKnRi-zNS0.pS0qfe05c1Btt; ALF=1747726199'),  # 需要更新为有效Cookie
        'referer': 'https://weibo.cn/u/2803301701',
        'user-agent': ua.random
    }

    # 代理池示例（需要替换为有效代理）
    proxies = [
        {'http': 'http://123.45.67.89:8080'},
        {'http': 'http://112.98.90.12:3128'},
        {'http': 'http://183.222.102.109:80'}
    ]

    with open('weibo_comments.csv', mode='a', encoding='utf-8-sig', newline='') as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow(['用户ID', '昵称', '内容', '点赞数', '发布时间', '设备', 'IP属地'])

        # 获取总页数
        total_pages = get_total_pages(f"{base_url}?page=1&uid=2803301701", headers)
        print(f"共发现 {total_pages} 页评论")

        for page in range(1, total_pages + 1):
            url = f"{base_url}?page={page}&uid=2803301701"
            print(f"正在爬取第 {page}/{total_pages} 页")

            try:
                # 随机代理和延时
                proxy = random.choice(proxies)
                response = requests.get(url,
                                        headers=headers,
                                        proxies=proxy,
                                        timeout=15)
                response.raise_for_status()

                # 使用更健壮的解析方式
                soup = BeautifulSoup(response.text, 'lxml')
                comments = soup.find_all('div', class_='c', id=lambda x: x and x.startswith('C_'))

                if not comments:
                    print(f"第 {page} 页未找到评论，可能触发反爬！")
                    break

                for comment in comments:
                    try:
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

                        csv_writer.writerow([user_id, screen_name, content, likes, time_source, device, ip_location])
                    except Exception as e:
                        print(f"解析评论时出错: {str(e)}")
                        continue

                # 动态延时（3-8秒随机）
                sleep_time = random.uniform(3, 8)
                time.sleep(sleep_time)

            except Exception as e:
                print(f"请求第 {page} 页失败: {str(e)}")
                # 切换UserAgent
                headers['user-agent'] = ua.random
                continue


if __name__ == '__main__':
    weibo_comment_spider()
    print("爬取完成！结果已保存到 weibo_comments.csv")