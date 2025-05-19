import requests
import time
import random
import logging
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
import threading
import re
from bs4 import BeautifulSoup

class ProxyPool:
    def __init__(self):
        self.proxy_queue = Queue()
        self.valid_proxies = set()
        self.lock = threading.Lock()
        self.logger = logging.getLogger('spider')
        
        # 本地代理配置
        self.local_proxies = [
            'http://127.0.0.1:7890',  # Clash默认端口
            'http://127.0.0.1:1080',  # V2Ray默认端口
            'http://127.0.0.1:8080'   # 其他代理软件
        ]
        
        self.test_urls = [
            'https://weibo.cn',
            'https://weibo.com'
        ]
        
        # 代理池配置
        self.min_proxies = 1  # 降低最小代理数量
        self.max_proxies = 3  # 降低最大代理数量
        self.update_interval = 60  # 更新间隔（秒）
        self.test_timeout = 10  # 测试超时时间（秒）
        self.max_retries = 2  # 最大重试次数
        self.last_update = 0
        
        # 初始化时添加本地代理
        self._add_local_proxies()
        
        # 启动代理池维护线程
        self.maintain_thread = threading.Thread(target=self._maintain_proxy_pool, daemon=True)
        self.maintain_thread.start()
        
        # 立即进行一次代理池更新
        self._update_proxy_pool()
        
        # 打印代理池状态
        self.print_status()

    def print_status(self):
        """打印代理池状态"""
        with self.lock:
            print("\n=== 代理池状态 ===")
            print(f"当前有效代理数量: {len(self.valid_proxies)}")
            print(f"代理队列大小: {self.proxy_queue.qsize()}")
            print("\n当前可用代理列表:")
            for proxy in self.valid_proxies:
                print(f"- {proxy}")
            print("\n代理源列表:")
            for source in self.local_proxies:
                print(f"- {source}")
            print("\n测试URL列表:")
            for url in self.test_urls:
                print(f"- {url}")
            print("\n代理池配置:")
            print(f"- 最小代理数量: {self.min_proxies}")
            print(f"- 最大代理数量: {self.max_proxies}")
            print(f"- 更新间隔: {self.update_interval}秒")
            print(f"- 测试超时: {self.test_timeout}秒")
            print(f"- 最大重试次数: {self.max_retries}")
            print("================\n")

    def _add_local_proxies(self):
        """添加本地代理到代理池"""
        # 直接使用无代理模式
        self.valid_proxies.add(None)
        self.proxy_queue.put(None)
        self.logger.info("使用无代理模式")

    def _fetch_proxies_from_source(self, source_url):
        """从代理源获取代理"""
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                'Connection': 'keep-alive',
                'Cache-Control': 'no-cache',
                'Pragma': 'no-cache'
            }
            response = requests.get(source_url, headers=headers, timeout=10)
            if response.status_code == 200:
                # 使用BeautifulSoup解析HTML
                soup = BeautifulSoup(response.text, 'lxml')
                
                # 根据不同代理源使用不同的解析方法
                if '89ip.cn' in source_url:
                    return self._parse_89ip(soup)
                elif '66ip.cn' in source_url:
                    return self._parse_66ip(soup)
                elif 'kuaidaili.com' in source_url:
                    return self._parse_kuaidaili(soup)
                elif 'ip3366.net' in source_url:
                    return self._parse_ip3366(soup)
                elif 'seofangfa.com' in source_url:
                    return self._parse_seofangfa(soup)
                elif '7yip.cn' in source_url:
                    return self._parse_7yip(soup)
                elif 'data5u.com' in source_url:
                    return self._parse_data5u(soup)
                elif 'zdaye.com' in source_url:
                    return self._parse_zdaye(soup)
                elif 'iphai.com' in source_url:
                    return self._parse_iphai(soup)
                elif 'xiladaili.com' in source_url:
                    return self._parse_xiladaili(soup)
                else:
                    # 默认使用正则表达式提取
                    ip_port_pattern = r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}):(\d+)'
                    proxies = re.findall(ip_port_pattern, response.text)
                    return [f'http://{ip}:{port}' for ip, port in proxies]
        except Exception as e:
            self.logger.error(f"从 {source_url} 获取代理失败: {str(e)}")
        return []

    def _parse_89ip(self, soup):
        """解析89代理"""
        proxies = []
        for tr in soup.find_all('tr')[1:]:
            tds = tr.find_all('td')
            if len(tds) >= 2:
                ip = tds[0].text.strip()
                port = tds[1].text.strip()
                proxies.append(f'http://{ip}:{port}')
        return proxies

    def _parse_66ip(self, soup):
        """解析66代理"""
        proxies = []
        for tr in soup.find_all('tr')[1:]:
            tds = tr.find_all('td')
            if len(tds) >= 2:
                ip = tds[0].text.strip()
                port = tds[1].text.strip()
                proxies.append(f'http://{ip}:{port}')
        return proxies

    def _parse_kuaidaili(self, soup):
        """解析快代理"""
        proxies = []
        for tr in soup.find_all('tr')[1:]:
            tds = tr.find_all('td')
            if len(tds) >= 2:
                ip = tds[0].text.strip()
                port = tds[1].text.strip()
                proxies.append(f'http://{ip}:{port}')
        return proxies

    def _parse_ip3366(self, soup):
        """解析云代理"""
        proxies = []
        for tr in soup.find_all('tr')[1:]:
            tds = tr.find_all('td')
            if len(tds) >= 2:
                ip = tds[0].text.strip()
                port = tds[1].text.strip()
                proxies.append(f'http://{ip}:{port}')
        return proxies

    def _parse_seofangfa(self, soup):
        """解析小幻代理"""
        proxies = []
        for tr in soup.find_all('tr')[1:]:
            tds = tr.find_all('td')
            if len(tds) >= 2:
                ip = tds[0].text.strip()
                port = tds[1].text.strip()
                proxies.append(f'http://{ip}:{port}')
        return proxies

    def _parse_7yip(self, soup):
        """解析齐云代理"""
        proxies = []
        for tr in soup.find_all('tr')[1:]:
            tds = tr.find_all('td')
            if len(tds) >= 2:
                ip = tds[0].text.strip()
                port = tds[1].text.strip()
                proxies.append(f'http://{ip}:{port}')
        return proxies

    def _parse_data5u(self, soup):
        """解析无忧代理"""
        proxies = []
        for tr in soup.find_all('tr')[1:]:
            tds = tr.find_all('td')
            if len(tds) >= 2:
                ip = tds[0].text.strip()
                port = tds[1].text.strip()
                proxies.append(f'http://{ip}:{port}')
        return proxies

    def _parse_zdaye(self, soup):
        """解析站大爷"""
        proxies = []
        for tr in soup.find_all('tr')[1:]:
            tds = tr.find_all('td')
            if len(tds) >= 2:
                ip = tds[0].text.strip()
                port = tds[1].text.strip()
                proxies.append(f'http://{ip}:{port}')
        return proxies

    def _parse_iphai(self, soup):
        """解析IP海"""
        proxies = []
        for tr in soup.find_all('tr')[1:]:
            tds = tr.find_all('td')
            if len(tds) >= 2:
                ip = tds[0].text.strip()
                port = tds[1].text.strip()
                proxies.append(f'http://{ip}:{port}')
        return proxies

    def _parse_xiladaili(self, soup):
        """解析西拉代理"""
        proxies = []
        for tr in soup.find_all('tr')[1:]:
            tds = tr.find_all('td')
            if len(tds) >= 2:
                ip = tds[0].text.strip()
                port = tds[1].text.strip()
                proxies.append(f'http://{ip}:{port}')
        return proxies

    def _test_proxy(self, proxy):
        """测试代理是否可用"""
        for _ in range(self.max_retries):
            try:
                proxies = {
                    'http': proxy,
                    'https': proxy
                }
                test_url = random.choice(self.test_urls)
                print(f"测试代理 {proxy} 访问 {test_url}")
                response = requests.get(test_url, proxies=proxies, timeout=self.test_timeout)
                if response.status_code == 200:
                    # 额外检查是否能够访问微博
                    if 'weibo' in test_url:
                        print(f"代理 {proxy} 测试成功")
                        return True
                    # 如果不是微博域名，再测试一次微博
                    test_weibo = requests.get('https://weibo.cn', proxies=proxies, timeout=self.test_timeout)
                    if test_weibo.status_code == 200:
                        print(f"代理 {proxy} 测试成功")
                        return True
            except Exception as e:
                print(f"代理 {proxy} 测试失败: {str(e)}")
                continue
        return False

    def _update_proxy_pool(self):
        """更新代理池，优先使用本地代理"""
        current_time = time.time()
        if current_time - self.last_update < self.update_interval:
            return
        
        self.last_update = current_time
        print("\n开始更新代理池...")
        
        # 清空现有代理
        self.valid_proxies.clear()
        while not self.proxy_queue.empty():
            self.proxy_queue.get()
        
        # 添加本地代理
        self._add_local_proxies()
        
        # 如果本地代理都不可用，则不使用代理
        if not self.valid_proxies:
            print("所有本地代理都不可用，将不使用代理")
            self.valid_proxies.add(None)
            self.proxy_queue.put(None)
        
        self.print_status()

    def _maintain_proxy_pool(self):
        """维护代理池的线程函数"""
        while True:
            try:
                # 如果代理数量小于最小值，立即更新
                if len(self.valid_proxies) < self.min_proxies:
                    self._update_proxy_pool()
                else:
                    # 否则按正常间隔更新
                    time.sleep(self.update_interval)
                    self._update_proxy_pool()
            except Exception as e:
                self.logger.error(f"维护代理池时出错: {str(e)}")
                time.sleep(60)

    def get_proxy(self):
        """获取一个代理"""
        try:
            proxy = self.proxy_queue.get(timeout=1)
            self.proxy_queue.put(proxy)  # 放回队列
            return {'http': proxy, 'https': proxy}
        except:
            return None

    def remove_proxy(self, proxy):
        """移除无效代理"""
        with self.lock:
            if proxy in self.valid_proxies:
                self.valid_proxies.remove(proxy)
                self.logger.info(f"移除无效代理: {proxy}")

# 创建全局代理池实例
proxy_pool = ProxyPool() 