"""
页面下载类 - 负责浏览器管理和HTML保存
"""
from __future__ import annotations
import random
import time
from pathlib import Path
from DrissionPage import ChromiumOptions, ChromiumPage
from config.config import Config

class Fetcher:
    """
    页面下载器
    职责：管理浏览器，获取页面，保存HTML到本地
    """

    def __init__(self, config: 'Config'):
        self.config = config
        self._page: ChromiumPage | None = None

    def _create_browser(self) -> ChromiumPage:
        """创建并配置浏览器"""
        co = ChromiumOptions()
        co.set_argument('--remote-debugging-port', self.config.DEBUG_PORT)
        co.set_argument('--user-data-dir', self.config.USER_DATA_DIR)
        co.set_argument('--disable-blink-features', 'AutomationControlled')
        return ChromiumPage(addr_or_opts=co)

    @property
    def page(self) -> ChromiumPage:
        """懒加载浏览器实例"""
        if self._page is None:
            self._page = self._create_browser()
        return self._page

    def fetch_and_save(self, url: str, idx: int) -> Path | None:
        """
        获取页面并保存HTML到本地

        Args:
            url: 目标网址
            idx: 任务序号（用于生成文件名）

        Returns:
            Path: HTML文件路径，失败返回None
        """
        try:
            # 1. 获取页面
            self.page.get(url)
            if not self.page.wait.ele_displayed(
                    'xpath://div[@class="gh-header__logo-cats-wrap"]',timeout=15):
                print(f'[Fetcher] 页面未加载完成: {url}')
                return None

            # 2. 获取HTML内容
            html_content = self.page.html

            # 3. 保存到本地文件
            cache_file = self.config.html_cache_dir / f'page_{idx:06d}.html'
            cache_file.write_text(html_content, encoding='utf-8')

            print(f'[Fetcher] ✓ HTML已保存: {cache_file.name}')
            return cache_file

        except Exception as e:
            print(f'[Fetcher] ✗ 获取失败: {e}')
            return None

    def delay(self) -> None:
        """随机延迟"""
        time.sleep(random.uniform(self.config.MIN_DELAY, self.config.MAX_DELAY))

    def close(self) -> None:
        """关闭浏览器"""
        if self._page:
            self._page.quit()
            self._page = None


class Fetcher_1:
    """
    页面下载器
    职责：管理浏览器，获取页面，保存HTML到本地
    """

    def __init__(self, config: 'Config'):
        self.config = config
        self._page: ChromiumPage | None = None

    def _create_browser(self) -> ChromiumPage:
        """创建并配置浏览器"""
        co = ChromiumOptions()
        co.set_argument('--remote-debugging-port', self.config.DEBUG_PORT)
        co.set_argument('--user-data-dir', self.config.USER_DATA_DIR)
        co.set_argument('--disable-blink-features', 'AutomationControlled')
        return ChromiumPage(addr_or_opts=co)

    @property
    def page(self) -> ChromiumPage:
        """懒加载浏览器实例"""
        if self._page is None:
            self._page = self._create_browser()
        return self._page

    def fetch_and_save(self, url: str, idx: int) -> Path | None:
        """
        获取页面并保存HTML到本地

        Args:
            url: 目标网址
            idx: 任务序号（用于生成文件名）

        Returns:
            Path: HTML文件路径，失败返回None
        """
        try:
            # 1. 获取页面
            self.page.get(url)
            if not self.page.wait.ele_displayed(
                    'xpath://div[@class="gh-header__logo-cats-wrap"]',timeout=15):
                print(f'[Fetcher] 页面未加载完成: {url}')
                return None

            # 2. 获取HTML内容
            html_content = self.page.html

            # 3. 保存到本地文件
            cache_file = self.config.html_cache_dir_1 / f'page_{idx:06d}.html'
            cache_file.write_text(html_content, encoding='utf-8')

            print(f'[Fetcher] ✓ HTML已保存: {cache_file.name}')
            return cache_file

        except Exception as e:
            print(f'[Fetcher] ✗ 获取失败: {e}')
            return None

    def delay(self) -> None:
        """随机延迟"""
        time.sleep(random.uniform(self.config.MIN_DELAY, self.config.MAX_DELAY))

    def close(self) -> None:
        """关闭浏览器"""
        if self._page:
            self._page.quit()
            self._page = None