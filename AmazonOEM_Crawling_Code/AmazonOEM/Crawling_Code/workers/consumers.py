"""
消费者类 - 从HTML文件读取并解析
"""

from __future__ import annotations
import json
import threading
import re
from typing import List, Dict, Callable
from pathlib import Path
from queue import Empty

from bs4 import BeautifulSoup  # 用BeautifulSoup解析静态HTML，避免DrissionPage并发问题

from core.page_queue import PageQueue
from model.dataclasses import PageTask

class BaseConsumer(threading.Thread):
    """消费者基类"""

    def __init__(self,
                 name: str,
                 page_queue: PageQueue,
                 result_callback: Callable[[Dict], None]):
        super().__init__(name=name, daemon=True)
        self.queue = page_queue
        self.save_result = result_callback
        self.processed = 0

    def parse(self, task: PageTask, soup: BeautifulSoup) -> Dict:
        """解析方法（子类实现）"""
        raise NotImplementedError

    def run(self) -> None:
        """主循环"""
        print(f'[{self.name}] 消费者启动')

        while True:
            task = None

            try:
                # 获取任务
                task = self.queue.get(block=True, timeout=5)

                # 毒丸信号
                if task is None:
                    print(f'[{self.name}] 收到毒丸，结束运行')
                    self.queue.task_done()
                    break

                print(f'[{self.name}] 处理任务 #{task.idx}: {task.html_file.name}')

                # 读取HTML文件
                html_content = task.html_file.read_text(encoding='utf-8')

                # 用BeautifulSoup解析（线程安全，不依赖浏览器）
                soup = BeautifulSoup(html_content, 'lxml')

                # 解析
                result = self.parse(task, soup)
                self.processed += 1

                # 保存结果
                self.save_result(result)

                print(f'[{self.name}] ✓ 完成 #{task.idx}')

                # 删除HTML文件（即用即删）
                try:
                    task.html_file.unlink()
                    print(f'[{self.name}]已删除缓存文件')
                except Exception as e:
                    print(f'[{self.name}]删除文件失败: {e}')

            except Empty:
                continue

            except Exception as e:
                print(f'[{self.name}]处理失败: {e}')
                import traceback
                traceback.print_exc()

                #url = f'https://www.ebay.com/sch/i.html?_nkw={OEM_No}'



                # 保存错误结果
                if task:
                    self.save_result({
                        'URL': task.url,
                        'Title': f'[ERROR: {str(e)}]',
                        'price': '',
                        'About_table': '',
                        'item specifics': '',
                        'description_from_the_seller': '',
                        'seller_feedback': '',
                        'picture': '',
                        '_idx': task.idx,
                        '_error': True
                    })

                    # 出错也尝试删除文件
                    try:
                        if task.html_file.exists():
                            task.html_file.unlink()
                    except:
                        pass


            finally:
                if task is not None and task != self.queue.POISON_PILL:
                    self.queue.task_done()

        print(f'[{self.name}] 消费者停止，共处理 {self.processed} 个任务')


class FullParserConsumer(BaseConsumer):
        """全量解析消费者 - 用BeautifulSoup解析"""

        def __init__(self, page_queue: PageQueue, result_callback: Callable):
            super().__init__('FullParser', page_queue, result_callback)

        @staticmethod
        def safe_text(ele, default: str = '') -> str:
            """安全获取文本"""
            try:
                return ele.get_text(strip=True) if ele else default
            except Exception:
                return default

        @staticmethod
        def safe_attr(ele, attr: str, default: str = '') -> str:
            """安全获取属性"""
            try:
                return ele.get(attr, default) if ele else default
            except Exception:
                return default

        def parse(self, task: PageTask, soup: BeautifulSoup) -> Dict:
            oem_no = getattr(task, 'oem_No', '')

            containers = soup.select('div.a-section.a-spacing-base.desktop-grid-content-view')

            # 2. 方法2失败，降级方法1：直接找 search-result
            if len(containers) < 1:
                cards = soup.select('[data-component-type="s-search-result"]')
                print(f"[Parser] 降级到方法1，找到 {len(cards)} 个卡片")
            else:
                # 方法2：在容器里找卡片（也可以直接用容器本身）
                cards = containers
                print(f"[Parser] 方法2找到 {len(cards)} 个容器")

            # 3. 收集 href
            urls = [
                (href if href.startswith(
                    'http') else f"https://www.amazon.com{href if href.startswith('/') else '/' + href}")
                for c in cards
                if (link := c.select_one('a.a-link-normal.s-no-outline'))
                   and (href := link.get('href', '').strip())
            ]

            # 4. 拼成一格
            return {
                'URL': '\n'.join(urls) if urls else '',
                'oem_No': oem_no,
                '_idx': task.idx
            }
            #prase_out先不用，用谁，谁改名成prase
        def parse_out(self, task: PageTask, soup: BeautifulSoup) -> Dict:
            """解析所有字段"""

            # 解析标题
            title = self._parse_title(soup)

            # 解析价格
            price = self._parse_price(soup)

            # 解析About this item (商品要点)
            about_table = self._parse_about(soup)

            # 解析Technical specifications (技术规格)
            item_specifics = self._parse_specs(soup)

            # 解析Product description (商品描述)
            description = self._parse_description(soup)

            # 解析卖家反馈/评分
            seller_feedback = self._parse_rating(soup)

            # 解析图片
            pictures = self._parse_pictures(soup)

            # 解析Additional Information
            additional_info = self._parse_additional_info(soup)

            return {
                'URL': task.url,
                'Title': title,
                'price': price,
                'About_table': about_table,
                'item specifics': item_specifics,
                'Additional_info': description,
                'seller_feedback': seller_feedback,
                'picture': pictures,
                '_idx': task.idx
            }


        def _parse_pictures(self, soup: BeautifulSoup) -> str:
            """解析图片URL"""
            thumb_grid = soup.select_one('div[data-testid="grid-container"].ux-image-grid.no-scrollbar')
            if not thumb_grid:
                return ''

            seen = []

            for btn in thumb_grid.select('button[data-idx]'):
                img = btn.select_one('img')
                if not img:
                    continue

                # 优先data-src，再src
                thumb_url = img.get('data-src') or img.get('src', '')

                if not thumb_url or 'i.ebayimg.com' not in thumb_url or 's-l' not in thumb_url:
                    continue

                # 跳过视频（有play图标）
                if btn.select_one('svg.icon-play'):
                    continue

                # 转高清
                highres_url = re.sub(r'/s-l\d+\.(webp|jpg|jpeg)', '/s-l1600.jpg', thumb_url)

                if highres_url not in seen:
                    seen.append(highres_url)

            return '\n'.join(seen) if seen else ''


class QuickInfoConsumer(BaseConsumer):
    """快速信息消费者"""

    def __init__(self, page_queue: PageQueue,url_list, result_callback: Callable):
        super().__init__('QuickInfo', page_queue, result_callback)

    def parse(self, task: PageTask, soup: BeautifulSoup) -> Dict:
        """只解析标题和价格"""
        title_ele = soup.select_one('h1.x-item-title-label')
        title = title_ele.get_text(strip=True) if title_ele else task.title

        price_ele = soup.select_one('span[itemprop="price"]')
        price = price_ele.get_text(strip=True) if price_ele else task.price

        return {
            'URL': task.url,
            'Title': title,
            'price': price,
            'About_table': '[快速模式]',
            'item specifics': '[快速模式]',
            'description_from_the_seller': '[快速模式]',
            'seller_feedback': '[快速模式]',
            'picture': '[快速模式]',
            '_idx': task.idx,
            '_mode': 'quick'
        }

class FullParserConsumer_1(BaseConsumer):
        """全量解析消费者 - 用BeautifulSoup解析"""

        def __init__(self, page_queue: PageQueue,result_callback: Callable):
            super().__init__('FullParser', page_queue, result_callback)

        @staticmethod
        def safe_text(ele, default: str = '') -> str:
            """安全获取文本"""
            try:
                return ele.get_text(strip=True) if ele else default
            except Exception:
                return default

        @staticmethod
        def safe_attr(ele, attr: str, default: str = '') -> str:
            """安全获取属性"""
            try:
                return ele.get(attr, default) if ele else default
            except Exception:
                return default

        def parse_out(self,task: PageTask, soup: BeautifulSoup) -> Dict:
            oem_no = getattr(task, 'oem_No', '')

            # 1. 取单个 ul，找不到就 None
            ulv = soup.select_one('ul.srp-results.srp-list.clearfix')
            if not ulv:  # 防止 None.select 再报错
                return {'URL': '', 'oem_No': oem_no, '_idx': task.idx}

            # 2. 在单个 ul 里继续拿卡片
            cards = ulv.select('div.su-card-container__media')

            # 3. 收集 href
            urls = [
                link['href'].strip()
                for c in cards
                if (link := c.select_one('a.s-card__link.image-treatment')) and link.get('href')
            ]

            # 4. 拼成一格
            return {
                'URL': '\n'.join(urls) if urls else '',
                'oem_No': oem_no,
                '_idx': task.idx
            }
            # prase_out先不用，用谁，谁改名成prase

        def parse(self, task: PageTask, soup: BeautifulSoup) -> Dict:
            """解析所有字段"""

            # 解析标题
            title_ele = soup.select_one('span[id="productTitle"]')
            title = self.safe_text(title_ele) if title_ele else task.title

            # 解析价格
            price_section = soup.select_one('div.a-section.a-spacing-none.aok-align-center.aok-relative')
            price = ''
            if price_section:
                price_span = price_section.select_one('span.aok-offscreen')
                price = self.safe_text(price_span)
            if not price:
                price = ''

            about_table = self._parse_about(soup)

            # 解析Technical specifications (技术规格)
            item_specifics = self._parse_specs(soup)

            # 解析Product description (商品描述)
            description = self._parse_description(soup)

            seller_feedback = self._parse_rating(soup)

            pictures = self._parse_pictures(soup,task)

            category = self._pares_category(soup)

            return {
                'URL': task.url,
                'Title': title,
                'price': price,
                'About_table': about_table,
                'item specifics': item_specifics,
                'description_from_the_seller': description,
                'seller_feedback': seller_feedback,
                'category': category,
                'picture': pictures,
                'OEM': task.oem_No,
                '_idx': task.idx
            }

        def _parse_about(self, soup: BeautifulSoup) -> str:
            """解析About this item (商品要点)"""
            # 查找About this item的ul列表
            about_ul = soup.select_one('ul.a-unordered-list.a-vertical.a-spacing-mini')
            if not about_ul:
                return ''

            lines = []
            for li in about_ul.select('li'):
                text = self.safe_text(li)
                if text:
                    lines.append(text)

            return '\n'.join(lines)


        def _pares_category(self, soup:BeautifulSoup) -> str:
            """
            解析，物品分类
            """
            cate_divs = soup.select_one('ul.a-unordered-list.a-horizontal.a-size-small')
            if not cate_divs:
                return ''
            lines = []
            for li in cate_divs.select('li'):
                text = self.safe_text(li)
                if text:
                    lines.append(text)

            return ' '.join(lines)

        def _parse_description(self, soup: BeautifulSoup) -> str:
            """解析Product description (商品描述)"""
            descriptions = []

            # 方法1: launchpad-text-left-justify
            desc_divs = soup.select('div.a-section.launchpad-text-left-justify')
            for d in desc_divs:
                text = self._get_text_with_breaks(d)
                if text:
                    descriptions.append(text)

            # 方法2: productDescription
            desc_div = soup.select_one('div#productDescription')
            if desc_div:
                text = self._get_text_with_breaks(desc_div)
                if text:
                    descriptions.append(text)

            # 方法3: feature-bullets (特殊处理列表)
            feature_div = soup.select_one('div#feature-bullets')
            if feature_div:
                text = self._parse_feature_bullets(feature_div)
                if text:
                    descriptions.append(text)

            # 不同区块之间用双换行分隔
            return '\n\n'.join(descriptions) if descriptions else ''

        def _get_text_with_breaks(self, element) -> str:
            """提取文本，保留换行结构"""
            if not element:
                return ''

            # 复制元素避免修改原soup
            import copy
            elem = copy.copy(element)

            # 把 <br>, <br/> 替换为换行符
            for br in elem.find_all('br'):
                br.replace_with('\n')

            # 把 <p> 段落前后加换行
            for p in elem.find_all('p'):
                p.insert_before('\n')
                p.unwrap()  # 去掉p标签但保留内容

            # 获取文本，用换行符连接
            text = elem.get_text(separator='\n', strip=False)

            # 清理：去掉空行但保留换行结构
            lines = [line.strip() for line in text.split('\n') if line.strip()]
            return '\n'.join(lines)

        def _parse_feature_bullets(self, element) -> str:
            """解析特性列表，每个li一行"""
            if not element:
                return ''

            # 先找li标签
            items = element.select('li')
            if items:
                texts = []
                for li in items:
                    # 去掉"See more"之类的按钮文本
                    text = li.get_text(strip=True)
                    # 过滤掉常见垃圾信息
                    if text and not any(x in text.lower() for x in ['see more', 'see less', '›']):
                        texts.append(text)
                return '\n'.join(texts)

            # 没有li就按普通文本处理
            return self._get_text_with_breaks(element)


        def _parse_specs(self, soup: BeautifulSoup) -> str:
            """解析Technical specifications (技术规格表)"""
            specs = []

            # 方法1: 查找productDetails_techSpec_section_1表格
            t_1=soup.select_one('table#productDetails_techSpec_section_1')
            t_2=soup.select_one('table#productDetails_detailBullets_sections1')
            table = t_1 if t_1 else t_2
            if not table:
                product = soup.find("div", {"id": "productDetails_expanderTables_depthLeftSections"})
                if product:
                    table = product.find("table", {"class": "a-keyvalue prodDetTable"})

            if table:
                for row in table.select('tr'):
                    key_ele = row.select_one('th')
                    val_ele = row.select_one('td')
                    if key_ele and val_ele:
                        key = self.safe_text(key_ele)
                        val = self.safe_text(val_ele)
                        if key and val:
                            specs.append(f'{key}: {val}')

            # 方法2: 查找detail-bullets中的技术细节
            if not specs:
                detail_bullets = soup.select_one('div#detailBullets_feature_div')
                if detail_bullets:
                    for li in detail_bullets.select('li'):
                        text = self.safe_text(li)
                        if text and ':' in text:
                            specs.append(text)

            return '\n'.join(specs)

        def _parse_rating(self, soup: BeautifulSoup) -> str:
            """
            """
            specs = []

            div_rating = soup.select('li.review.aok-relative')

            if not div_rating:
                return ''

            for li in div_rating:
                import re

                # 复制li内容
                li_html = str(li)

                # 1. 移除script标签
                li_html = re.sub(r'<script\b[^<]*(?:(?!<\/script>)<[^<]*)*<\/script>', '', li_html,
                                 flags=re.DOTALL | re.IGNORECASE)

                # 2. 替换所有形式的<br>为/?|\（字符串替换，不用正则）
                li_html = li_html.replace('<br>', '/?|\\').replace('<br/>', '/?|\\').replace('<br />', '/?|\\')
                li_html = li_html.replace('<BR>', '/?|\\').replace('<BR/>', '/?|\\').replace('<BR />', '/?|\\')

                # 3. 重新解析
                li_processed = BeautifulSoup(li_html, 'html.parser')

                # 4. 提取所有span
                span_texts = []
                for span in li_processed.find_all('span'):
                    # 去除剩余标签，保留文本
                    span_text = re.sub(r'<[^>]+>', '', str(span))
                    span_text = span_text.strip()
                    if span_text:
                        span_texts.append(span_text)

                if span_texts:
                    specs.append('-%-'.join(span_texts))

            return '\n'.join(specs)



        def _parse_pictures(self, soup: BeautifulSoup, task: PageTask) -> str:
            """解析商品图片URL - 只从imageBlock后的script中提取"""
            import re
            import json

            image_block = soup.select_one('div#imageBlock')
            if not image_block:
                return ''

            script = image_block.find_next_sibling('script')
            if not script:
                return ''

            script_text = script.string if script.string else script.text

            # 找到 'initial': 的位置
            initial_start = script_text.find("'initial':")
            if initial_start == -1:
                initial_start = script_text.find('"initial":')
            if initial_start == -1:
                return ''

            # 从 '[' 开始找，手动匹配括号
            bracket_start = script_text.find('[', initial_start)
            if bracket_start == -1:
                return ''

            # 手动匹配中括号，处理嵌套
            bracket_count = 0
            bracket_end = bracket_start
            for i, char in enumerate(script_text[bracket_start:], start=bracket_start):
                if char == '[':
                    bracket_count += 1
                elif char == ']':
                    bracket_count -= 1
                    if bracket_count == 0:
                        bracket_end = i + 1
                        break

            json_str = script_text[bracket_start:bracket_end]

            try:
                images = json.loads(json_str)
                urls = [img.get('hiRes') or img.get('large') for img in images if img.get('hiRes') or img.get('large')]
                return '\n'.join(urls) if urls else ''
            except Exception as e:
                return ''

