#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
国家金融监督管理总局 - 派出机构行政处罚数据爬虫
目标: https://www.nfra.gov.cn/cn/view/pages/ItemList.html?itemPId=923&itemId=4293&itemsubPId=931

表格有两种典型格式:
  5列: 序号, 当事人名称, 主要违法违规行为, 行政处罚内容, 作出决定机关
  6列: 序号, 当事人名称, 行政处罚决定书文号, 主要违法违规行为, 行政处罚内容, 作出决定机关
第一行是表头, 后续每行是一条独立的处罚记录.
"""

import json
import random
import re
import time
import warnings
from io import StringIO
from pathlib import Path

import pandas as pd
import requests
from tqdm import tqdm

# ============ 配置 ============

ITEM_ID = "4293"  # 派出机构
PAGE_SIZE = 100   # 增大页面大小, 减少请求总数
OUTPUT_FILE = "nfra_派出机构_行政处罚.xlsx"

# 中间缓存文件
CACHE_DIR = Path(__file__).parent / "_cache"
DOC_LIST_CACHE = CACHE_DIR / "doc_list.json"
DETAIL_CACHE = CACHE_DIR / "detail_cache.json"

# 请求延迟配置 (秒) - 模拟人工浏览
DELAY_MIN = 2.0       # 正常请求最小间隔
DELAY_MAX = 5.0       # 正常请求最大间隔
BACKOFF_BASE = 30     # 遇到403时的基础退避时间
BACKOFF_MAX = 120     # 最大退避时间
MAX_RETRIES = 5       # 单次请求最大重试次数
SAVE_INTERVAL = 50    # 每爬N条详情自动保存一次

# User-Agent 池, 随机切换
USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

# API 地址
LIST_API = "https://www.nfra.gov.cn/cbircweb/DocInfo/SelectDocByItemIdAndChild"
DETAIL_API_TEMPLATE = "https://www.nfra.gov.cn/cn/static/data/DocInfo/SelectByDocId/data_docId={doc_id}.json"
DETAIL_URL_TEMPLATE = "https://www.nfra.gov.cn/cn/view/pages/governmentDetail.html?docId={doc_id}&itemId=4293&generaltype=1"


# ============ 智能请求器 ============

class SmartRequester:
    """
    带反爬对策的智能请求器:
    - 随机 User-Agent
    - 随机延迟 (模拟人工)
    - 遇到 403 自动指数退避重试
    - 连续失败自动加大间隔
    """

    def __init__(self):
        self.session = requests.Session()
        self.consecutive_fails = 0
        self.total_requests = 0
        self.total_403s = 0

    def _build_headers(self) -> dict:
        return {
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Host": "www.nfra.gov.cn",
            "Pragma": "no-cache",
            "Referer": "https://www.nfra.gov.cn/cn/view/pages/ItemList.html?"
                       "itemPId=923&itemId=4293&itemUrl=ItemListRightList.html"
                       "&itemName=%E6%B4%BE%E5%87%BA%E6%9C%BA%E6%9E%84"
                       "&itemsubPId=931&itemsubPName=%E8%A1%8C%E6%94%BF%E5%A4%84%E7%BD%9A",
            "X-Requested-With": "XMLHttpRequest",
            "User-Agent": random.choice(USER_AGENTS),
        }

    def _smart_delay(self):
        """智能延迟: 连续失败越多, 延迟越长"""
        extra = min(self.consecutive_fails * 2, 15)
        delay = random.uniform(DELAY_MIN + extra, DELAY_MAX + extra)
        time.sleep(delay)

    def get(self, url: str, params: dict = None) -> requests.Response | None:
        """
        发送 GET 请求, 带自动重试和退避。
        返回 Response 或 None (全部重试失败)。
        """
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                self.total_requests += 1
                headers = self._build_headers()
                resp = self.session.get(url, params=params, headers=headers, timeout=30)

                if resp.status_code == 403:
                    self.total_403s += 1
                    self.consecutive_fails += 1
                    backoff = min(BACKOFF_BASE * (2 ** (attempt - 1)) + random.uniform(0, 10), BACKOFF_MAX)
                    print(f"\n  [403] 第{attempt}次重试, 退避 {backoff:.0f}s...")
                    time.sleep(backoff)
                    # 重建 session 刷新连接
                    if attempt >= 3:
                        self.session.close()
                        self.session = requests.Session()
                    continue

                resp.raise_for_status()
                self.consecutive_fails = 0
                return resp

            except requests.exceptions.HTTPError:
                # 非403的HTTP错误
                self.consecutive_fails += 1
                if attempt < MAX_RETRIES:
                    time.sleep(random.uniform(5, 15))
                continue
            except requests.exceptions.RequestException as e:
                # 网络错误
                self.consecutive_fails += 1
                if attempt < MAX_RETRIES:
                    print(f"\n  [网络错误] {e}, 第{attempt}次重试...")
                    time.sleep(random.uniform(5, 15))
                continue

        return None

    def stats(self) -> str:
        return f"总请求: {self.total_requests}, 403次数: {self.total_403s}"


# 全局请求器
requester = SmartRequester()


# ============ 缓存管理 ============

def ensure_cache_dir():
    CACHE_DIR.mkdir(exist_ok=True)


def load_doc_list_cache() -> list[dict] | None:
    if DOC_LIST_CACHE.exists():
        data = json.loads(DOC_LIST_CACHE.read_text(encoding="utf-8"))
        print(f"从缓存加载 {len(data)} 条文档列表")
        return data
    return None


def save_doc_list_cache(rows: list[dict]):
    ensure_cache_dir()
    DOC_LIST_CACHE.write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")


def load_detail_cache() -> dict:
    """返回 {docId: records_list}"""
    if DETAIL_CACHE.exists():
        return json.loads(DETAIL_CACHE.read_text(encoding="utf-8"))
    return {}


def save_detail_cache(cache: dict):
    ensure_cache_dir()
    DETAIL_CACHE.write_text(json.dumps(cache, ensure_ascii=False), encoding="utf-8")


# ============ 列表抓取 ============

def get_total_count() -> int:
    """获取总记录数"""
    params = {"itemId": ITEM_ID, "pageSize": "1", "pageIndex": "1"}
    resp = requester.get(LIST_API, params=params)
    if resp is None:
        raise RuntimeError("无法获取总记录数, 请检查网络")
    data = resp.json()
    total = int(data["data"]["total"])
    return total


DEFAULT_MAX_PAGES = 179  # 默认最大爬取页数


def fetch_doc_list(max_pages: int = DEFAULT_MAX_PAGES) -> list[dict]:
    """抓取文档列表, 支持缓存
    :param max_pages: 最大爬取页数, 默认179页
    """
    cached = load_doc_list_cache()
    if cached:
        return cached

    total = get_total_count()
    total_pages = (total + PAGE_SIZE - 1) // PAGE_SIZE
    actual_pages = min(total_pages, max_pages)
    print(f"共 {total} 条记录, {total_pages} 页 (pageSize={PAGE_SIZE}), 本次爬取 {actual_pages} 页")

    all_rows = []
    for page_idx in tqdm(range(1, actual_pages + 1), desc="抓取列表"):
        params = {"itemId": ITEM_ID, "pageSize": str(PAGE_SIZE), "pageIndex": str(page_idx)}
        resp = requester.get(LIST_API, params=params)

        if resp is not None:
            try:
                rows = resp.json()["data"]["rows"]
                all_rows.extend(rows)
            except (KeyError, ValueError) as e:
                print(f"\n  列表第 {page_idx} 页解析失败: {e}")
        else:
            print(f"\n  列表第 {page_idx} 页抓取失败 (重试耗尽)")

        requester._smart_delay()

        # 每100页保存一次中间结果
        if page_idx % 100 == 0:
            save_doc_list_cache(all_rows)
            print(f"\n  [进度保存] 已抓取 {len(all_rows)} 条")

    save_doc_list_cache(all_rows)
    print(f"文档列表抓取完成: {len(all_rows)} 条 (期望 {total} 条)")
    return all_rows


# ============ 详情抓取 ============

def fetch_detail_html(doc_id: int) -> str | None:
    """获取单条处罚详情的 HTML 内容"""
    url = DETAIL_API_TEMPLATE.format(doc_id=doc_id)
    resp = requester.get(url)
    if resp is None:
        return None
    try:
        data = resp.json()
        return data.get("data", {}).get("docClob", "")
    except (ValueError, KeyError):
        return None


def clean_text(text) -> str:
    """清洗单元格文本"""
    if pd.isna(text) or text is None:
        return ""
    text = str(text).strip()
    text = re.sub(r'\s+', ' ', text)
    return text


# ============ 新增提取函数 ============

# 中文数字转换映射
CN_NUM = {'零': 0, '一': 1, '二': 2, '三': 3, '四': 4, '五': 5, '六': 6, '七': 7, '八': 8, '九': 9,
          '十': 10, '百': 100, '千': 1000, '万': 10000, '亿': 100000000, '壹': 1, '贰': 2, '叁': 3,
          '肆': 4, '伍': 5, '陆': 6, '柒': 7, '捌': 8, '玖': 9, '拾': 10, '佰': 100, '仟': 1000}


def cn_to_number(cn_str: str) -> float | None:
    """中文数字转阿拉伯数字, 如 '三十五' -> 35, '一百二十' -> 120"""
    cn_str = cn_str.strip()
    if not cn_str:
        return None
    # 如果已经是阿拉伯数字
    try:
        return float(cn_str)
    except ValueError:
        pass

    result = 0
    section = 0  # 当前万以下的累加
    current = 0  # 当前数字
    for ch in cn_str:
        if ch not in CN_NUM:
            return None
        val = CN_NUM[ch]
        if val == 100000000:  # 亿
            section += current
            result += section * 100000000
            section = 0
            current = 0
        elif val == 10000:  # 万
            section += current
            result += section * 10000
            section = 0
            current = 0
        elif val >= 10:  # 十/百/千
            if current == 0:
                current = 1  # "十五" -> 15
            section += current * val
            current = 0
        else:  # 0-9
            current = val
    section += current
    result += section
    return result if result > 0 else None


def extract_penalty_type(content: str) -> str:
    """从处罚内容提取处罚类型"""
    if not content:
        return ""
    types = []
    if "警告" in content:
        types.append("警告")
    if re.search(r'罚款|罚金', content):
        types.append("罚款")
    if "禁止从事" in content or "禁止进入" in content:
        types.append("禁止从业")
    if "责令" in content:
        types.append("责令改正")
    if "吊销" in content:
        types.append("吊销许可证")
    if "没收" in content:
        types.append("没收违法所得")
    if "撤销" in content:
        types.append("撤销任职资格")
    if "取消" in content:
        types.append("取消任职资格")
    return "、".join(types) if types else "其他"


def extract_fine_amount(content: str) -> float | None:
    """
    从处罚内容提取罚款金额并统一转为元。
    支持: '罚款30万元', '罚款1万元', '罚款三十五万元', '处1万元罚款',
          '罚款5000元', '罚金100万元', '罚款一万元'
    """
    if not content:
        return None

    # 模式1: "罚款X万元" / "罚款X元" / "罚金X万元" (阿拉伯数字)
    m = re.search(r'(?:罚款|罚金|处罚款)\s*([\d.]+)\s*万元', content)
    if m:
        return float(m.group(1)) * 10000

    m = re.search(r'(?:罚款|罚金|处罚款)\s*([\d.]+)\s*元', content)
    if m:
        return float(m.group(1))

    # 模式2: "处X万元罚款" / "处X元罚款"
    m = re.search(r'处\s*([\d.]+)\s*万元\s*(?:的)?罚款', content)
    if m:
        return float(m.group(1)) * 10000

    m = re.search(r'处\s*([\d.]+)\s*元\s*(?:的)?罚款', content)
    if m:
        return float(m.group(1))

    # 模式3: 中文数字 "罚款三十五万元" / "罚款一万元"
    m = re.search(r'(?:罚款|罚金|处罚款)\s*([零一二三四五六七八九十百千壹贰叁肆伍陆柒捌玖拾佰仟]+)\s*(万)?元', content)
    if m:
        cn_part = m.group(1)
        has_wan = m.group(2) == '万'
        val = cn_to_number(cn_part)
        if val is not None:
            if has_wan:
                val *= 10000
            return val

    # 模式4: "并罚款X万元" / "并处罚款X万元"
    m = re.search(r'(?:并|并处)罚款\s*([\d.]+)\s*万元', content)
    if m:
        return float(m.group(1)) * 10000

    m = re.search(r'(?:并|并处)罚款\s*([\d.]+)\s*元', content)
    if m:
        return float(m.group(1))

    # 模式5: 单独 "X万元" (如果上下文有罚款关键词)
    if re.search(r'罚', content):
        m = re.search(r'([\d.]+)\s*万元', content)
        if m:
            return float(m.group(1)) * 10000
        m = re.search(r'([\d.]+)\s*元', content)
        if m:
            return float(m.group(1))

    return None


# 城市到省份映射 (金融监管分局所在城市)
CITY_PROVINCE_MAP = {
    # 直辖市
    "北京": "北京市", "天津": "天津市", "上海": "上海市", "重庆": "重庆市",
    # 计划单列市
    "深圳": "广东省", "青岛": "山东省", "大连": "辽宁省", "宁波": "浙江省", "厦门": "福建省",
    # 特殊区域
    "滨海": "天津市", "两江": "重庆市", "万州": "重庆市", "涪陵": "重庆市", "永川": "重庆市", "黔江": "重庆市",
    # 广东省
    "广州": "广东省", "佛山": "广东省", "东莞": "广东省", "中山": "广东省", "珠海": "广东省", "汕头": "广东省",
    "惠州": "广东省", "江门": "广东省", "湛江": "广东省", "茂名": "广东省", "肇庆": "广东省", "梅州": "广东省",
    "揭阳": "广东省", "清远": "广东省", "韶关": "广东省", "河源": "广东省", "阳江": "广东省", "潮州": "广东省",
    "汕尾": "广东省", "云浮": "广东省",
    # 江苏省
    "南京": "江苏省", "苏州": "江苏省", "无锡": "江苏省", "常州": "江苏省", "南通": "江苏省", "徐州": "江苏省",
    "盐城": "江苏省", "扬州": "江苏省", "泰州": "江苏省", "镇江": "江苏省", "淮安": "江苏省", "连云港": "江苏省",
    "宿迁": "江苏省",
    # 浙江省
    "杭州": "浙江省", "温州": "浙江省", "嘉兴": "浙江省", "湖州": "浙江省", "绍兴": "浙江省", "金华": "浙江省",
    "台州": "浙江省", "衢州": "浙江省", "丽水": "浙江省", "舟山": "浙江省",
    # 山东省
    "济南": "山东省", "烟台": "山东省", "潍坊": "山东省", "临沂": "山东省", "济宁": "山东省", "淄博": "山东省",
    "威海": "山东省", "东营": "山东省", "泰安": "山东省", "日照": "山东省", "聊城": "山东省", "德州": "山东省",
    "滨州": "山东省", "菏泽": "山东省", "枣庄": "山东省",
    # 河南省
    "郑州": "河南省", "洛阳": "河南省", "南阳": "河南省", "许昌": "河南省", "周口": "河南省", "新乡": "河南省",
    "信阳": "河南省", "驻马店": "河南省", "商丘": "河南省", "开封": "河南省", "安阳": "河南省", "焦作": "河南省",
    "平顶山": "河南省", "鹤壁": "河南省", "濮阳": "河南省", "漯河": "河南省", "三门峡": "河南省",
    # 河北省
    "石家庄": "河北省", "唐山": "河北省", "保定": "河北省", "邯郸": "河北省", "廊坊": "河北省", "沧州": "河北省",
    "承德": "河北省", "张家口": "河北省", "衡水": "河北省", "邢台": "河北省", "秦皇岛": "河北省",
    # 湖北省
    "武汉": "湖北省", "宜昌": "湖北省", "襄阳": "湖北省", "荆州": "湖北省", "黄冈": "湖北省", "孝感": "湖北省",
    "十堰": "湖北省", "黄石": "湖北省", "咸宁": "湖北省", "恩施": "湖北省", "鄂州": "湖北省", "荆门": "湖北省",
    "随州": "湖北省",
    # 湖南省
    "长沙": "湖南省", "衡阳": "湖南省", "株洲": "湖南省", "常德": "湖南省", "岳阳": "湖南省", "邵阳": "湖南省",
    "益阳": "湖南省", "娄底": "湖南省", "永州": "湖南省", "怀化": "湖南省", "郴州": "湖南省", "湘潭": "湖南省",
    "张家界": "湖南省", "湘西": "湖南省",
    # 四川省
    "成都": "四川省", "绵阳": "四川省", "德阳": "四川省", "宜宾": "四川省", "攀枝花": "四川省", "眉山": "四川省",
    "南充": "四川省", "自贡": "四川省", "乐山": "四川省", "泸州": "四川省", "达州": "四川省", "内江": "四川省",
    "遂宁": "四川省", "广安": "四川省", "巴中": "四川省", "资阳": "四川省", "广元": "四川省", "雅安": "四川省",
    "甘孜": "四川省", "阿坝": "四川省", "凉山": "四川省",
    # 福建省
    "福州": "福建省", "泉州": "福建省", "漳州": "福建省", "龙岩": "福建省", "三明": "福建省", "宁德": "福建省",
    "莆田": "福建省", "南平": "福建省",
    # 安徽省
    "合肥": "安徽省", "芜湖": "安徽省", "蚌埠": "安徽省", "阜阳": "安徽省", "淮南": "安徽省", "安庆": "安徽省",
    "马鞍山": "安徽省", "淮北": "安徽省", "铜陵": "安徽省", "亳州": "安徽省", "黄山": "安徽省", "巢湖": "安徽省",
    "宣城": "安徽省", "池州": "安徽省", "六安": "安徽省", "滁州": "安徽省", "宿州": "安徽省",
    # 江西省
    "南昌": "江西省", "赣州": "江西省", "九江": "江西省", "吉安": "江西省", "上饶": "江西省", "宜春": "江西省",
    "抚州": "江西省", "景德镇": "江西省", "萍乡": "江西省", "新余": "江西省", "鹰潭": "江西省",
    # 辽宁省
    "沈阳": "辽宁省", "鞍山": "辽宁省", "抚顺": "辽宁省", "本溪": "辽宁省", "丹东": "辽宁省", "锦州": "辽宁省",
    "营口": "辽宁省", "阜新": "辽宁省", "辽阳": "辽宁省", "铁岭": "辽宁省", "朝阳": "辽宁省", "盘锦": "辽宁省",
    "葫芦岛": "辽宁省",
    # 吉林省
    "长春": "吉林省", "吉林": "吉林省", "四平": "吉林省", "辽源": "吉林省", "通化": "吉林省", "白山": "吉林省",
    "松原": "吉林省", "白城": "吉林省", "延边": "吉林省",
    # 黑龙江省
    "哈尔滨": "黑龙江省", "齐齐哈尔": "黑龙江省", "牡丹江": "黑龙江省", "佳木斯": "黑龙江省", "大庆": "黑龙江省",
    "鸡西": "黑龙江省", "双鸭山": "黑龙江省", "伊春": "黑龙江省", "七台河": "黑龙江省", "鹤岗": "黑龙江省",
    "绥化": "黑龙江省", "黑河": "黑龙江省", "大兴安岭": "黑龙江省",
    # 陕西省
    "西安": "陕西省", "咸阳": "陕西省", "宝鸡": "陕西省", "渭南": "陕西省", "汉中": "陕西省", "延安": "陕西省",
    "榆林": "陕西省", "安康": "陕西省", "商洛": "陕西省", "铜川": "陕西省",
    # 甘肃省
    "兰州": "甘肃省", "天水": "甘肃省", "白银": "甘肃省", "金昌": "甘肃省", "平凉": "甘肃省", "庆阳": "甘肃省",
    "定西": "甘肃省", "陇南": "甘肃省", "武威": "甘肃省", "张掖": "甘肃省", "酒泉": "甘肃省", "嘉峪关": "甘肃省",
    "甘南": "甘肃省", "临夏": "甘肃省",
    # 山西省
    "太原": "山西省", "大同": "山西省", "运城": "山西省", "临汾": "山西省", "晋中": "山西省", "长治": "山西省",
    "吕梁": "山西省", "忻州": "山西省", "晋城": "山西省", "朔州": "山西省", "阳泉": "山西省",
    # 云南省
    "昆明": "云南省", "曲靖": "云南省", "红河": "云南省", "大理": "云南省", "文山": "云南省", "昭通": "云南省",
    "玉溪": "云南省", "楚雄": "云南省", "普洱": "云南省", "保山": "云南省", "临沧": "云南省", "德宏": "云南省",
    "迪庆": "云南省", "怒江": "云南省", "丽江": "云南省", "西双版纳": "云南省",
    # 贵州省
    "贵阳": "贵州省", "遵义": "贵州省", "黔东南": "贵州省", "黔南": "贵州省", "毕节": "贵州省", "铜仁": "贵州省",
    "安顺": "贵州省", "六盘水": "贵州省", "黔西南": "贵州省",
    # 广西
    "南宁": "广西壮族自治区", "柳州": "广西壮族自治区", "桂林": "广西壮族自治区", "梧州": "广西壮族自治区",
    "北海": "广西壮族自治区", "玉林": "广西壮族自治区", "百色": "广西壮族自治区", "贺州": "广西壮族自治区",
    "河池": "广西壮族自治区", "来宾": "广西壮族自治区", "崇左": "广西壮族自治区", "钦州": "广西壮族自治区",
    "防城港": "广西壮族自治区",
    # 内蒙古
    "呼和浩特": "内蒙古自治区", "包头": "内蒙古自治区", "赤峰": "内蒙古自治区", "鄂尔多斯": "内蒙古自治区",
    "通辽": "内蒙古自治区", "呼伦贝尔": "内蒙古自治区", "巴彦淖尔": "内蒙古自治区", "乌兰察布": "内蒙古自治区",
    "锡林郭勒": "内蒙古自治区", "兴安": "内蒙古自治区", "阿拉善": "内蒙古自治区", "乌海": "内蒙古自治区",
    # 新疆
    "乌鲁木齐": "新疆维吾尔自治区", "昌吉": "新疆维吾尔自治区", "伊犁": "新疆维吾尔自治区", "喀什": "新疆维吾尔自治区",
    "阿克苏": "新疆维吾尔自治区", "巴音郭楞": "新疆维吾尔自治区", "克拉玛依": "新疆维吾尔自治区",
    "哈密": "新疆维吾尔自治区", "克孜勒苏": "新疆维吾尔自治区", "阿勒泰": "新疆维吾尔自治区",
    "博尔塔拉": "新疆维吾尔自治区", "塔城": "新疆维吾尔自治区", "和田": "新疆维吾尔自治区", "吐鲁番": "新疆维吾尔自治区",
    # 西藏
    "拉萨": "西藏自治区", "日喀则": "西藏自治区", "山南": "西藏自治区", "林芝": "西藏自治区",
    "昌都": "西藏自治区", "那曲": "西藏自治区", "阿里": "西藏自治区",
    # 宁夏
    "银川": "宁夏回族自治区", "石嘴山": "宁夏回族自治区", "吴忠": "宁夏回族自治区", "固原": "宁夏回族自治区",
    "中卫": "宁夏回族自治区",
    # 海南省
    "海口": "海南省", "三亚": "海南省",
    # 青海省
    "西宁": "青海省", "海东": "青海省", "海北": "青海省", "海南": "青海省", "海西": "青海省",
    "黄南": "青海省", "果洛": "青海省", "玉树": "青海省",
}

# 省级监管局名称中直接包含的省份
PROVINCE_KEYWORDS = [
    ("北京", "北京市"), ("天津", "天津市"), ("上海", "上海市"), ("重庆", "重庆市"),
    ("河北", "河北省"), ("山西", "山西省"), ("辽宁", "辽宁省"), ("吉林", "吉林省"),
    ("黑龙江", "黑龙江省"), ("江苏", "江苏省"), ("浙江", "浙江省"), ("安徽", "安徽省"),
    ("福建", "福建省"), ("江西", "江西省"), ("山东", "山东省"), ("河南", "河南省"),
    ("湖北", "湖北省"), ("湖南", "湖南省"), ("广东", "广东省"), ("海南", "海南省"),
    ("四川", "四川省"), ("贵州", "贵州省"), ("云南", "云南省"), ("陕西", "陕西省"),
    ("甘肃", "甘肃省"), ("青海", "青海省"),
    ("广西", "广西壮族自治区"), ("内蒙古", "内蒙古自治区"), ("西藏", "西藏自治区"),
    ("宁夏", "宁夏回族自治区"), ("新疆", "新疆维吾尔自治区"),
]


def extract_province(authority: str) -> str:
    """从作出决定机关名称提取所属省份"""
    if not authority:
        return ""
    # 1. 先匹配省级关键词 (如 "江西监管局", "湖北金融监管局")
    for keyword, province in PROVINCE_KEYWORDS:
        if keyword in authority:
            return province
    # 2. 匹配城市名 (如 "青岛金融监管局", "宜宾监管分局")
    # 去掉前缀 "国家金融监督管理总局" 后匹配
    clean_auth = authority.replace("国家金融监督管理总局", "").replace("国家金融监管总局", "")
    clean_auth = clean_auth.replace("中国银保监会", "").replace("原中国银保监会", "")
    # 按城市名长度降序匹配 (先匹配 "石家庄" 再匹配 "石")
    for city in sorted(CITY_PROVINCE_MAP.keys(), key=len, reverse=True):
        if clean_auth.startswith(city):
            return CITY_PROVINCE_MAP[city]
    # 3. 直辖市/特殊: "深圳金融监管局" → 广东省
    for city, province in CITY_PROVINCE_MAP.items():
        if city in clean_auth:
            return province
    return ""


def extract_org_for_person(party_name: str, penalty_content: str, authority: str) -> str:
    """
    为个人类型当事人提取所属机构。
    优先从当事人名称括号中提取, 其次从处罚内容、作出决定机关等推断。
    """
    if not party_name:
        return ""
    # 1. 从括号中提取: "朱喜兰（时任富德生命人寿保险股份有限公司金昌中心支公司总经理）"
    m = re.search(r'[（(](.*?)[）)]', party_name)
    if m:
        bracket_content = m.group(1)
        # 提取机构名: 找到最后一个机构关键词结尾
        org_match = re.search(r'([一-鿿]*(?:公司|银行|支行|分行|营业部|中心支公司|分公司|支公司|'
                              r'信用社|联社|办事处|基金|证券|信托|集团|中心|营业|局))', bracket_content)
        if org_match:
            return org_match.group(1)
    # 2. 从处罚内容中提取: "对时任该公司总经理..." -> 无法获取具体机构名
    # 3. 返回空 (在 try_split_combined_party 中会由调用者设置)
    return ""


def enrich_record(record: dict) -> dict:
    """对已构建的记录补充: 所属机构、处罚类型、罚款金额、所属省份"""
    content = record.get("行政处罚内容", "")
    party_name = record.get("当事人名称", "")
    party_type = record.get("当事人类型", "")
    authority = record.get("作出决定机关", "")

    # 处罚类型
    record["处罚类型"] = extract_penalty_type(content)

    # 罚款金额(元)
    fine = extract_fine_amount(content)
    record["罚款金额(元)"] = fine if fine else ""

    # 所属省份
    record["所属省份"] = extract_province(authority)

    # 所属机构 (仅个人类型)
    if party_type == "个人":
        record["所属机构"] = extract_org_for_person(party_name, content, authority)
    else:
        record["所属机构"] = ""

    return record


def classify_party_type(name: str) -> str:
    """判断当事人是机构还是个人"""
    if not name:
        return "未知"
    # 含括号说明的人名, 如 "朱喜兰（时任...公司总经理）"
    # 先提取括号前的部分判断
    base_name = re.split(r'[（(]', name)[0].strip()
    # 机构关键词
    org_keywords = [
        "银行", "保险", "公司", "支行", "分行", "营业部", "中心支公司",
        "分公司", "支公司", "办事处", "信用社", "合作联社", "联社",
        "担保", "基金", "证券", "信托", "财务", "村镇", "农商", "农信",
        "中心", "营业", "机构", "集团", "协会", "局",
    ]
    for kw in org_keywords:
        if kw in base_name:
            return "机构"
    # 排除 "及相关责任人" 这种复合名称
    if "及相关责任人" in name or "及" in base_name:
        return "机构/个人"
    return "个人"


# ============ 标准列映射 ============
# 根据表头模糊匹配映射到统一字段名

COLUMN_MAPPING = {
    "当事人": "当事人名称",
    "名称": "当事人名称",
    "姓名": "当事人名称",
    "违法违规": "主要违法违规事实",
    "案由": "主要违法违规事实",
    "处罚内容": "行政处罚内容",
    "处罚决定": "行政处罚内容",
    "决定机关": "作出决定机关",
    "机关名称": "作出决定机关",
    "机关": "作出决定机关",
    "文号": "处罚决定书文号",
    "决定书": "处罚决定书文号",
    "处罚依据": "行政处罚依据",
    "依据": "行政处罚依据",
    "日期": "作出处罚决定的日期",
}


def map_header(raw_header: str) -> str:
    """将原始表头映射为统一字段名"""
    h = clean_text(raw_header)
    if not h:
        return ""
    # 先精确匹配
    for keyword, field in COLUMN_MAPPING.items():
        if keyword in h:
            return field
    # 序号列跳过
    if "序号" in h or h.isdigit():
        return "序号"
    return h  # 未知列保留原名


def parse_penalty_table(html_content: str, doc_id: int) -> list[dict]:
    """
    解析处罚信息公开表, 每行数据对应一条处罚记录。
    同一个公开表可能包含多行 (对机构+对个人各一行, 或更多)。
    """
    if not html_content:
        return []

    source_url = DETAIL_URL_TEMPLATE.format(doc_id=doc_id)

    try:
        tables = pd.read_html(StringIO(html_content))
    except Exception:
        return parse_non_table_html(html_content, doc_id)

    all_records = []
    for table in tables:
        if table.empty or table.shape[0] < 2:
            continue
        try:
            records = parse_standard_table(table, doc_id)
            all_records.extend(records)
        except Exception as e:
            warnings.warn(f"docId={doc_id} 表格解析异常: {e}")
            continue

    # 如果标准解析没有结果, 尝试键值对解析 (两列表格)
    if not all_records:
        for table in tables:
            if table.shape[1] == 2 and table.shape[0] >= 3:
                record = parse_kv_table(table, doc_id)
                if record:
                    all_records.append(record)

    # 后处理: 为个人记录填充所属机构 (从同一文档的机构记录中推断)
    if all_records:
        # 找出同一文档中的机构名称
        org_names = [r.get("当事人名称", "") for r in all_records
                     if r.get("当事人类型") == "机构" and r.get("当事人名称")]
        if org_names:
            first_org = org_names[0]
            for r in all_records:
                if r.get("当事人类型") == "个人" and not r.get("所属机构"):
                    r["所属机构"] = first_org

    return all_records


def parse_standard_table(table: pd.DataFrame, doc_id: int) -> list[dict]:
    """
    解析标准多行表格:
    第一行是表头 (当事人名称, 主要违法违规行为, 行政处罚内容, 作出决定机关 等),
    后续每一行是一条处罚记录。
    """
    source_url = DETAIL_URL_TEMPLATE.format(doc_id=doc_id)
    nrows, ncols = table.shape

    if ncols < 3:
        return []

    # 第一行当作表头
    raw_headers = [clean_text(table.iloc[0, col]) for col in range(ncols)]
    mapped_headers = [map_header(h) for h in raw_headers]

    # 验证: 表头中至少应包含 "当事人名称" 或 "主要违法违规"
    header_str = " ".join(mapped_headers)
    if "当事人名称" not in header_str and "违法违规" not in header_str and "处罚" not in header_str:
        return []

    records = []
    for row_idx in range(1, nrows):
        row_data = {}
        for col_idx in range(ncols):
            field = mapped_headers[col_idx]
            if field and field != "序号":
                value = clean_text(table.iloc[row_idx, col_idx])
                row_data[field] = value

        # 跳过全空行
        if not any(v for v in row_data.values()):
            continue

        party_name = row_data.get("当事人名称", "")

        # 处理 "机构及相关责任人" 的合并记录: 尝试拆分
        split_records = try_split_combined_party(row_data, doc_id)
        if split_records:
            records.extend(split_records)
        else:
            record = build_standard_record(row_data, doc_id)
            records.append(record)

    return records


def try_split_combined_party(row_data: dict, doc_id: int) -> list[dict] | None:
    """
    尝试拆分 "机构及相关责任人" 的合并记录。
    例如当事人名称为 "XX公司及相关责任人"、处罚内容为 "对该公司罚款X万; 对XXX警告"。
    """
    party_name = row_data.get("当事人名称", "")
    penalty_content = row_data.get("行政处罚内容", "")

    if "及相关责任人" not in party_name and "及" not in party_name:
        return None

    # 从处罚内容中拆分: 以 "；" 或 "；" 分隔
    parts = re.split(r'[；;]', penalty_content)
    if len(parts) < 2:
        return None

    source_url = DETAIL_URL_TEMPLATE.format(doc_id=doc_id)
    records = []

    # 提取机构名 ("及"之前的部分)
    org_match = re.match(r'(.+?)(?:及相关责任人|及)', party_name)
    org_name = org_match.group(1).strip() if org_match else party_name

    for part in parts:
        part = part.strip()
        if not part:
            continue

        skip_current = False
        extra_records = []

        record = {
            "处罚决定书文号": row_data.get("处罚决定书文号", ""),
            "当事人名称": "",
            "当事人类型": "",
            "主要违法违规事实": row_data.get("主要违法违规事实", ""),
            "行政处罚依据": row_data.get("行政处罚依据", ""),
            "行政处罚内容": part,
            "作出决定机关": row_data.get("作出决定机关", ""),
            "作出处罚决定的日期": row_data.get("作出处罚决定的日期", ""),
            "源链接": source_url,
        }

        # 判断这部分处罚是对机构还是对个人
        # 先尝试提取个人姓名: "对时任该公司总经理金红岩给予..."
        person_patterns = [
            # "对时任该公司XX金红岩给予" / "对时任该公司XX金红岩予以"
            r'对时任[^\u4e00-\u9fff]*(?:该公司|该行|该机构|该银行|该支行)?[^\u4e00-\u9fff]*(?:[\u4e00-\u9fff]+?(?:部|室|处|科|中心|营业部))?(?:负责人|经理|主任|行长|总监|主管|总经理|副总经理|副行长|部长|科长)?([\u4e00-\u9fff]{2,4}?)(?:给予|予以|处以)',
            # "对XXX给予/予以/处以"
            r'对([\u4e00-\u9fff]{2,4}?)(?:给予|予以|处以)',
        ]
        person_name = None
        for pattern in person_patterns:
            m = re.search(pattern, part)
            if m:
                candidate = m.group(1).strip()
                # 排除机构关键词
                if not any(kw in candidate for kw in ['公司', '银行', '支行', '分行', '机构']):
                    person_name = candidate
                    break

        if person_name:
            record["当事人名称"] = person_name
            record["当事人类型"] = "个人"
        elif "对机构" in part or "该公司" in part or "该行" in part or "该机构" in part or "该银行" in part or "该支行" in part \
                or re.search(r'对[\u4e00-\u9fff]*(?:公司|银行|支行|分行|信用社|联社|分公司|支公司)[\u4e00-\u9fff]*(?:罚款|警告|责令)', part):
            # 没有个人姓名, 但有机构指代词 -> 机构
            record["当事人名称"] = org_name
            record["当事人类型"] = "机构"
        else:
            # 尝试提取多个人名: "对方宇峰、陈鹏飞给予警告" / "对米俊毅禁止从事..."
            multi_match = re.search(r'对([\u4e00-\u9fff]{2,4}(?:[,、、][\u4e00-\u9fff]{2,4})+)(?:给予|予以|处以|警告|罚款|禁止)', part)
            if multi_match:
                # 多个人, 拆分成多条
                names = re.split(r'[,、、]', multi_match.group(1))
                for name in names:
                    name = name.strip()
                    if name:
                        sub_record = record.copy()
                        sub_record["当事人名称"] = name
                        sub_record["当事人类型"] = "个人"
                        extra_records.append(sub_record)
                skip_current = True
            else:
                single_match = re.search(r'对([\u4e00-\u9fff]{2,4}?)(?:给予|予以|处以|警告|罚款|禁止)', part)
                # "对责任人员XXX" / "对XXX（涉刑）"
                resp_match = re.search(r'对责任人员([\u4e00-\u9fff]{2,4})', part)
                paren_match = re.search(r'对([\u4e00-\u9fff]{2,4}?)[（(]', part)
                if resp_match:
                    record["当事人名称"] = resp_match.group(1).strip()
                    record["当事人类型"] = "个人"
                elif paren_match:
                    candidate = paren_match.group(1).strip()
                    if not any(kw in candidate for kw in ['公司', '银行', '支行', '分行', '机构']):
                        record["当事人名称"] = candidate
                        record["当事人类型"] = "个人"
                    else:
                        record["当事人名称"] = org_name
                        record["当事人类型"] = "机构"
                elif single_match:
                    candidate = single_match.group(1).strip()
                    if not any(kw in candidate for kw in ['公司', '银行', '支行', '分行', '机构']):
                        record["当事人名称"] = candidate
                        record["当事人类型"] = "个人"
                    else:
                        record["当事人名称"] = org_name
                        record["当事人类型"] = "机构"
                else:
                    record["当事人名称"] = org_name
                    record["当事人类型"] = "未知"

        if not skip_current:
            # 为拆分记录中的个人设置所属机构
            if record.get("当事人类型") == "个人" and not record.get("所属机构"):
                record["所属机构"] = org_name
            records.append(enrich_record(record))
        for er in extra_records:
            if er.get("当事人类型") == "个人" and not er.get("所属机构"):
                er["所属机构"] = org_name
            records.append(enrich_record(er))

    return records if len(records) >= 2 else None


def build_standard_record(row_data: dict, doc_id: int) -> dict:
    """从行数据构建标准记录"""
    source_url = DETAIL_URL_TEMPLATE.format(doc_id=doc_id)
    party_name = row_data.get("当事人名称", "")

    record = {
        "处罚决定书文号": row_data.get("处罚决定书文号", ""),
        "当事人名称": party_name,
        "当事人类型": classify_party_type(party_name),
        "主要违法违规事实": row_data.get("主要违法违规事实", ""),
        "行政处罚依据": row_data.get("行政处罚依据", ""),
        "行政处罚内容": row_data.get("行政处罚内容", ""),
        "作出决定机关": row_data.get("作出决定机关", ""),
        "作出处罚决定的日期": row_data.get("作出处罚决定的日期", ""),
        "源链接": source_url,
    }

    # 保留原始表中的额外字段
    standard_fields = set(record.keys())
    for k, v in row_data.items():
        if k not in standard_fields and v:
            record[k] = v

    return enrich_record(record)


def parse_kv_table(table: pd.DataFrame, doc_id: int) -> dict | None:
    """解析两列键值对格式的表格"""
    source_url = DETAIL_URL_TEMPLATE.format(doc_id=doc_id)
    kv = {}
    for _, row in table.iterrows():
        key = clean_text(row.iloc[0])
        val = clean_text(row.iloc[1])
        if key:
            mapped = map_header(key)
            kv[mapped] = val

    if not kv:
        return None

    party_name = kv.get("当事人名称", "")
    record = {
        "处罚决定书文号": kv.get("处罚决定书文号", ""),
        "当事人名称": party_name,
        "当事人类型": classify_party_type(party_name),
        "主要违法违规事实": kv.get("主要违法违规事实", ""),
        "行政处罚依据": kv.get("行政处罚依据", ""),
        "行政处罚内容": kv.get("行政处罚内容", ""),
        "作出决定机关": kv.get("作出决定机关", ""),
        "作出处罚决定的日期": kv.get("作出处罚决定的日期", ""),
        "源链接": source_url,
    }
    return enrich_record(record)


def parse_non_table_html(html_content: str, doc_id: int) -> list[dict]:
    """处理非表格型的 HTML 内容 (处罚决定书等纯文本)"""
    source_url = DETAIL_URL_TEMPLATE.format(doc_id=doc_id)

    text = re.sub(r'<[^>]+>', '\n', html_content)
    text = re.sub(r'\n+', '\n', text).strip()

    if not text:
        return []

    record = {
        "处罚决定书文号": "",
        "当事人名称": "",
        "当事人类型": "未知",
        "主要违法违规事实": "",
        "行政处罚依据": "",
        "行政处罚内容": "",
        "作出决定机关": "",
        "作出处罚决定的日期": "",
        "源链接": source_url,
        "原始文本": text[:1000],
    }

    party_match = re.search(r'当事人[：:]\s*(.+?)(?:\n|，|,)', text)
    if party_match:
        record["当事人名称"] = party_match.group(1).strip()
        record["当事人类型"] = classify_party_type(record["当事人名称"])

    return [enrich_record(record)]


# ============ 主流程 ============

def scrape_penalties(test_mode: bool = False, test_pages: int = 1, max_pages: int = DEFAULT_MAX_PAGES):
    """
    主爬取函数, 支持断点续爬。
    :param test_mode: 测试模式
    :param test_pages: 测试模式下爬取的页数
    :param max_pages: 最大爬取页数, 默认179页
    """
    ensure_cache_dir()

    # 第一步: 获取文档列表
    print("=" * 60)
    print("第1步: 抓取文档列表...")
    print("=" * 60)

    if test_mode:
        # 测试模式不用缓存
        total = get_total_count()
        print(f"共 {total} 条记录, 测试模式只爬 {test_pages} 页")
        all_rows = []
        for p in range(1, test_pages + 1):
            params = {"itemId": ITEM_ID, "pageSize": str(PAGE_SIZE), "pageIndex": str(p)}
            resp = requester.get(LIST_API, params=params)
            if resp:
                all_rows.extend(resp.json()["data"]["rows"])
            requester._smart_delay()
        doc_rows = all_rows
    else:
        doc_rows = fetch_doc_list(max_pages=max_pages)

    print(f"获取到 {len(doc_rows)} 条文档")

    if not doc_rows:
        print("未获取到任何文档, 退出")
        return pd.DataFrame()

    # 第二步: 逐条获取详情并解析 (支持断点续爬)
    print("\n" + "=" * 60)
    print("第2步: 抓取并解析详情...")
    print("=" * 60)

    # 加载已完成的详情缓存
    detail_cache = {} if test_mode else load_detail_cache()
    print(f"已缓存 {len(detail_cache)} 条详情")

    all_records = []
    failed_docs = []
    new_count = 0

    for i, doc in enumerate(tqdm(doc_rows, desc="解析详情")):
        doc_id = str(doc["docId"])
        doc_subtitle = doc.get("docSubtitle", "")
        doc_title = doc.get("docTitle", "")
        publish_date = doc.get("publishDate", "")

        # 检查缓存
        if doc_id in detail_cache:
            records = detail_cache[doc_id]
        else:
            html_content = fetch_detail_html(int(doc_id))

            if html_content is None:
                failed_docs.append(doc_id)
                requester._smart_delay()
                continue

            records = parse_penalty_table(html_content, int(doc_id))

            if not records:
                source_url = DETAIL_URL_TEMPLATE.format(doc_id=doc_id)
                records = [{
                    "处罚决定书文号": "",
                    "当事人名称": "",
                    "当事人类型": "未知",
                    "主要违法违规事实": "",
                    "行政处罚依据": "",
                    "行政处罚内容": "",
                    "作出决定机关": "",
                    "作出处罚决定的日期": "",
                    "源链接": source_url,
                    "原始文本": "解析失败, 请手动查看源链接",
                }]

            # 保存到缓存
            if not test_mode:
                detail_cache[doc_id] = records

            new_count += 1
            requester._smart_delay()

            # 增量保存
            if not test_mode and new_count % SAVE_INTERVAL == 0:
                save_detail_cache(detail_cache)
                print(f"\n  [进度保存] 新增 {new_count} 条, 缓存总计 {len(detail_cache)} 条")

        # 给每条记录添加文档元数据
        for r in records:
            r["发布日期"] = publish_date
            r["文档标题"] = clean_text(doc_subtitle or doc_title)

        all_records.extend(records)

    # 最终保存缓存
    if not test_mode:
        save_detail_cache(detail_cache)

    # 第三步: 整理结果
    print("\n" + "=" * 60)
    print("第3步: 整理结果...")
    print("=" * 60)

    result_df = pd.DataFrame(all_records)

    if not result_df.empty:
        desired_cols = [
            "处罚决定书文号", "当事人名称", "当事人类型", "所属机构",
            "主要违法违规事实", "行政处罚依据", "行政处罚内容",
            "处罚类型", "罚款金额(元)",
            "作出决定机关", "所属省份", "作出处罚决定的日期",
            "发布日期", "文档标题", "源链接",
        ]
        extra_cols = [c for c in result_df.columns if c not in desired_cols]
        final_cols = [c for c in desired_cols if c in result_df.columns] + extra_cols
        result_df = result_df[final_cols]

    if failed_docs:
        print(f"\n失败文档数: {len(failed_docs)}")

    print(f"共解析出 {len(result_df)} 条处罚记录")
    print(f"请求统计: {requester.stats()}")
    return result_df


def save_to_excel(df: pd.DataFrame, filename: str = OUTPUT_FILE):
    """保存到 Excel"""
    output_path = Path(__file__).parent / filename
    df.to_excel(output_path, index=False, engine="openpyxl")
    print(f"\n已保存到: {output_path}")
    return output_path


# ============ 入口 ============

if __name__ == "__main__":
    import sys

    test = "--test" in sys.argv
    clean_cache = "--clean" in sys.argv

    # 解析 --pages=N 参数
    max_pages = DEFAULT_MAX_PAGES
    for arg in sys.argv:
        if arg.startswith("--pages="):
            try:
                max_pages = int(arg.split("=")[1])
            except ValueError:
                print(f"无效的页数参数: {arg}, 使用默认值 {DEFAULT_MAX_PAGES}")

    if clean_cache:
        print("清除缓存...")
        if CACHE_DIR.exists():
            for f in CACHE_DIR.iterdir():
                f.unlink()
            print("缓存已清除")

    if test:
        print(">>> 测试模式: 只爬取第1页 <<<\n")
        df = scrape_penalties(test_mode=True)
    else:
        print(">>> 全量爬取模式 (支持断点续爬) <<<")
        print(f"延迟: {DELAY_MIN}-{DELAY_MAX}s | 重试: {MAX_RETRIES}次 | 退避: {BACKOFF_BASE}-{BACKOFF_MAX}s")
        print(f"pageSize: {PAGE_SIZE} | 最大页数: {max_pages} | 增量保存: 每{SAVE_INTERVAL}条\n")
        df = scrape_penalties(max_pages=max_pages)

    if not df.empty:
        save_to_excel(df)
        print(f"\n前5条数据预览:")
        pd.set_option('display.max_colwidth', 30)
        pd.set_option('display.width', 200)
        print(df.head().to_string())
