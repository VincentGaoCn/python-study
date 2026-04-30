"""调试: 查看表格解析结果"""
import requests
import pandas as pd
from io import StringIO

HEADERS = {
    'Accept': '*/*',
    'Host': 'www.nfra.gov.cn',
    'Referer': 'https://www.nfra.gov.cn/cn/view/pages/ItemList.html',
    'X-Requested-With': 'XMLHttpRequest',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
}

for doc_id in [1256138, 1256113, 1255921, 1255879, 1255654]:
    url = f'https://www.nfra.gov.cn/cn/static/data/DocInfo/SelectByDocId/data_docId={doc_id}.json'
    resp = requests.get(url, headers=HEADERS, timeout=30)
    data = resp.json()['data']
    html = data.get('docClob', '')
    print(f'\n{"="*80}')
    print(f'docId={doc_id}  title={data.get("docSubtitle", "")[:60]}')
    print(f'{"="*80}')

    try:
        tables = pd.read_html(StringIO(html))
        for i, t in enumerate(tables):
            print(f'\n--- Table {i}: shape={t.shape} ---')
            pd.set_option('display.max_colwidth', 40)
            pd.set_option('display.width', 200)
            print(t.to_string())
    except Exception as e:
        print(f'解析失败: {e}')
