"""
国家/地区映射数据及提取逻辑

数据结构：以规范中文名为 key，别名为 list（含简体/繁体/英文/代码）
查找时构建按长度降序的别名列表，优先匹配长关键词避免歧义
"""

import re

# ============================================================
# 国家 → 别名列表
# ============================================================
COUNTRIES = {
    "美国": ["美国", "美國", "US", "USA", "United States", "America"],
    "香港": ["香港", "HK", "Hong Kong", "Hongkong"],
    "台湾": ["台湾", "台灣", "臺灣", "TW", "Taiwan"],
    "日本": ["日本", "JP", "Japan"],
    "韩国": ["韩国", "韓國", "KR", "Korea", "South Korea"],
    "新加坡": ["新加坡", "SG", "Singapore"],
    "英国": ["英国", "英國", "UK", "GB", "United Kingdom", "England"],
    "德国": ["德国", "德國", "DE", "Germany"],
    "法国": ["法国", "法國", "FR", "France"],
    "加拿大": ["加拿大", "CA", "Canada"],
    "澳大利亚": ["澳大利亚", "澳大利亞", "澳洲", "AU", "Australia"],
    "印度": ["印度", "IN", "India"],
    "俄罗斯": ["俄罗斯", "俄羅斯", "RU", "Russia"],
    "荷兰": ["荷兰", "荷蘭", "NL", "Netherlands"],
    "巴西": ["巴西", "BR", "Brazil"],
    "土耳其": ["土耳其", "TR", "Turkey", "Türkiye"],
    "阿根廷": ["阿根廷", "AR", "Argentina"],
    "越南": ["越南", "VN", "Vietnam"],
    "泰国": ["泰国", "泰國", "TH", "Thailand"],
    "马来西亚": ["马来西亚", "馬來西亞", "MY", "Malaysia"],
    "印尼": ["印尼", "ID", "Indonesia"],
    "菲律宾": ["菲律宾", "菲律賓", "PH", "Philippines"],
    "意大利": ["意大利", "義大利", "IT", "Italy"],
    "西班牙": ["西班牙", "ES", "Spain"],
    "瑞士": ["瑞士", "CH", "Switzerland"],
    "瑞典": ["瑞典", "SE", "Sweden"],
    "挪威": ["挪威", "NO", "Norway"],
    "芬兰": ["芬兰", "芬蘭", "FI", "Finland"],
    "波兰": ["波兰", "波蘭", "PL", "Poland"],
    "乌克兰": ["乌克兰", "烏克蘭", "UA", "Ukraine"],
    "以色列": ["以色列", "IL", "Israel"],
    "南非": ["南非", "ZA", "South Africa"],
    "墨西哥": ["墨西哥", "MX", "Mexico"],
    "智利": ["智利", "CL", "Chile"],
    "哥伦比亚": ["哥伦比亚", "哥倫比亞", "CO", "Colombia"],
    "爱尔兰": ["爱尔兰", "愛爾蘭", "IE", "Ireland"],
    "新西兰": ["新西兰", "新西蘭", "NZ", "New Zealand"],
    "埃及": ["埃及", "EG", "Egypt"],
    "罗马尼亚": ["罗马尼亚", "羅馬尼亞", "RO", "Romania"],
    "捷克": ["捷克", "CZ", "Czech"],
    "匈牙利": ["匈牙利", "HU", "Hungary"],
    "奥地利": ["奥地利", "奧地利", "AT", "Austria"],
    "比利时": ["比利时", "比利時", "BE", "Belgium"],
    "丹麦": ["丹麦", "丹麥", "DK", "Denmark"],
    "葡萄牙": ["葡萄牙", "PT", "Portugal"],
    "希腊": ["希腊", "希臘", "GR", "Greece"],
    "哈萨克斯坦": ["哈萨克斯坦", "哈薩克斯坦", "KZ", "Kazakhstan"],
    "巴基斯坦": ["巴基斯坦", "PK", "Pakistan"],
    "孟加拉": ["孟加拉", "BD", "Bangladesh"],
    "尼日利亚": ["尼日利亚", "尼日利亞", "NG", "Nigeria"],
    "伊朗": ["伊朗", "IR", "Iran"],
    "欧盟": ["欧盟", "歐盟", "EU"],
    "阿联酋": ["阿联酋", "阿聯酋", "AE", "UAE", "United Arab Emirates"],
    "塞尔维亚": ["塞尔维亚", "塞爾維亞", "RS", "Serbia"],
    "保加利亚": ["保加利亚", "保加利亞", "BG", "Bulgaria"],
    "斯洛文尼亚": ["斯洛文尼亚", "斯洛文尼亞", "SI", "Slovenia"],
    "立陶宛": ["立陶宛", "LT", "Lithuania"],
    "阿富汗": ["阿富汗", "AF", "Afghanistan"],
    "委内瑞拉": ["委内瑞拉", "委內瑞拉", "VE", "Venezuela"],
    "亚太": ["亚太", "亞太", "Asia Pacific", "亞太地區", "亚太地区"],
}

# ============================================================
# 构建查找索引（模块加载时一次性完成）
# ============================================================
# 别名 → 国家，按长度降序排列，优先匹配长关键词
_ALIASES_SORTED = sorted(
    ((alias, country) for country, aliases in COUNTRIES.items() for alias in aliases),
    key=lambda x: len(x[0]),
    reverse=True,
)

# 2字母代码 → 国家 的查找表（需在开头且后接分隔符才匹配，避免误匹配）
_TWO_LETTER_MAP = {
    a: c for a, c in _ALIASES_SORTED
    if len(a) == 2 and a.isalpha() and a.isupper()
}


def extract_country(original_name):
    """从节点名称中提取国家/地区，返回中文名或 "未知" """
    if not original_name:
        return "未知"

    # 按长度降序匹配别名（长关键词优先，避免短词误匹配）
    for alias, country in _ALIASES_SORTED:
        if alias in original_name:
            return country

    # 匹配2字母国家代码（需在开头且后接分隔符）
    name_upper = original_name.upper()
    for code, country in _TWO_LETTER_MAP.items():
        if re.match(rf'^{code}(?=[\W_]|$)', name_upper):
            return country

    return "未知"
