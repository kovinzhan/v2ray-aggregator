"""
国家/地区映射数据及提取逻辑
"""

import re

# 国家关键词映射表
COUNTRY_MAP = {
    # 中文名称
    "美国": "美国", "香港": "香港", "台湾": "台湾", "日本": "日本",
    "韩国": "韩国", "新加坡": "新加坡", "英国": "英国", "德国": "德国",
    "法国": "法国", "加拿大": "加拿大", "澳大利亚": "澳大利亚",
    "澳洲": "澳大利亚", "印度": "印度", "俄罗斯": "俄罗斯",
    "荷兰": "荷兰", "巴西": "巴西", "土耳其": "土耳其",
    "阿根廷": "阿根廷", "越南": "越南", "泰国": "泰国",
    "马来西亚": "马来西亚", "印尼": "印尼", "菲律宾": "菲律宾",
    "意大利": "意大利", "西班牙": "西班牙", "瑞士": "瑞士",
    "瑞典": "瑞典", "挪威": "挪威", "芬兰": "芬兰",
    "波兰": "波兰", "乌克兰": "乌克兰", "以色列": "以色列",
    "南非": "南非", "墨西哥": "墨西哥", "智利": "智利",
    "哥伦比亚": "哥伦比亚", "爱尔兰": "爱尔兰", "新西兰": "新西兰",
    "埃及": "埃及", "罗马尼亚": "罗马尼亚", "捷克": "捷克",
    "匈牙利": "匈牙利", "奥地利": "奥地利", "比利时": "比利时",
    "丹麦": "丹麦", "葡萄牙": "葡萄牙", "希腊": "希腊",
    "哈萨克斯坦": "哈萨克斯坦", "巴基斯坦": "巴基斯坦",
    "孟加拉": "孟加拉", "尼日利亚": "尼日利亚",
    # 英文国家代码 / 名称
    "US": "美国", "USA": "美国", "United States": "美国", "America": "美国",
    "HK": "香港", "Hong Kong": "香港", "Hongkong": "香港",
    "TW": "台湾", "Taiwan": "台湾",
    "JP": "日本", "Japan": "日本",
    "KR": "韩国", "Korea": "韩国", "South Korea": "韩国",
    "SG": "新加坡", "Singapore": "新加坡",
    "UK": "英国", "GB": "英国", "United Kingdom": "英国", "England": "英国",
    "DE": "德国", "Germany": "德国",
    "FR": "法国", "France": "法国",
    "CA": "加拿大", "Canada": "加拿大",
    "AU": "澳大利亚", "Australia": "澳大利亚",
    "IN": "印度", "India": "印度",
    "RU": "俄罗斯", "Russia": "俄罗斯",
    "NL": "荷兰", "Netherlands": "荷兰",
    "BR": "巴西", "Brazil": "巴西",
    "TR": "土耳其", "Turkey": "土耳其", "Türkiye": "土耳其",
    "AR": "阿根廷", "Argentina": "阿根廷",
    "VN": "越南", "Vietnam": "越南",
    "TH": "泰国", "Thailand": "泰国",
    "MY": "马来西亚", "Malaysia": "马来西亚",
    "ID": "印尼", "Indonesia": "印尼",
    "PH": "菲律宾", "Philippines": "菲律宾",
    "IT": "意大利", "Italy": "意大利",
    "ES": "西班牙", "Spain": "西班牙",
    "CH": "瑞士", "Switzerland": "瑞士",
    "SE": "瑞典", "Sweden": "瑞典",
    "NO": "挪威", "Norway": "挪威",
    "FI": "芬兰", "Finland": "芬兰",
    "PL": "波兰", "Poland": "波兰",
    "UA": "乌克兰", "Ukraine": "乌克兰",
    "IL": "以色列", "Israel": "以色列",
    "ZA": "南非", "South Africa": "南非",
    "MX": "墨西哥", "Mexico": "墨西哥",
    "CL": "智利", "Chile": "智利",
    "CO": "哥伦比亚", "Colombia": "哥伦比亚",
    "IE": "爱尔兰", "Ireland": "爱尔兰",
    "NZ": "新西兰", "New Zealand": "新西兰",
}

# 国旗 emoji 映射
FLAG_MAP = {
    "🇺🇸": "美国", "🇭🇰": "香港", "🇹🇼": "台湾", "🇯🇵": "日本",
    "🇰🇷": "韩国", "🇸🇬": "新加坡", "🇬🇧": "英国", "🇩🇪": "德国",
    "🇫🇷": "法国", "🇨🇦": "加拿大", "🇦🇺": "澳大利亚", "🇮🇳": "印度",
    "🇷🇺": "俄罗斯", "🇳🇱": "荷兰", "🇧🇷": "巴西", "🇹🇷": "土耳其",
    "🇦🇷": "阿根廷", "🇻🇳": "越南", "🇹🇭": "泰国", "🇲🇾": "马来西亚",
    "🇮🇩": "印尼", "🇵🇭": "菲律宾", "🇮🇹": "意大利", "🇪🇸": "西班牙",
    "🇨🇭": "瑞士", "🇸🇪": "瑞典", "🇳🇴": "挪威", "🇫🇮": "芬兰",
    "🇵🇱": "波兰", "🇺🇦": "乌克兰", "🇮🇱": "以色列", "🇿🇦": "南非",
    "🇲🇽": "墨西哥",
}

# 2字母国家代码列表（从 COUNTRY_MAP 中提取）
_TWO_LETTER_CODES = [k for k in COUNTRY_MAP if len(k) == 2 and k.isalpha() and k.isupper()]


def extract_country(original_name):
    """从节点名称中提取国家/地区，返回中文名或 "未知" """
    if not original_name:
        return "未知"

    # 先检查国旗 emoji
    for flag, country in FLAG_MAP.items():
        if flag in original_name:
            return country

    # 优先匹配中文及英文长关键词
    for keyword, country in COUNTRY_MAP.items():
        if len(keyword) >= 2 and keyword in original_name:
            return country

    # 匹配2字母国家代码（需在开头且后接分隔符）
    name_upper = original_name.upper()
    for code in _TWO_LETTER_CODES:
        if re.match(rf'^{code}(?=[\W_]|$)', name_upper):
            return COUNTRY_MAP[code]

    return "未知"
