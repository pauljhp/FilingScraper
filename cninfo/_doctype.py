cninfo_doctypes = {
    'periodic_report': '度报告',
    'interim_report': '半年度报告$',
    'annual_report': '年年度报告$',
    'quarterly_report': '季度报告全文$',
    'presentation': '演示文稿',
    'prospectus': '首次公开发行股票招股说明书',
    'esg_report': '[(ESG)(社会责任)]',
    'all': ''
}

filing_list_flds = {
    "TEXTID": "text_id",		
    "RECID": "record_id",	
    "SECCODE": "ticker",	
    "SECNAME": "security_name",
    "F001D": "annoucement_date",	
    "F002V": "annoucement_title",	
    "F003V": "url",
    "F004V": "announcement_format", 	
    "F005N": "announcement_size",		
    "F006V": "info_type_code",
    "F007V": "security_type_code",		
    "F008V": "security_type_name",	
    "F009V": "market_code",
    "F010V": "market_name",
    "OBJECTID": "object_id",
    "RECTIME": "annoucement_time",
    }

filing_list_types = {
    "TEXTID": str,		
    "RECID": str,	
    "SECCODE": str,	
    "SECNAME": str,
    "F001D": 'datetime64[ns]',	
    "F002V": str,	
    "F003V": str,
    "F004V": str, 	
    "F005N": int,		
    "F006V": str,
    "F007V": str,		
    "F008V": str,	
    "F009V": str,
    "F010V": str,
    "OBJECTID": str,
    "RECTIME": 'datetime64[ns]',
    }