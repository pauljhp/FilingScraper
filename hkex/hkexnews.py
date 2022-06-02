from __future__ import annotations # to enable type hints when returning self. not needed in python 3.11
import requests

import re
from typing import Union, Optional, List, Dict, Any
from types import SimpleNamespace
import json
from urllib.parse import urljoin
import pandas as pd
import numpy as np
import datetime as dt
import PyPDF2
from ..utils import config, iter_by_chunk
from .._Abstract_scraper import AbstractScraper
from . import _filetypes
from argparse import ArgumentParser
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import deque

hkexnews_doc_types = _filetypes.hkexnews_doc_types

class HKEXNews(AbstractScraper):
    def __init__(self, config: Optional[Union[str, config]]=None, 
        db_path: Optional[str]="hkexnews.db", **kwargs):
        """
        :param config: the config file path or config object
        :param db_path: the path to the sqlite database. Must be specified if 
            not specified in the config file
        """
        super(HKEXNews, self).__init__(config=config, db_path=db_path, **kwargs)
        self.endpoint = 'https://www1.hkexnews.hk/'
    def __get_stock_info(self, keyword: str) -> dict:
        params = self.params.copy()
        params.update(dict(callback="callback",
                    lang="EN",
                    type="A",
                    name=str(keyword),
                    market="SEHK"))
        headers = self.headers
        headers.update(dict(referer=self.endpoint, Accept='application/json'))
        url = urljoin(self.endpoint, "/search/prefix.do")
        res = self.session.get(url, params=params, headers=headers, timeout=self.timeout)
        return res

    def _get_stock_info(self, keyword: str) -> dict:
        res = self.__get_stock_info(keyword)
        res = re.findall("(?:callback\()(.*?)(?:\);\n)", res.text)[0]
        res = json.loads(res).get('stockInfo')[0]
        return {str(k): str(v) for k, v in res.items()}

    def get_filing_list(self, keyword: str, start_date: dt.date=dt.date(1999, 12, 31),
        end_date: dt.dated=dt.date.today(), doctype='all', ascending: bool=False,
        verbose: bool=False, save_to_sql=False) -> pd.DataFrame:
        """return filing list for a given stock
        :param keyword: the stock name or symbol
        :param start_date: the start date of filings
        :param end_date: the end date of filings
        :param doctype: the type of filing, default is all
        :param ascending: sort the list in ascending order
        :param verbose: print the progress if True"""
        url = urljoin(self.endpoint, "search/titleSearchServlet.do")
        headers = self.headers.copy()
        params = self.params.copy()
        stock_info = self._get_stock_info(keyword)
        params.update({
            'stockId': stock_info.get('stockId'),
            'fromDate': start_date.strftime("%Y%m%d"),		
            'toDate': end_date.strftime("%Y%m%d"),
            'searchType': "1", 
            'documentType': "-1",
            't2code': hkexnews_doc_types.get(doctype, "-2"),
            'sortDir': "1" if ascending else "0",
            'category': "0",
            'rowRange': "100",
            "sortByOptions": "DateTime",
            "lang": "E"})
        res = self.session.get(url, headers=headers, params=params, timeout=self.timeout).json()
        def returndf(res: dict): 
            assert isinstance(res, dict) and 'hasNextRow' in res.keys()
            if not res.get('hasNextRow'):
                if 'result' in res.keys():
                    res = json.loads(res.get('result'))
                    df = pd.DataFrame([pd.Series(d) for d in res])
                    if verbose: print(f"found {df.TOTAL_COUNT[0]} results")
                    return df
            else:
                total_count = json.loads(res.get('result'))[0].get('TOTAL_COUNT')
                params.update({"rowRange": total_count})
                res = self.session.get(url, headers=headers, params=params, timeout=self.timeout).json()
                df = returndf(res)
            return df
        df = returndf(res)
        df = df.applymap(self.html_entities_to_unicode)
        df.loc[:, 'FILE_LINK'] = df.loc[:, 'FILE_LINK'].apply(lambda x: urljoin(self.endpoint, x))
        if save_to_sql:
            table_name = f"{keyword}_hkexnews_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}_{doctype}"
            self.frame_to_sql(df, table_name, if_exists='replace')
        return df
    
    def get_filing_content(self, keyword: str, start_date: dt.date=dt.date(1999, 12, 31),
        end_date: dt.dated=dt.date.today(), doctype='all', ascending: bool=False,
        verbose: bool=False, save_to_sql=False, convert_to_text=False, 
        ignore_errors: bool=False) -> pd.DataFrame:
        """return filing list for a given stock
        :param keyword: the stock name or symbol
        :param start_date: the start date of filings
        :param end_date: the end date of filings
        :param doctype: the type of filing, default is all
        :param ascending: sort the list in ascending order
        :param verbose: print the progress if True"""
        df = self.get_filing_list(keyword, start_date, end_date, doctype, 
            ascending, verbose)
        for i, url in df.FILE_LINK.iteritems():
            df.loc[i, 'FILE_CONTENT'] = self.session.get(url, timeout=self.timeout).content
        if convert_to_text:
            try:
                df.loc[:, 'FILE_CONTENT'] = df.loc[:, 'FILE_CONTENT'].apply(self.pdf_to_text)
            except Exception as e:
                if ignore_errors:
                    print(e)
                    pass
                else:
                    raise e
        if save_to_sql:
            table_name = f"{keyword}_hkexnews_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}_{doctype}"
            self.frame_to_sql(df, table_name, if_exists='replace')
        return df

    @classmethod
    def batch_download(cls, stock_list: List[str], verbose=False, 
        ignore_errors: bool=False, max_workers: int=0, 
        start_date: dt.date=dt.date(2015, 12, 31), 
        end_date: dt.date=dt.date.today(), doctype='annual_report',
        **kwargs):
        """download filing list for a list of stocks
        :param stock_list: a list of stock names or symbols
        :param verbose: print the progress if True
        :param ignore_errors: ignore errors if True
        :param max_workers: the number of workers to download the data. If set to <= 1, will not use multithreading
        """
        if max_workers > 1: # use multithreading
            for chunk in iter_by_chunk(stock_list, max_workers):
                if verbose: print(chunk)
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = [executor.submit(cls(**kwargs).get_filing_content, 
                        keyword=stock_name, 
                        save_to_sql=True, 
                        verbose=verbose, start_date=start_date,
                        end_date=end_date, 
                        doctype=doctype,
                        convert_to_text=kwargs.get('convert_to_text', False),
                        ignore_errors=ignore_errors)
                        for stock_name in chunk]
                    for future in as_completed(futures):
                        _ = future.result()

        else: # single threaded execution
            queue = deque() 
            for stock_name in stock_list:
                tablename = f"{stock_name}_hkexnews_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}_{doctype}"
                if tablename not in cls(**kwargs).existing_tables:
                    queue.append(stock_name)
            while queue:
                stock_name = queue.popleft()
                try:
                    if verbose: print(stock_name)
                    cls(**kwargs).get_filing_content(keyword=stock_name, save_to_sql=True, 
                        verbose=verbose, 
                        start_date=start_date,
                        end_date=end_date, 
                        doctype=doctype,
                        convert_to_text=kwargs.get('convert_to_text', False),
                        ignore_errors=ignore_errors
                    )
                except Exception as e:
                    if verbose: print(e)
                    if not ignore_errors: raise e
                    else: pass

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('-SP', '--stocks_path', type=str, default='stock_list.txt',
        help='path to file that stores the stock list, which should be *.txt. Seperate stocks by the newline character',)
    parser.add_argument('-S', '--stock_list', type=str, nargs='+', 
        help='stock list to be processed')
    parser.add_argument('-D', '--db_path', type=str, default='hkexnews.db', 
        help='path to sqlite database')
    parser.add_argument('-s', '--start_date', type=str, default='20151231',
        help='start date of the query range')
    parser.add_argument('-e', '--end_date', type=str, 
        default=dt.date.today().strftime('%Y%m%d'),
        help='end date of the query range')
    parser.add_argument('-C', '--config', type=str, 
        default='./FilingScraper/config/hkex_config.json',
        help='path to the config file, which should be *.json')
    parser.add_argument('-V', '--verbose', action='store_true',
        help='print the progress')
    parser.add_argument('-d', '--doctype', type=str, default='annual_report',
        help='the type of filing, default is all. Specify --display_doctype_list to see the list')
    parser.add_argument('--display_doctype_list', action='store_true',
        help='display the list of available document types, wont do anything else if specified')
    parser.add_argument('-ct', '--convert_to_text', action='store_true',
        help='if specified, will convert the pdf files to text')
    parser.add_argument('-M', '--maxworker', default=0, type=int,
        help='max workers allowed for multithreading. Default 0, if set to <=1, will not use multithreading')
    parser.add_argument('-I', '--ignore_errors', action='store_true',
        help='if specified, will ignore errors and continue')
    args = parser.parse_args()
    if args.display_doctype_list:
        print(list(hkexnews_doc_types.keys()))
    else:
        if not args.stock_list:
            stocks_path = args.stocks_path
            with open(stocks_path, "r") as f:
                stock_list = f.read()
                stock_list = stock_list.split("\n")
        else:
            stock_list = args.stock_list
        HKEXNews().batch_download(stock_list=stock_list, 
            verbose=args.verbose, start_date=dt.datetime.strptime(args.start_date, '%Y%m%d').date(),
            end_date=dt.datetime.strptime(args.end_date, '%Y%m%d').date(),
            config=args.config,
            dbpath=args.db_path, 
            doctype=args.doctype,
            convert_to_text=args.convert_to_text,
            max_workers=args.maxworker,
            ignore_errors=args.ignore_errors
            )