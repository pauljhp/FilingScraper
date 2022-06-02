"""API for CNINFO on Chinese stocks"""
from __future__ import annotations

import re
from typing import Dict, Any, Union, Optional, List
from argparse import ArgumentParser
import datetime as dt
from urllib.parse import urljoin
import pandas as pd
import numpy as np
import requests
from ..utils import config, iter_by_chunk
from .._Abstract_scraper import AbstractScraper
from ._doctype import cninfo_doctypes, filing_list_flds, filing_list_types

class CNInfo(AbstractScraper):
    def get_token(self, **kwargs) -> str:
        """get the token for the API"""
        url = urljoin(self.endpoint, "api-cloud-platform/oauth2/token")
        cred = self.config.get(name='credentials', default=dict())
        res = self.session.post(url, data=cred).json()
        if 'access_token' in res.keys() and res.get('expires_in') > 0:
            return res['access_token']
        else:
            raise ValueError(f'Cannot get token; please check credentials\n The following is returned from the server: {res}')

    def __init__(self, config: Optional[Union[str, config]]=None,
        db_path: Optional[str]='cninfo.db', **kwargs):
        """
        :param config: config path or config object
        :param db_path: path to the database for storage
        """
        super(CNInfo, self).__init__(config=config, db_path=db_path, **kwargs)
        self.endpoint = 'http://webapi.cninfo.com.cn/'
        self.token = self.get_token()
    
    def _get_category_df(self) -> None:
        """get filing categories which will be saved in self.category_df_"""
        cat_dict = self.session.get(
            urljoin(self.endpoint, "api/public/p_public0006"),
            params=dict(access_token=self.token)
        ).json() # category lookup
        ls = []
        for i, d in enumerate(cat_dict.get('records')):
            s = pd.DataFrame(data=d.values(), index=list(d.keys()), columns=[i])
            ls.append(s.T)
        self.category_df_ = pd.concat(ls)
        self.category_df_.columns = self.category_df.columns.str.replace("SORTCODE", "F006V") # TODO - this seems to be wrong field returned by CNINFO but can't find where the correct field is.       

    def get_filing_list(self, ticker: str, start_date: dt.date=dt.date(2015, 12, 31),
        end_date: dt.date=dt.date.today(), return_format: str='json',
        doctype: str='all', verbose: bool=False, **kwargs) -> pd.DataFrame:
        """get a list of the filings for a stock
        :param ticker: the ticker of the stock
        :param start_date: the start date of the filings
        :param end_date: the end date of the filings
        
        :param return_format: the format of the returned data. Takes 'xml', 
            'json', 'csv' and 'dbf'. 'json' by default
        :param doctype: the type of the filing. The following are specified in 
            English already:
            - 'all': default, will not filter anything
            - 'annual_report': 
            - 'quarterly_report'
            - 'prospectus'
            - 'periodic_report': all periodic reports including quarterly and annual
        :param kwargs: takes these keyword arguments:
            - market: the market of the stock. Takes the following:
                - '012001': Shanghai Exchange 
                - '012029': STAR board
                - '012002': Shenzhen Mainboard
                - '012015': Shenzhen ChiNext Board
            - textid
            - maxid
            check http://webapi.cninfo.com.cn/#/apiDoc > p_info3015 for more information
        """
        url = urljoin(self.endpoint, 'api/info/p_info3015')
        res = self.session.post(url, data=dict(scode=ticker,
            access_token=self.token,
            sdate=start_date.strftime('%Y%m%d'),
            edate=end_date.strftime('%Y%m%d'),
            format=return_format,
            **kwargs
            )).json()
        if res.get('resultmsg') == 'success' and res.get('total') > 0:
            df = pd.concat([pd.Series(s).to_frame().T 
                for s in res['records']])
            df = df.loc[df.F002V.str.contains(
                cninfo_doctypes.get(doctype, ''), regex=True), :]
            df.loc[:, 'F001D'] = df.loc[:, 'F001D'].apply(lambda d: dt.datetime.strptime(d, '%Y-%m-%d %H:%M:%S'))
            df.loc[:, 'RECTIME'] = df.loc[:, 'RECTIME'].apply(lambda d: dt.datetime.strptime(d, '%Y-%m-%d %H:%M:%S'))
            for colname, col in df.iteritems():
                df.loc[:, colname] = col.astype(filing_list_types.get(colname, str)).values
            df.columns = df.columns.to_series().map(filing_list_flds)
            return df
        else:
            raise ValueError(f"Cannot get list of filings; error message: {res.get('resultmsg')};\nerror code: {res.get('resultcode')}")

    def get_filing_content(self, ticker: str, start_date: dt.date=dt.date(2015, 12, 31),
        end_date: dt.date=dt.date.today(), return_format: str='json',
        doctype: str='all', verbose: bool=False, **kwargs) -> pd.DataFrame:
        """get the content of the filings for a stock"""
        df = self.get_filing_list(ticker=ticker, start_date=start_date,
            end_date=end_date, return_format=return_format, doctype=doctype, 
            verbose=verbose, **kwargs)
        session = requests.Session()
        session.mount("http://", self.adapter) # filings are accessible via http only for cninfo
        df.loc[:, 'filing_content'] = df.loc[:, 'url'].apply(
            lambda u: self.get_pdf(u, session=session, timeout=self.timeout))
        return df
        raise NotImplementedError

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
        default='./FilingScraper/config/cninfo_config.json',
        help='path to the config file, which should be *.json')