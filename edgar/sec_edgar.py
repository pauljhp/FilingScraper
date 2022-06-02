"""extract text from filings on EDGAR"""
from __future__ import annotations
import re, os
from typing import Union, Optional, List, Dict, Any
from ..utils import config
from .._Abstract_scraper import AbstractScraper
from urllib.parse import urljoin

class SECEdgar(AbstractScraper):
    def __init__(self,  financial_modeling_prep_key: str,
        config: Optional[Union[str, config]]=None, 
        db_path: Optional[str]='secedgar.db', 
        **kwargs):
        """
        :param config: config path or config object
        :param db_path: path to the database for storage
        """
        super(SECEdgar, self).__init__(config=config, db_path=db_path, **kwargs)
        self.fmp_endpoint = 'https://financialmodelingprep.com/api/v3/'
        self.endpoint = 'https://www.sec.gov/Archives/edgar/data/'
        self.fmp_key = financial_modeling_prep_key
        res = self.session.get(
            urljoin(self.fmp_endpoint,'financial-statement-symbol-lists'),
            params={'apikey': self.fmp_key}
            )
        self.available_stocks = res.json()
    def get_filing_list(self, **kwargs) -> pd.DataFrame:
        """get a list of the filings for a stock"""
        raise NotImplementedError
    def get_filing_content(self, **kwargs) -> pd.DataFrame:
        """get the content of the filings for a stock"""
        raise NotImplementedError