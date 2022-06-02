"""Abstract scraper class. Inheret from this when building your own scraper
for different exchanges."""

from __future__ import annotations
from typing import List, Dict, Any, Optional, Union
import json
from types import SimpleNamespace
from . import utils
import requests
import sqlite3
import PyPDF2
from bs4 import BeautifulSoup as bs
import html
import io
from abc import ABC, abstractmethod
import re, os
import pandas as pd
from itertools import chain

class AbstractScraper(ABC):
    def get_existing_tables(self) -> List[str]:
        """get existing tables in the connected database.
        We can skip downloading if the data is already in the database"""
        sqlite_schema = self.cur.execute("""SELECT name
        FROM sqlite_schema
        WHERE type = 'table'
        ORDER BY name""").fetchall()
        sqlite_schema = list(chain(*sqlite_schema))
        return sqlite_schema

    def __init__(self, config: Optional[Union[str, utils.config]]=None, **kwargs):
        """
        :param config_path: The path to the config file.
            Wrap your configurations in a *.json file and pass the path to it. 
            Include the following keys: 
                - "headers": The headers to use for the request.
                - "method": post or get
                - "endpoint": If you have a specific endpoint
                - "params": The parameters to pass to the request. None if not
                    specified
                - "max_retries": The maximum number of retries to make. 3 by 
                    default if not specified.
                - "timeout": The timeout for the request. 10 seconds by default
        :param kwargs: You can pass the following keyword args:
            - db_path: str which will then be used as the database connection
        
        properties:
            - config: the config object (utils.config, which is inherited 
                from SimpleNamedspace)
            - headers: The default headers to use for the request. can be 
                updated via headers.setter. If need to update default headers
                and would like to not do it inplace, the update_headers() method
                can be used instead and pass inplace=False
            - params: default params to pass to get request, should be 
                overwritten when initiating a request
            - default_method: The default method to use for the request.
            - endpoint: The default endpoint to use for the request. 
                TODO - Some websites have multiple endpoints, do we seperate
                into two classes that inherit from the same parent or create 
                another object to store the endpoints?
            - max_retries: The maximum number of retries to make. 3 by default
            - timeout: The timeout for the request. 10 seconds by default
        """
        if config:
            if isinstance(config, str):
                self._config = json.load(open(config, "r"), 
                    object_hook=lambda d: utils.config(**d))
            else:
                assert isinstance(config, utils.config), "config must be a string or utils.config object"
                self._config = config
        else:
            self._config = utils.config()
        self._headers = self.config.get(name='headers', returntype='dict', default=dict())
        self._params = self.config.get(name='params', returntype='dict', default=dict())
        self._data = self.config.get(name='data', returntype='dict', default=dict())
        self._default_method = self.config.get(name='method', returntype='dict',  default='get')
        self._endpoint = self.config.get(name='endpoint', returntype='str',  default=None)
        
        max_retries =  self.config.get(name='max_retries', returntype='int',  default=3)
        assert isinstance(max_retries, int), "max_retries must be an integer"
        self._adapter = requests.adapters.HTTPAdapter(max_retries=max_retries)
        self.session = requests.Session()
        
        schema =  self.config.get(name='schema', returntype='str', default='https://')
        self.session.mount(schema, self.adapter)
        self._timeout =  self.config.get(name='timeout', returntype='int', default=10)
        assert isinstance(self.timeout, int), "timeout mut be an integer"
        if 'db_path' not in kwargs.keys():
            db_path = self.config.get(name='db_path', returntype='str', default=None)
        else: db_path = kwargs.get('db_path', None)
        self.sql_conn = sqlite3.connect(db_path)
        self.cur = self.sql_conn.cursor()
        self.existing_tables = self.get_existing_tables()
        __all__ = ['update_headers', 'close_sql_conn', 'frame_to_sql', 'get_filing_list']
    
    @property
    def config(self) -> utils.config:
        return self._config
    @config.setter
    def config(self, newconfig: utils.config):
        self._config = newconfig
    @config.deleter
    def config(self, verbose=False):
        if verbose:
            print("Deleting config. Press enter to confirm")
            input()
            del self._config
        else:
            del self._config
    
    @property
    def headers(self) -> Dict[str, str]:
        return self._headers
    @headers.setter
    def headers(self, newheaders: Dict[str, str]):
        self._headers = newheaders
    @headers.deleter
    def headers(self, verbose=False):
        if verbose:
            print("Deleting headers. Press enter to confirm")
            input()
            del self._headers
        else:
            del self._headers
    
    @property
    def params(self) -> Dict[str, str]:
        return self._params
    @params.setter
    def params(self, newparams: Dict[str, str]):
        self._params = newparams
    @params.deleter
    def params(self, verbose=False):
        if verbose:
            print("Deleting params. Press enter to confirm")
            input()
            del self._params
        else:
            del self._params
    
    @property
    def default_method(self) -> str:
        return self._default_method
    @default_method.setter
    def default_method(self, newmethod: str):
        self._default_method = newmethod
    @default_method.deleter
    def default_method(self, verbose=False):
        if verbose:
            print("Deleting default_method. Press enter to confirm")
            input()
            del self._default_method
        else:
            del self._default_method

    @property
    def endpoint(self) -> str:
        return self._endpoint
    @endpoint.setter
    def endpoint(self, newendpoint: str):
        self._endpoint = newendpoint
    @endpoint.deleter
    def endpoint(self, verbose=False):
        if verbose:
            print("Deleting endpoint. Press enter to confirm")
            input()
            del self._endpoint
        else:
            del self._endpoint
   
    @property
    def timeout(self) -> int:
        return self._timeout
    @timeout.setter
    def timeout(self, newtimeout: int):
        self._timeout = newtimeout
    @timeout.deleter
    def timeout(self, verbose=False):
        if verbose:
            print("Deleting timeout. Press enter to confirm")
            input()
            del self._timeout
        else:
            del self._timeout
    
    @property
    def adapter(self) -> requests.adapters.HTTPAdapter:
        return self._adapter
    @adapter.setter
    def adapter(self, newadapter: requests.adapters.HTTPAdapter):
        self._adapter = newadapter
    @adapter.deleter
    def adapter(self, verbose=False):
        if verbose:
            print("Deleting adapter. Press enter to confirm")
            input()
            del self._adapter
        else:
            del self._adapter

    def update_headers(self, headers: Dict[str, str], inplace: bool=False) -> Union[AbstractScraper, Dict[str, str]]:
        """
        Update the headers for the request. Inplace by default
        :param headers: The headers to update. This will only update the 
            values where keys already exists
        :param inplace: Inplace update if true, else returns the new header
        """
        if inplace:
            self.headers.update(headers)
        else:
            oldheaders = self.headers.copy()
            oldheaders.update(headers)
            return oldheaders

    def close_sql_conn(self):
        """close SQL connection"""
        self.sql_conn.close()

    def frame_to_sql(self, df: pd.DataFrame, table_name: str, 
        **kwargs) -> None:
        """
        :param df: The dataframe to save to the database
        """
        df.to_sql(name=table_name, con=self.sql_conn, 
            if_exists=kwargs.get('if_exists', 'append'), 
            index=kwargs.get('index', True))
    
    @staticmethod
    def pdf_to_text(pdf_file: bytes, keep_chinese: bool=False) -> str:
        """pass in the byte stream of the pdf file by calling open()
        pdfs are already stored as byte streams in the dataframes and sqlite
        """
        pdf_file = io.BytesIO(pdf_file)
        pdf_reader = PyPDF2.PdfFileReader(pdf_file)
        text = ""
        for page in range(pdf_reader.numPages):
            text += pdf_reader.getPage(page).extract_text()
        if not keep_chinese:
            text = re.sub("(\\s\d\d\w)", "", text)
        return text
    
    @staticmethod
    def get_pdf(url: str, session: requests.Session=None, **kwargs) -> bytes:
        """
        :param url: The url to the pdf
        :param session: The session to use for the request. If no session is 
            passed, a new session will be created
        :return: The pdf as a byte stream
        """
        assert isinstance(url, str), "url passed is not a string"
        assert url.split('.')[-1].lower() == 'pdf', "url doesn't look like a pdf!"
        default_adapter = requests.adapters.HTTPAdapter(max_retries=3)
        if not session:
            session = requests.Session()
            schema = kwargs.get('schema', 'http')
            adapter = kwargs.get('adapter', default_adapter)
            session.mount(schema, adapter)
        return session.get(url, **kwargs).content
    
    @staticmethod
    def html_entities_to_unicode(text: str) -> str:
        """
        :param text: The text to convert
        :return: The text converted to unicode
        """
        return html.unescape(text)
    
    @staticmethod
    def html_to_text(html: str) -> str:
        """
        :param html: The html to convert
        :return: The text converted from the html
        """
        pass
    
    @abstractmethod
    def get_filing_list(self, **kwargs) -> pd.DataFrame:
        """
        :param kwargs: The arguments to pass to the request
        :return: The filing list as a dataframe
        """
        pass

    @abstractmethod
    def get_filing_content(self, **kwargs) -> pd.DataFrame:
        """
        :return: The filing content as a string
        """
        pass