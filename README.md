# Utilities package for getting filings from exchanges and getting text

version 1.0

## Background

This was created as part of an information retrieval (IR) project, which aims at structurally extracting information from companies\' financial finings. Use this package to extract information from company filings, store them in sqlite, and generate the training dataset from the created databases.

The following exchanges are currently included:

- All US listed stocks (through EDGAR)
- HKEX (through hkexnews.com)
- SZ and SH exchange in China (through CNinfo)
- TWSE

We aim to add the following:

- UK (through [gov.hk](find-and-update.company-information.service.gov.u)))
- Germany
- Japan

## Modules

- _Abstract_scraper: Abscract class. All other scrapers should inherit from this.
- hkex.hkexnews.HKEXNews: extract filings on HKEX website
- cninfo: extract information from CNInfo

## Setup

You should create a new venv with conda

>  conda create --name <env\> --file requirements.txt

Then run

> conda --yes --file requirements.txt

Or run 

>  python -m pip install -r requirements.txt


## Using as a script

All the packages can be used as scripts. Usually they take the following CLI arguments:

>  -S | --stock_list: the stocklist to be processed. Seperated by space. If not specified, you will need to pass the stock list through the stocks_path argument, via a file.
>
> -SP | --stocks_path: if your list of stocks is long, you can store them in a *.txt file and this will be wrapped into a list.
>
> -ct | --convert_to_text: True if specified, will store the filings as text strings, instead of bytestream;
>
> -s | --start_date: start date of the query;
>
> -e | --end_date: end date of the query;
>
> -d | --doctype: type of the document to select;
>
> -C | --config: path of the config file to be used
>
> -D | --db_path: path to the database
>
> -V | --verbose: verbose or not
>
> --display_doctype_list: if you are not sure what doctypes are available, specify this, then a list of the valid doctypes will be printed.

This will get the related information and put them into the database
