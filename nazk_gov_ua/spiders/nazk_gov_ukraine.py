from scrapy.cmdline import execute
from lxml.html import fromstring
from unidecode import unidecode
from html import unescape
from unicodedata import normalize
from datetime import datetime
from typing import Iterable
from scrapy import Request
from urllib import parse
import pandas as pd
import random
import string
import scrapy
import json
import time
import evpn
import os
import re


def df_cleaner(data_frame: pd.DataFrame) -> pd.DataFrame:
    print('Cleaning DataFrame...')
    data_frame = data_frame.astype(str)  # Convert all data to string
    data_frame.drop_duplicates(inplace=True)  # Remove duplicate data from DataFrame

    # Apply the function to all columns for Cleaning
    for column in data_frame.columns:
        data_frame[column] = data_frame[column].apply(set_na)  # Setting "N/A" where data is empty string
        data_frame[column] = data_frame[column].apply(unidecode)  # Remove diacritics characters
        # data_frame[column] = data_frame[column].apply(remove_extra_spaces)  # Remove extra spaces and newline characters from each column
        if 'title' in column:
            data_frame[column] = data_frame[column].str.replace('–', '')  # Remove specific punctuation 'dash' from name string
            # data_frame[column] = data_frame[column].str.translate(str.maketrans('', '', string.punctuation))  # Removing Punctuation from name text
        data_frame[column] = data_frame[column].apply(remove_extra_spaces)  # Remove extra spaces and newline characters from each column

    data_frame.replace(to_replace='nan', value=pd.NA, inplace=True)  # After cleaning, replace 'nan' strings back with actual NaN values
    data_frame.fillna(value='N/A', inplace=True)  # Replace NaN values with "N/A"
    print('DataFrame Cleaned...!')
    return data_frame


def set_na(text: str) -> str:
    # Remove extra spaces (assuming remove_extra_spaces is a custom function)
    text = remove_extra_spaces(text=text)
    pattern = r'^([^\w\s]+)$'  # Define a regex pattern to match all the conditions in a single expression
    text = re.sub(pattern=pattern, repl='N/A', string=text)  # Replace matches with "N/A" using re.sub
    return text


# Function to remove Extra Spaces from Text
def remove_extra_spaces(text: str) -> str:
    return re.sub(pattern=r'\s+', repl=' ', string=text).strip()  # Regular expression to replace multiple spaces and newlines with a single space


def clean_text(raw_string):
    """Cleans a raw string by removing special characters, normalizing Unicode,
    unescaping HTML entities, and collapsing whitespace."""
    unescaped_string = unescape(raw_string)  # Step 1: HTML unescape
    normalized_string = normalize('NFKC', unescaped_string)  # Step 2: normalize Unicode
    # Matches \xa0, \r, \n, and other unwanted characters
    cleaned_string = re.sub(pattern=r'[\xa0\r\n]+', repl=' ', string=normalized_string)  # Step 3: Remove unwanted characters using regex
    cleaned_string = re.sub(pattern=r'\s+', repl=' ', string=cleaned_string).strip()  # Step 4: Collapse multiple spaces into one and trim
    return cleaned_string


def get_detail_page_url(news_div) -> str:
    detail_page_url = ' '.join(news_div.xpath('.//a[@class="cover-div"]/@href')).strip()
    return detail_page_url if detail_page_url not in ['', None, []] else 'N/A'


def get_news_title(news_div) -> str:
    news_title = clean_text(' '.join(news_div.xpath('.//h1//text()')))
    return news_title if news_title != '' else 'N/A'


def get_news_date(news_div) -> str:
    news_date = clean_text(' '.join(news_div.xpath('.//div[@class="news-date"]//text()')))
    if news_date != '':
        date_obj = datetime.strptime(news_date, "%d.%m.%Y")  # Parse the date using datetime.strptime
        formatted_date = date_obj.strftime(format="%Y-%m-%d").strip()  # Convert to desired format
        return formatted_date if formatted_date != '' else 'N/A'
    else:
        return 'N/A'


def get_description(news_text_div):
    description = clean_text(' '.join(news_text_div.xpath('./p//text() | .//ul//li//text() | .//ol//li//text()')))
    return description if description != '' else 'N/A'


def get_blockquote(news_text_div):
    blockquote = clean_text(' '.join(news_text_div.xpath('//div[@class="text-content"]//blockquote//text()')))
    return blockquote if blockquote != '' else 'N/A'


class NazkGovUkraineSpider(scrapy.Spider):
    name = "nazk_gov_ukraine"

    def __init__(self):
        self.start = time.time()
        super().__init__()
        print('Connecting to VPN (UKRAINE)')
        self.api = evpn.ExpressVpnApi()  # Connecting to VPN (UKRAINE)
        self.api.connect(country_id='87')  # UKRAINE country code for vpn
        time.sleep(5)  # keep some time delay before starting scraping because connecting
        print('VPN Connected!' if self.api.is_connected else 'VPN Not Connected!')

        # self.delivery_date = datetime.now().strftime('%Y%m%d')
        self.final_data_list = list()  # List of data to make DataFrame then Excel

        # Path to store the Excel file can be customized by the user
        self.excel_path = r"../Excel_Files"  # Client can customize their Excel file path here (default: govtsites > govtsites > Excel_Files)
        os.makedirs(self.excel_path, exist_ok=True)  # Create Folder if not exists
        self.filename = fr"{self.excel_path}/{self.name}.xlsx"  # Filename with Scrape Date

        self.cookies = {
            '_ga': 'GA1.1.955734906.1732855356',
            'PHPSESSID': '5ubqbnmhmdnrnob92pnfeb8nm9',
            '_csrf': 'e2daa3c67df74796488f53e69cebcd7ccd5ffd87276766e872e3557c15c8dc18a%3A2%3A%7Bi%3A0%3Bs%3A5%3A%22_csrf%22%3Bi%3A1%3Bs%3A32%3A%22MqqGp76uJC_mG4HC8qA-v0FuTnlrAiml%22%3B%7D',
            '_ga_M882VHG5S0': 'GS1.1.1733221813.9.1.1733224040.60.0.0',
        }
        self.api_headers = {
            'accept': 'application/json, text/javascript, */*; q=0.01',
            'accept-language': 'en-US,en;q=0.9',
            'priority': 'u=1, i',
            'referer': 'https://nazk.gov.ua/',
            'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'x-requested-with': 'XMLHttpRequest',
        }
        self.details_headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-US,en;q=0.9',
            'priority': 'u=0, i',
            'referer': 'https://nazk.gov.ua/',
            'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        }

        self.browsers = ["chrome110", "edge99", "safari15_5"]

    def start_requests(self) -> Iterable[Request]:
        params = {'id': '24', 'offset': '0', 'lang': 'en', }
        url = 'https://nazk.gov.ua/category/next/?' + parse.urlencode(params)
        # Sending request on an api which gives news detail page's url in html text in response json.
        yield scrapy.Request(url=url, cookies=self.cookies, headers=self.api_headers, method='GET', callback=self.parse,
                             meta={'impersonate': random.choice(self.browsers)}, dont_filter=True)

    def parse(self, response, **kwargs):
        if response.status == 200:
            json_dict: dict = json.loads(response.text)
            next_page_url = json_dict.get('url', 'N/A')
            html_text = json_dict.get('html', 'N/A')
            if html_text != '':
                parsed_tree = fromstring(html=html_text)
                news_list = parsed_tree.xpath('//div[@class="one-news"]')
                for news_div in news_list:
                    detail_page_url = get_detail_page_url(news_div)
                    data_dict = dict()
                    data_dict['url'] = 'https://nazk.gov.ua/en/news/'
                    data_dict['detail_page_url'] = detail_page_url
                    yield scrapy.Request(url=detail_page_url, cookies=self.cookies, headers=self.details_headers, method='GET', callback=self.detail_parse,
                                         meta={'impersonate': random.choice(self.browsers)}, dont_filter=True, cb_kwargs={'data_dict': data_dict})
                # Find the URL of the next page & Handle Pagination
                if next_page_url:
                    parsed_url = parse.urlparse(next_page_url)  # Parse the URL
                    query_params = parse.parse_qs(parsed_url.query)  # Extract query parameters
                    page = query_params.get('page', [None])[0]  # # Get the value of the 'page' parameter & Use [None] as default if 'page' doesn't exist

                    print('Sending request on next page', page)
                    yield scrapy.Request(url=next_page_url, cookies=self.cookies, headers=self.api_headers, method='GET', callback=self.parse,
                                         meta={'impersonate': random.choice(self.browsers)}, dont_filter=True)
            else:
                print('No More Pagination found.')

    def detail_parse(self, response, **kwargs):
        data_dict = kwargs.get('data_dict')
        parsed_tree = fromstring(html=response.text)
        news_div = parsed_tree.xpath('//div[contains(@class, "news-content")]')[0]
        news_text_div = news_div.xpath('.//div[@class="text-content"]//div')[0]
        data_dict['news_title'] = get_news_title(news_div)
        data_dict['news_date'] = get_news_date(news_div)
        data_dict['description'] = get_description(news_text_div)
        data_dict['blockquote'] = get_blockquote(news_text_div)
        print(data_dict)
        self.final_data_list.append(data_dict)

    def close(self, reason):
        print('closing spider...')
        print("Converting List of Dictionaries into DataFrame, then into Excel file...")
        try:
            print("Creating Native sheet...")
            data_df = pd.DataFrame(self.final_data_list)
            data_df = df_cleaner(data_frame=data_df)  # Apply the function to all columns for Cleaning
            data_df.insert(loc=0, column='id', value=range(1, len(data_df) + 1))  # Add 'id' column at position 1
            # data_df.set_index(keys='id', inplace=True)  # Set 'id' as index for the Excel output
            with pd.ExcelWriter(path=self.filename, engine='xlsxwriter', engine_kwargs={"options": {'strings_to_urls': False}}) as writer:
                data_df.to_excel(excel_writer=writer, index=False)

            print("Native Excel file Successfully created.")
        except Exception as e:
            print('Error while Generating Native Excel file:', e)
        if self.api.is_connected:  # Disconnecting VPN if it's still connected
            self.api.disconnect()

        end = time.time()
        print(f'Scraping done in {end - self.start} seconds.')


if __name__ == '__main__':
    execute(f'scrapy crawl {NazkGovUkraineSpider.name}'.split())
