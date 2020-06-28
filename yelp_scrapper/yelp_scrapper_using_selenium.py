# encoding=utf8
__author__ = "Sohaib Asif"

from selenium import webdriver
from lxml.html import fromstring
import requests
import json
from time import sleep
import pandas as pd
import logging
import unicodedata
import sys
import os

root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(root_dir)

class YelpSeleniumScrapper:

    url = "https://www.yelp.com/search?find_desc={0}&find_loc={1}&start="
    headers = {
        'User-Agent': "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.117 Safari/537.36"
    }

    scrapped_data = []
    browser = webdriver.Firefox()

    def __init__(self, location, keywords):

        self.url = self.url.format(keywords, location)

        logging.basicConfig(level=logging.INFO, format='%(asctime)s;%(levelname)s;%(funcName)s;%(lineno)d;%(message)s')
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)

    def get_page_info(self, url):

        try:

            self.browser.get(url)
            self.logger.info("scrapping business URLs")

            if len(self.browser.find_elements_by_css_selector(".lemon--div__373c0__1mboc.border-color--default__373c0__3-ifU "
                                               ".lemon--ul__373c0__1_cxs.undefined.list__373c0__2G8oH ")) > 0:
                companies_list = self.browser.find_elements_by_css_selector(".lemon--div__373c0__1mboc.border-color--default__373c0__3-ifU "
                                                   ".lemon--ul__373c0__1_cxs.undefined.list__373c0__2G8oH ")[0]

                for ele in companies_list.find_elements_by_css_selector("li h4"):
                    if len(ele.find_elements_by_css_selector("a")) > 0:
                        url = ele.find_elements_by_css_selector("a")[0].get_attribute('href')
                        self.get_company_detail(url)

        except Exception as e:
            self.logger.error("Exception: {}".format(e))

    def get_company_detail(self, url):
        data = {
            'name': 'not found',
            'website': 'not found',
            'phone_number': 'not found',
            'address': 'not found',
            'description': 'not found',
            'owner': 'not found'
        }

        self.logger.info("scrapping business details")

        try:

            browser = webdriver.Firefox()
            browser.get(url)

            if len(browser.find_elements_by_css_selector("h1")) > 0:
                data['name'] = browser.find_elements_by_css_selector("h1")[0].text

            if len(browser.find_elements_by_css_selector(".lemon--section__373c0__fNwDM.u-space-b3.border-color--default__373c0__2oFDT .lemon--a__373c0__IEZFH.link__373c0__29943.link-color--blue-dark__"
                                        "373c0__1mhJo.link-size--default__373c0__1skgq")) > 0:
                data['website'] = browser.find_elements_by_css_selector(".lemon--section__373c0__fNwDM.u-space-b3.border-color--default__373c0__2oFDT .lemon--a__373c0__IEZFH.link__373c0__29943.link-color--blue-dark__"
                                            "373c0__1mhJo.link-size--default__373c0__1skgq")[0].text

            if len(browser.find_elements_by_css_selector(".lemon--section__373c0__fNwDM.u-space-b3.border-color--default__373c0__2oFDT .lemon--p__373c0__3Qnnj.text__373c0__2pB8f"
                                             ".text-color--normal__373c0__K_MKN.text-align--left__373c0__2pnx_")) > 2:
                data['phone_number'] = browser.find_elements_by_css_selector(".lemon--section__373c0__fNwDM.u-space-b3.border-color--default__373c0__2oFDT .lemon--p__373c0__3Qnnj.text__373c0__2pB8f"
                                                 ".text-color--normal__373c0__K_MKN.text-align--left__373c0__2pnx_")[2].text

            if len(browser.find_elements_by_css_selector(".lemon--div__373c0__1mboc.arrange-unit__373c0__1piwO.border-color--default__373c0__2oFDT address span")) > 0:
                data['address'] = browser.find_elements_by_css_selector(".lemon--div__373c0__1mboc.arrange-unit__373c0__1piwO.border-color--default__373c0__2oFDT address span")[0].text

            if len(browser.find_elements_by_css_selector("meta[property=\"og:description\"]")) > 0:
                data['description'] = browser.find_elements_by_css_selector("meta[property=\"og:description\"]")[0].get_attribute("content")

            if len(browser.find_elements_by_css_selector(".lemon--div__373c0__1mboc.arrange__373c0__UHqhV.gutter-6__373c0__zqA5A.border-color--default__373c0__2oFDT .lemon--p__373c0__3Qnnj.text__373c0__2pB8f.text-color--normal__373c0__K_MKN.text-align--left__373c0__2pnx_.text-weight--bold__373c0__3HYJa")) > 0:
                data['owner'] = browser.find_elements_by_css_selector(".lemon--div__373c0__1mboc.arrange__373c0__UHqhV.gutter-6__373c0__zqA5A.border-color--default__373c0__2oFDT .lemon--p__373c0__3Qnnj.text__373c0__2pB8f.text-color--normal__373c0__K_MKN.text-align--left__373c0__2pnx_.text-weight--bold__373c0__3HYJa")[0].text

            if data['name'] != 'not found':
                self.scrapped_data.append(data)

            browser.close()

        except Exception as e:
            self.logger.error("Exception: {}".format(e))

    def run(self, pages, output_file, records_per_page):

        start = 0
        for i in range(pages):

            url = self.url + str(start)
            logging.info("scrapping URL: {}".format(url))
            self.get_page_info(url)
            self.logger.info("Scrapped page {}".format(i))
            start += records_per_page
            if len(self.scrapped_data) > 0:
                self.save_data(output_file+"temp.csv")
            sleep(20)
        if len(self.scrapped_data) > 0:
            self.save_data(output_file)

    def save_data(self, output_file):
        # with open('json_output.json', 'w') as json_file:
        #     json.dump(self.scraped_biz, json_file, indent = 4)

        self.logger.info("Writing output")

        name = []
        owner = []
        phone = []
        address = []
        website = []
        description = []

        output_data = pd.DataFrame()

        try:
            for record in self.scrapped_data:
                name = name + [record["name"]]
                owner = owner + [record["owner"]]
                phone = phone + [record["phone_number"]]
                address = address + [record["address"]]
                website = website + [record["website"]]
                description = description + [record["description"]]
        except Exception as e:
            self.logger.info("Exception: {}".format(e))

        output_data['name'] = name
        output_data['owner'] = owner
        output_data['phone'] = phone
        output_data['address'] = address
        output_data['website'] = website
        output_data['description'] = description

        output_data.to_csv(output_file, encoding='utf-8')
        self.logger.info("Output file is generated.")


if __name__ == '__main__':

    location = sys.argv[1]
    keyword = sys.argv[2]
    output_file = sys.argv[3]
    pages = sys.argv[4]
    records_per_page = sys.argv[5]
    pages = int(pages)
    records_per_page = int(records_per_page)

    scrapper = YelpSeleniumScrapper(location, keyword)
    scrapper.run(pages, output_file, records_per_page)
