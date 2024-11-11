import time
import traceback

import pandas as pd
import scrapy,os,json,datetime
from bs4 import BeautifulSoup

from scrapy.crawler import CrawlerProcess


class AcsSpider(scrapy.Spider):

    name = "martin"
    SOURCE_SITE='https://www.martinfurniture.com'
    COOKIES_ENABLED=False
    ROBOTSTXT_OBEY = True
    DATA=[]
    custom_settings = {
        "DOWNLOAD_DELAY": "0.5",
        "CONCURRENT_REQUESTS":"1",
        "ROBOTSTXT_OBEY":True,
        "COOKIES_ENABLED":False
        }
    FILENAME=os.path.join(os.path.abspath('output'),'output_{}.json'.format(str(datetime.datetime.utcnow().timestamp()).split('.')[0].strip()))
    def start_requests(self):
        url='https://www.martinfurniture.com'
        yield scrapy.Request(
            url=url,
            callback=self.get_categ_links,
            dont_filter=True
        )

    def get_categ_links(self, response):
        soup=BeautifulSoup(response.body,'html.parser')
        main_categs=soup.find('ul',attrs={'id':'menu-main-navigation-1'}).find_all('li',recursive=False)
        for main_categ in main_categs:
            sub_categs=main_categ.find('ul').find_all('li',recursive=False)
            for sub_categ in sub_categs:
                link=sub_categ.find('a')['href']
                if not self.SOURCE_SITE in link:
                    link=self.SOURCE_SITE+link
                yield scrapy.Request(
                    url=link,
                    callback=self.get_sub_sub_link,
                    meta={'main_categ':main_categ,'sub_categ':sub_categ}
                )
    def get_sub_sub_link(self,response):
        soup=BeautifulSoup(response.body,'html.parser')
        if not 'Sort By:' in soup.find('body').text:
            try:
                sub_sub_categs=soup.find('ul',attrs={"data-product_layout":"product-wq_onimage"}).find_all('li',recursive=False)
            except:
                div_sub_categs=soup.find_all('div',class_='vc_row wpb_row row top-row')
                sub_sub_categs=[]
                for div in div_sub_categs:
                    sub_sub_categs.append(div.find('div',recursive=False))
            if len(sub_sub_categs)!=0:

                for sub_sub_categ in sub_sub_categs:
                    try:
                        row={}
                        row['Main Category']=response.meta['main_categ'].text.splitlines()[0].strip()
                        row['Collection']=response.meta['sub_categ'].text.strip()
                        row['Products Starting Link']=sub_sub_categ.find('a')['href']
                        if not self.SOURCE_SITE in row['Products Starting Link']:
                            row['Products Starting Link']=self.SOURCE_SITE+row['Products Starting Link']
                        # print(row)
                    except:
                        pass
        else:
            row={}
            row['Main Category']=response.meta['main_categ'].text.splitlines()[0].strip()
            row['Collection']=response.meta['sub_categ'].text.strip()
            row['Products Starting Link']=response.meta['sub_categ'].find('a')['href']
            if not self.SOURCE_SITE in row['Products Starting Link']:
                row['Products Starting Link']=self.SOURCE_SITE+row['Products Starting Link']
            # print(row)
        try:
            for page in range(5):
                yield scrapy.Request(
                    url=row['Products Starting Link']+'page/{}/?load_posts_only=1'.format(page),
                    callback=self.get_products_links,
                    meta={'row':row}
                )
        except:
            pass
    def get_products_links(self, response):
        if str(response.status).strip()!='404':
            soup=BeautifulSoup(response.body,'html.parser')
            try:
                products=soup.find('ul',attrs={"data-product_layout":"product-wq_onimage"}).find_all('li',recursive=False)
                row=response.meta['row']
                for product in products:
                    lnk=product.find('a',class_='product-loop-title')['href']
                    if not self.SOURCE_SITE in lnk:
                        lnk=self.SOURCE_SITE+lnk
                    yield scrapy.Request(
                        url=lnk,
                        callback=self.get_products_details,
                        meta={'row':row}
                    )
            except:
                pass

    def get_products_details(self,response):
        soup=BeautifulSoup(response.body,'html.parser')
        row=response.meta['row']
        row['Product Link']=response.request.url
        row['Title']=soup.find('h2').text.strip()
        row['SKU']=soup.find('span',class_='sku_wrapper').text.split(':')[1].strip()
        try:
            for a in soup.find_all('a'):
                href = a.get("href")
                if 'https://www.martinfurniture.com/available-finishes/' in href:
                    row['AVAILABLE FINISHES']= a.text.strip()
                    break
        except:
            pass
        try:
            row['Description']=soup.find('div',class_='desc-bullets').text.strip()
        except:
            pass
        try:
            row['Collection Features']=soup.find('div',class_='description woocommerce-product-details__short-description').text.strip()
        except:
            pass
        try:
            i=0
            row['Images']=[]
            for div in soup.find_all('div',class_='iconic-woothumbs-thumbnails__image-wrapper'):
                i+=1
                rw={}
                rw['Image '+str(i)]=div.find('img')['src']
                if not self.SOURCE_SITE in rw['Image '+str(i)]:
                    rw['Image '+str(i)]=self.SOURCE_SITE+rw['Image '+str(i)]
                row['Images'].append(rw)
        except:
            pass
        try:
            info_trs=soup.find('div',attrs={'id':'tab-additional_information'}).find_all('tr')
            for info in info_trs:
                row[info.find('th').text.strip()] = info.find('td').text.strip()
        except:
            pass
        self.DATA.append(row)
        df=pd.DataFrame(self.DATA)
        try:
            df.sort_values(by=['Main Category','Collection','Collection 2'],inplace=True)
        except:
            df.sort_values(by=['Main Category','Collection'],inplace=True)
        df=df.rename(columns={'Available Finishes':'Colour'})
        rows_=df.to_dict('records')

        rows_updates=[]
        for row_ in rows_:
            r={}
            for key,value in row_.items():
                if str(value).lower().strip()=='nan' or str(value).lower().strip()=='' or str(value).lower().strip()=='nat':
                    pass
                else:
                    if not 'Link' in key:
                        try:
                            if '"' in value:
                                value=value.replace('"','\"')
                        except:
                            pass
                    r[key]=value
            rows_updates.append(r)

        self.DATA=rows_updates
        with open(self.FILENAME, 'w',encoding='utf8') as fout:
            json.dump(self.DATA , fout,indent=4,ensure_ascii=False)


def run_spiders():
    process = CrawlerProcess()
    process.crawl(AcsSpider)
    process.start()

run_spiders()
