import pandas as pd

import scrapy
from scrapy.exceptions import CloseSpider
from scrapy.http import Request


class HomeDepot(scrapy.Spider):
    # name of the spider
    name = 'homedepot'

    # allowed domain
    allowed_domains = ['homedepot.com']

    # starting url for scraping
    start_urls = ['https://www.homedepot.com/c/site_map']

    BASE = 'https://www.homedepot.com'

    def parse(self, response):
        # Remove XML namespaces
        response.selector.remove_namespaces()
        # the url pool of potential products such dishwasher and Refrigerators
        map = response.xpath('//li[contains(@class, "list__item list__item--padding-none")]/a/@href').extract()
        # if there are more than one threads of products on the web page, ask spider to search for next page.
        next_page = response.xpath(
            '//ul[@class = "hd-pagination__wrapper"]/li[@class= "hd-pagination__item"]/a/@href').extract()
        # add urls of next page to url pool
        if next_page:
            map = map + next_page

        # # the xpath to search url of Mattresses
        # mat_map = response.xpath('//div[contains(@class,"grid input-filter__wrapper")]//a/@href').extract()
        # # add the urls to url pool
        # if mat_map:
        #     map = map + mat_map

        # Since too many brands under Mattress sub department, I just use end url links to search for prices of Sealy Mattress
        mat_map2 = [
            'https://www.homedepot.com/b/Home-Decor-Bedding-Bath-Mattress-Protectors-Pillow-Protectors/Sealy/N-5yc1vZc306Zf98',
            'https://www.homedepot.com/b/Home-Decor-Bedding-Bath-Mattress-Toppers-Mattress-Pads/Sealy/N-5yc1vZc1kuZf98']
        # add the url links to url pool
        map = map + mat_map2

        # filter the url with the keywords in the requirements
        map = [i for i in map if
               'Dishwashers' in i or 'Refrigerators' in i or 'Sealy' in i or 'Mattress' in i or 'LG' in i or 'Samsung' in i or 'Whirlpool' in i
               or 'GE' in i]

        # add the base url in the url links in order to call in next event
        for i in range(len(map)):
            if 'https://www.homedepot.com/' not in map[i]:
                map[i] = self.BASE + map[i]

        # department xpath for Appliances
        department = response.xpath('//ul[@id = "headerCrumb"]/li[2]//a/text()').extract()
        # sub department xpath for Dishwasher and Refrigerators
        sub_dep = response.xpath('//ul[@id = "headerCrumb"]/li[3]//a/text()').extract()
        # brands xpath for Dishwasher and Refrigerators
        brand = response.xpath(
            '//div[@id = "products"]//div[contains(@data-podaction, "product name")]//span[contains(@class, "pod-plp__brand-name")]/text()').extract()
        # detail xpath for Dishwasher and Refrigerators under each brand
        detail = response.xpath(
            '//div[@id = "products"]//div[contains(@data-podaction, "product name")]/a/text()').extract()
        # prices xpath for Dishwasher and Refrigerators under each brand
        price = response.xpath(
            '//div[@id = "products"]//div[@class="overflow__inner"]/div[contains(@class, "price__numbers")]/text()').extract()

        # data cleaning in detail and price lists
        detail = [x.strip(' \n\t') for x in detail]
        detail = [x for x in detail if x]
        price = [x.strip(' \n\t') for x in price]
        price = [x for x in price if x]

        # department xpath for mattress
        mat_dep = response.xpath('//ul[@class = "breadcrumb__header"]/li[2]//a/text()').extract()
        # sub xpath department for mattress
        mat_sub = response.xpath('//ul[@class = "breadcrumb__header"]/li[4]//a/text()').extract()
        # brand xpath for mattress
        mat_brand = response.xpath(
            '//div[@class = "product-pod product-pod--hover-float"]//span[@class = "product-pod__title__brand--bold"]/text()').extract()
        # mattress deatail xpath
        mat_detail = response.xpath(
            '//div[@class = "product-pod product-pod--hover-float"]//span[@class = "product-pod__title__product--text"]/text()').extract()
        # mattress prices xpath
        mat_price = response.xpath(
            '//div[@class = "product-pod product-pod--hover-float"]//div[@class = "product-pod__pricing"]//span[2]/text()').extract()
        # data cleaning for mattress brand list
        mat_brand = [i for i in mat_brand if i != ' ']

        # products container
        product = dict()
        # if the spider searches for dishwasher, Refrigerators and mattress at the same time, catch all products' info
        if (mat_dep and mat_sub and mat_brand and mat_detail and mat_price) and (
                                department and sub_dep and brand and detail and price):

            # make sure department, sub department lists have the same length as brand, detail and prices
            mat_dep = mat_dep * len(mat_brand)
            mat_sub = mat_sub * len(mat_brand)
            department = department * len(brand)
            sub_dep = sub_dep * len(brand)

            # put all products info into products container
            product['Department'] = department + mat_dep
            product['Sub_dep'] = sub_dep + mat_sub
            product['Brand'] = brand + mat_brand
            product['Detail'] = detail + mat_detail
            product['Price'] = price + mat_price

            # convert products container to dataframe
            df = pd.DataFrame(product)
            df = df.drop_duplicates()

            # filter out useless products info which is not in requirements
            df = df[((df.Sub_dep == 'Dishwashers') & (df.Brand == 'LG Electronics')) | (
                (df.Sub_dep == 'Dishwashers') & (df.Brand == 'Samsung'))
                    | ((df.Sub_dep == 'Refrigerators') & (df.Brand == 'GE')) | (
                        (df.Sub_dep == 'Refrigerators') & (df.Brand == 'Whirlpool'))
                    | ((df.Sub_dep == 'Mattress Protectors & Pillow Protectors') & (df.Brand == 'Sealy'))]
            # transform dataframe to csv file
            with open('tmp/dishwasher.csv', 'a') as f:
                df.to_csv(f, header=False)

        # if the spider searches for  mattress only, catch all products' info
        elif mat_dep and mat_sub and mat_brand and mat_detail and mat_price and not (
                                department or sub_dep or brand or detail or price):

            mat_dep = mat_dep * len(mat_brand)
            mat_sub = mat_sub * len(mat_brand)
            product['Department'] = mat_dep
            product['Sub_dep'] = mat_sub
            product['Brand'] = mat_brand
            product['Detail'] = mat_detail
            product['Price'] = mat_price

            # filter out useless info
            df = pd.DataFrame(product)
            df = df.drop_duplicates()

            df = df[df.Brand == 'Sealy']

            with open('tmp/dishwasher.csv', 'a') as f:
                df.to_csv(f, header=False)

        # if the spider searches for dishwasher and Refrigerators only, catch all products' info
        elif department and sub_dep and brand and detail and price and not (
                            mat_dep or mat_sub or mat_brand or mat_detail):
            department = department * len(brand)
            sub_dep = sub_dep * len(brand)

            product['Department'] = department
            product['Sub_dep'] = sub_dep
            product['Brand'] = brand
            product['Detail'] = detail
            product['Price'] = price

            # filter out useless info
            df = pd.DataFrame(product)
            df = df.drop_duplicates()

            df = df[((df.Sub_dep == 'Dishwashers') & (df.Brand == 'LG Electronics')) | (
                (df.Sub_dep == 'Dishwashers') & (df.Brand == 'Samsung'))
                    | ((df.Sub_dep == 'Refrigerators') & (df.Brand == 'GE')) | (
                        (df.Sub_dep == 'Refrigerators') & (df.Brand == 'Whirlpool'))
                    ]

            with open('tmp/dishwasher.csv', 'a') as f:
                df.to_csv(f, header=False)
        # make sprider search for all potential url links in url pool and start the whole event again until crawling is finished
        if map:
            for i in map:
                yield Request(url=i, callback=self.parse)


if __name__ == "__main__":
    # driver class to make crawling start
    from scrapy.crawler import CrawlerProcess

    process = CrawlerProcess({
        'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
    })
    process.crawl(HomeDepot)
    process.start()  # the script will block here until the crawling is finished
