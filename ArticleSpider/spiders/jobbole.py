# -*- coding: utf-8 -*-
from urllib import parse
import re
import json

import scrapy
from scrapy import Request
from ArticleSpider.items import ArticleItemLoader
from pydispatch import dispatcher
from scrapy import signals
import requests

from ArticleSpider.items import JobBoleArticleItem
from ArticleSpider.utils.common import get_md5
from selenium import webdriver


class JobboleSpider(scrapy.Spider):
    name = 'jobbole'
    allowed_domains = ['news.cnblogs.com']
    start_urls = ['http://news.cnblogs.com/']

    # def __init__(self):
    #     self.browser = webdriver.Chrome(executable_path="D:/tools/chromedriver.exe")
    #     super(JobboleSpider, self).__init__()
    #     dispatcher.connect(self.spider_closed, signals.spider_closed)
    #
    # def spider_closed(self, spider):
    #     # 当爬虫退出的时候推出chrome
    #     self.browser.quit()

    # 收集伯乐在线所有404url及页面数量
    handle_httpstatus_list = [404]

    def __init__(self):
        self.fail_urls = []
        dispatcher.connect(self.handle_spider_closed, signals.spider_closed)

    def handle_spider_closed(self, spider, reason):
        self.crawler.stats.set_value("failed_urls", ",".join(self.fail_urls))

    def parse(self, response):
        """
        1.获取新闻列表页的详情url并交给scrapy进行下载后调用相应的解析方法
        2.获取下一页的url并交给scrapy进行下载，下载完成后交给parse继续跟进
        :param response:
        :return:
        """
        if response.status == 404:
            self.fail_urls.append(response.url)
            self.crawler.stats.inc_value("failed_url")
        # url = response.xpath("//*[@id='entry_654144']/div[2]/h2/a/@href").extract_first()
        # url = response.xpath("//div[@id='news_list']//h2[@class='news_entry']/a/@href").extract_first("")
        # url = response.css("#news_list h2 a::attr(href)").extract_first("")
        post_nodes = response.css("#news_list .news_block")  # [:1]
        for post_node in post_nodes:
            image_url = post_node.css(".entry_summary a img::attr(src)").extract_first("")
            match_re = re.match("^http", image_url)
            if not match_re:
                image_url = 'https:' + image_url
            post_url = post_node.css("h2 a::attr(href)").extract_first("")
            yield Request(url=parse.urljoin(response.url, post_url), meta={"front_image_url": image_url},
                          callback=self.parse_detail)  # 不能写parse_detail() 不然就变成返回值了

        # 提取下一页并交给scrapy进行下载
        next_url = response.css(".pager a:last-child::text").extract_first("")
        # next_url = response.xpath("a[contains(text(), 'Next >']/@href").extract_first("")
        if next_url == "Next >":
            next_url = response.css(".pager a:last-child::attr(href)").extract_first("")
            yield Request(url=parse.urljoin(response.url, next_url), callback=self.parse)

    def parse_detail(self, response):
        match_re = re.match(".*?(\d+)", response.url)
        if match_re:
            post_id = match_re.group(1)
            # article_item = JobBoleArticleItem()
            # title = response.css("#news_title a::text").extract_first("")
            # create_date = response.css("#news_info .time::text").extract_first("")
            # match_re = re.match(".*?(\d+.*)", create_date)
            # if match_re:
            #     create_date = match_re.group(1)
            # content = response.css("#news_content").extract()[0]
            # tag_list = response.css(".news_tags a::text").extract()
            # tags = ",".join(tag_list)
            #
            # # html = requests.get(parse.urljoin(response.url, "/NewsAjax/GetAjaxNewsInfo?contentId={}".format(post_id)))
            # # j_data = json.loads(html.text)
            #
            # article_item["title"] = title
            # article_item["create_date"] = create_date
            # article_item["content"] = content
            # article_item["tags"] = tags
            # article_item["url"] = response.url
            # if response.meta.get("front_image_url", ""):
            #     article_item["front_image_url"] = [response.meta.get("front_image_url", "")]
            # else:
            #     article_item["front_image_url"] = []

            item_loader = ArticleItemLoader(item=JobBoleArticleItem(), response=response)
            item_loader.add_css("title", "#news_title a::text")
            item_loader.add_css("content", "#news_content")
            item_loader.add_css("tags", ".news_tags a::text")
            item_loader.add_css("create_date", "#news_info .time::text")
            item_loader.add_value("url", response.url)
            if response.meta.get("front_image_url", []):
                item_loader.add_value("front_image_url", response.meta.get("front_image_url", ""))

            # article_item = item_loader.load_item()

            yield Request(url=parse.urljoin(response.url, "/NewsAjax/GetAjaxNewsInfo?contentId={}".format(post_id)),
                          meta={"article_item": item_loader, "url": response.url}, callback=self.parse_nums)

    def parse_nums(self, response):
        j_data = json.loads(response.text)
        # article_item = response.meta.get("article_item", "")
        item_loader = response.meta.get("article_item", "")

        praise_nums = j_data["DiggCount"]
        fav_nums = j_data["TotalView"]
        comment_nums = j_data["CommentCount"]

        item_loader.add_value("praise_nums", j_data["DiggCount"])
        item_loader.add_value("fav_nums", j_data["TotalView"])
        item_loader.add_value("comment_nums", j_data["CommentCount"])
        item_loader.add_value("url_object_id", get_md5(response.meta.get("url", "")))
        # article_item["praise_nums"] = praise_nums
        # article_item["fav_nums"] = fav_nums
        # article_item["comment_nums"] = comment_nums
        # article_item["url_object_id"] = get_md5(article_item["url"] )

        article_item = item_loader.load_item()

        yield article_item
