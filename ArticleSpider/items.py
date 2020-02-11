# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
import re
from datetime import datetime
from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose, TakeFirst, Identity, Join
from w3lib.html import remove_tags

from .utils.common import extract_num
from .settings import SQL_DATETIME_FORMAT, SQL_DATE_FORMAT
from ArticleSpider.models.es_types import ArticleType
import redis

from elasticsearch_dsl.connections import connections
es = connections.create_connection(ArticleType._doc_type.using)

redis_cli = redis.StrictRedis(host="localhost")


class ArticlespiderItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass


def gen_suggests(index, info_tuple):
    # 根据字符串生成搜索建议数组
    used_words = set()
    suggests = []
    for text, weight in info_tuple:
        if text:
            # 调用es的analyzer接口分析字符串
            words = es.indices.analyze(index=index, analyzer="ik_max_word", params={'filter': ["lowercase"]}, body=text)
            analyzed_words = set([r["token"] for r in words["tokens"] if len(r["token"]) > 1])
            new_words = analyzed_words - used_words
            used_words = used_words | new_words
        else:
            new_words = set()

        if new_words:
            suggests.append({"input": list(new_words), "weight": weight})

    return suggests


def date_convert(value):
    match_re = re.match(".*?(\d+.*)", value)
    if match_re:
        return match_re.group(1)
    else:
        return "1970-07-01"


def get_nums(value):
    match_re = re.match(".*?(\d+).*", value)
    if match_re:
        return int(match_re.group(1))
    else:
        return 0


class ArticleItemLoader(ItemLoader):
    default_output_processor = TakeFirst()


class JobBoleArticleItem(scrapy.Item):
    title = scrapy.Field()
    create_date = scrapy.Field(
        input_processor=MapCompose(date_convert)
    )
    url = scrapy.Field()
    url_object_id = scrapy.Field()
    front_image_url = scrapy.Field(
        output_processor=Identity()
    )
    front_image_path = scrapy.Field()
    praise_nums = scrapy.Field()
    comment_nums = scrapy.Field()
    fav_nums = scrapy.Field()
    tags = scrapy.Field(
        output_processor=Join(separator=",")
    )
    content = scrapy.Field()

    def get_insert_sql(self):
        insert_sql = """ insert into jobbole_article(title, url, url_object_id, front_image_url, front_image_path, praise_nums, comment_nums, fav_nums, tags, content, create_date)
                        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE praise_nums=VALUES(praise_nums), comment_nums=VALUES(comment_nums), fav_nums=VALUES(fav_nums)
                        """
        params = list()
        params.append(self.get("title", ""))
        params.append(self.get("url", ""))
        params.append(self.get("url_object_id", ""))
        front_image = ",".join(self.get("front_image_url", []))
        params.append(front_image)
        params.append(self.get("front_image_path", ""))
        params.append(self.get("praise_nums", 0))
        params.append(self.get("comment_nums", 0))
        params.append(self.get("fav_nums", 0))
        params.append(self.get("tags", ""))
        params.append(self.get("content", ""))
        params.append(self.get("create_date", "1970-07-01"))
        return insert_sql, params

    def save_to_es(self):
        article = ArticleType()
        article.title = self["title"]
        article.create_date = self["create_date"]
        article.content = remove_tags(self["content"])
        article.front_image_url = self["front_image_url"]
        if "front_image_path" in self:
            article.front_image_path = self["front_image_path"]
        article.praise_nums = self["praise_nums"]
        article.fav_nums = self["fav_nums"]
        article.comment_nums = self["comment_nums"]
        article.url = self["url"]
        article.tags = self["tags"]
        article.meta.id = self["url_object_id"]
        article.suggest = gen_suggests(ArticleType._doc_type.index, ((article.title, 10), (article.tags, 7)))

        article.save()
        redis_cli.incr("jobbole_count")
        return


# 知乎的问题item
class ZhihuQuestionItem(scrapy.Item):
    zhihu_id = scrapy.Field()
    topics = scrapy.Field()
    url = scrapy.Field()
    title = scrapy.Field()
    content = scrapy.Field()
    answer_num = scrapy.Field()
    comments_num = scrapy.Field()
    watch_user_num = scrapy.Field()
    click_num = scrapy.Field()
    crawl_time = scrapy.Field()

    def get_insert_sql(self):
        insert_sql = """ insert into zhihu_question(zhihu_id, topics, url, title, content, answer_num, comments_num, watch_user_num, click_num, crawl_time)
                        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE answer_num=VALUES(answer_num), comment_nums=VALUES(comment_nums), watch_user_num=VALUES(watch_user_num), click_num=VALUES(click_num)
                        """
        zhihu_id = int("".join(self["zhihu_id"]))
        topics = ",".join(self["zhihu_id"])
        url = self["url"][0]
        title = ",".join(self["title"])
        content = "".join(self["content"])
        answer_num = extract_num("".join(self["answer_num"]))
        comments_num = extract_num("".join(self["commnets_num"]))
        watch_user_num = extract_num("".join(self["watch_user_num"]))
        click_num = extract_num("".join(self["click_num"]))
        crawl_time = datetime.datetime.now().strftime(SQL_DATETIME_FORMAT)
        params = list()
        params.append(zhihu_id)
        params.append(topics)
        params.append(url)
        params.append(title)
        params.append(content)
        params.append(answer_num)
        params.append(comments_num)
        params.append(watch_user_num)
        params.append(click_num)
        params.append(crawl_time)
        return insert_sql, params


# 知乎的问题回答item
class ZhihuAnswerItem(scrapy.Item):
    zhihu_id = scrapy.Field()
    url = scrapy.Field()
    question_id = scrapy.Field()
    author_id = scrapy.Field()
    content = scrapy.Field()
    comments_num = scrapy.Field()
    praise_num = scrapy.Field()
    create_time = scrapy.Field()
    update_time = scrapy.Field()
    crawl_time = scrapy.Field()

    def get_insert_sql(self):
        insert_sql = """ insert into zhihu_answer(zhihu_id, url, question_id, author_id, content, praise_num, comments_num, create_time, update_time, crawl_time)
                        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE praise_num=VALUES(praise_nums), comments_num=VALUES(comment_nums)
                        """
        create_time = datetime.datetime.fromtimestamp(self["create_time"]).strftime(SQL_DATETIME_FORMAT)
        update_time = datetime.datetime.fromtimestamp(self["update_time"]).strftime(SQL_DATETIME_FORMAT)
        params = list()
        params.append(self["zhihu_id"])
        params.append(self["url"])
        params.append(self["question_id"])
        params.append(self["author_id"])
        params.append(self["content"])
        params.append(self["praise_num"])
        params.append(self["comments_num"])
        params.append(create_time)
        params.append(update_time)
        params.append(self["crawl_time"].strftime(SQL_DATETIME_FORMAT))
        return insert_sql, params


def remove_splash(value):
    # 去掉工作城市的斜线
    return value.replace("/", "")


def handle_job_addr(value):
    addr_list = value.spilt("\n")
    addr_list = [item.strip() for item in addr_list if item.strip() != "查看地图"]
    return "".join(addr_list)


class LagouJobItemLoader(ItemLoader):
    default_output_processor = TakeFirst()


class LagouJobItem(scrapy.Item):
    # 拉勾网职位信息
    title = scrapy.Field()
    url = scrapy.Field()
    url_object_id = scrapy.Field()
    salary = scrapy.Field()
    job_city = scrapy.Field(input_processor=MapCompose(remove_splash))
    work_years = scrapy.Field(input_processor=MapCompose(remove_splash))
    degree_need = scrapy.Field(input_processor=MapCompose(remove_splash))
    job_type = scrapy.Field()
    publish_time = scrapy.Field()
    job_advantage = scrapy.Field()
    job_desc = scrapy.Field()
    job_addr = scrapy.Field(input_processor=MapCompose(remove_tags, handle_job_addr))
    company_name = scrapy.Field()
    company_url = scrapy.Field()
    tags = scrapy.Field(input_processor=Join(","))
    crawl_time = scrapy.Field()

    def get_insert_sql(self):
        insert_sql = """ insert into lagou_job(title, url, url_object_id, salary, job_city, work_years, degree_need, job_type, publish_time, tags, job_advantage, job_desc, job_addr, company_url, company_name)
                                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE salary=VALUES(salary), job_desc=VALUES(job_desc)
                                """
        params = list()
        params.append(self["title"])
        params.append(self["url"])
        params.append(self["url_object_id"])
        params.append(self["salary"])
        params.append(self["job_city"])
        params.append(self["work_years"])
        params.append(self["degree_need"])
        params.append(self["job_type"])
        params.append(self["publish_time"])
        params.append(self["tags"])
        params.append(self["job_advantage"])
        params.append(self["job_desc"])
        params.append(self["job_addr"])
        params.append(self["company_url"])
        params.append(self["company_name"])
        # params.append(self["crawl_time"].strftime(SQL_DATETIME_FORMAT))
        return insert_sql, params
