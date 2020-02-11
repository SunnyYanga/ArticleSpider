# -*- coding: utf-8 -*-
import codecs
import json
from w3lib.html import remove_tags

from scrapy.pipelines.images import ImagesPipeline
from scrapy.exporters import JsonItemExporter
from twisted.enterprise import adbapi
import MySQLdb
from ArticleSpider.models.es_types import ArticleType

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


class ArticlespiderPipeline(object):
    def process_item(self, item, spider):
        return item


class MysqlPipeline(object):
    def __init__(self):  # init意味着方法可以被重载
        self.conn = MySQLdb.connect("127.0.0.1", 'root', '1234', 'article_spider', charset="utf8", use_unicode=True)
        self.cursor = self.conn.cursor()

    def process_item(self, item, spider):
        insert_sql = """ insert into jobbole_article(title, url, url_object_id, front_image_url, front_image_path, praise_nums, comment_nums, fav_nums, tags, content, create_date)
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        params = list()
        params.append(item.get("title", ""))
        params.append(item.get("url", ""))
        params.append(item.get("url_object_id", ""))
        front_image = ",".join(item.get("front_image_url", []))
        params.append(front_image)
        params.append(item.get("front_image_path", ""))
        params.append(item.get("praise_nums", 0))
        params.append(item.get("comment_nums", 0))
        params.append(item.get("fav_nums", 0))
        params.append(item.get("tags", ""))
        params.append(item.get("content", ""))
        params.append(item.get("create_date", "1970-07-01"))
        self.cursor.execute(insert_sql, tuple(params))
        self.conn.commit()
        return item


class MysqlTwistedPipeline(object):
    def __init__(self, dbpool):
        self.dbpool = dbpool

    @classmethod   # 类方法
    def from_settings(cls, settings):
        from MySQLdb.cursors import DictCursor
        dbparams = dict(
            host=settings["MYSQL_HOST"],
            db=settings["MYSQL_DBNAME"],
            user=settings["MYSQL_USER"],
            passwd=settings["MYSQL_PASSWORD"],
            charset='utf8',
            cursorclass=DictCursor,
            use_unicode=True,
        )
        dbpool = adbapi.ConnectionPool("MySQLdb", **dbparams)
        return cls(dbpool)

    def process_item(self, item, spider):
        query = self.dbpool.runInteraction(self.do_insert, item)
        query.addErrback(self.handle_error, item, spider)

    def handle_error(self, failure, item, spider):
        print(failure)

    def do_insert(self, cursor, item):
        # 根据不同的item 构建不同的sql语句病插入到mysql中
        insert_sql, params = item.get_insert_sql()
        # insert_sql = """ insert into jobbole_article(title, url, url_object_id, front_image_url, front_image_path, praise_nums, comment_nums, fav_nums, tags, content, create_date)
        #         values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE praise_nums=VALUES(praise_nums), comment_nums=VALUES(comment_nums), fav_nums=VALUES(fav_nums)
        #         """
        # params = list()
        # params.append(item.get("title", ""))
        # params.append(item.get("url", ""))
        # params.append(item.get("url_object_id", ""))
        # front_image = ",".join(item.get("front_image_url", []))
        # params.append(front_image)
        # params.append(item.get("front_image_path", ""))
        # params.append(item.get("praise_nums", 0))
        # params.append(item.get("comment_nums", 0))
        # params.append(item.get("fav_nums", 0))
        # params.append(item.get("tags", ""))
        # params.append(item.get("content", ""))
        # params.append(item.get("create_date", "1970-07-01"))

        cursor.execute(insert_sql, tuple(params))


class JsonWithEncodingPipeline(object):
    # 自定义Json文件的导出
    def __init__(self):  # init意味着方法可以被重载
        self.file = codecs.open("article.json", "a", encoding="utf-8")

    def process_item(self, item, spider):
        lines = json.dumps(dict(item), ensure_ascii=False) + "\n"
        self.file.write(lines)
        return item

    def spider_closed(self, spider):
        self.file.close()


class JsonExporterPipeline(object):
    def __init__(self):  # init意味着方法可以被重载
        self.file = open("articleExport.json", "wb")  # wb 二进制
        self.exporter = JsonItemExporter(self.file, encoding="utf-8", ensure_ascii=False)
        self.exporter.start_exporting()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item

    def spider_closed(self, spider):
        self.exporter.finish_exporting()
        self.file.close()


class ArticleImagePipeline(ImagesPipeline):
    def item_completed(self, results, item, info):
        if "front_image_url" in item:
            image_file_path = ""
            for ok, value in results:
                image_file_path = value["path"]
            item["front_image_path"] = image_file_path

        return item


class ElasticsearchPipeline(object):
    def process_item(self, item, spider):
        # 将item转换为es的数据
        item.save_to_es()

        return item