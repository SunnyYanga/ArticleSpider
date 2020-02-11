# -*- coding: utf-8 -*-
import scrapy
import re
import json
import pickle   # 序列化对象
import datetime
from selenium import webdriver
import time
from mouse import move, click
from urllib import parse
from scrapy.loader import ItemLoader

from ArticleSpider.items import ZhihuQuestionItem, ZhihuAnswerItem


class ZhihuSpider(scrapy.Spider):
    name = 'zhihu'
    allowed_domains = ['www.zhihu.com']
    start_urls = ['http://www.zhihu.com/']

    # question的第一页answer的请求url
    start_answer_url = "https://www.zhihu.com/api/v4/questions/{0}/answers?include=data%5B%2A%5D.is_normal%2Cadmin_closed_comment%2Creward_info%2Cis_collapsed%2Cannotation_action%2Cannotation_detail%2Ccollapse_reason%2Cis_sticky%2Ccollapsed_by%2Csuggest_edit%2Ccomment_count%2Ccan_comment%2Ccontent%2Ceditable_content%2Cvoteup_count%2Creshipment_settings%2Ccomment_permission%2Ccreated_time%2Cupdated_time%2Creview_info%2Crelevant_info%2Cquestion%2Cexcerpt%2Crelationship.is_authorized%2Cis_author%2Cvoting%2Cis_thanked%2Cis_nothelp%2Cis_labeled%2Cis_recognized%2Cpaid_info%2Cpaid_info_content%3Bdata%5B%2A%5D.mark_infos%5B%2A%5D.url%3Bdata%5B%2A%5D.author.follower_count%2Cbadge%5B%2A%5D.topics&limit={1}&offset={2}&platform=desktop&sort_by=default"

    headers = {
        "HOST": "www.zhihu.com",
        "Referer": "https://www.zhihu.com",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36",
    }

    custom_settings = {
        "COOKIES_ENABLED": True
    }

    def parse(self, response):
        all_urls = response.css("a::attr(href)").extract()
        all_urls = [parse.urljoin(response.url, url) for url in all_urls]
        all_urls = filter(lambda x: True if x.startswith("https") else False, all_urls)
        for url in all_urls:
            match_obj = re.match("(.*zhihu.com/question/(\d+))(/|$).*", url)
            if match_obj:
                request_url = match_obj.group(1)
                question_id = match_obj.group(2)
                yield scrapy.Request(request_url, headers=self.headers, callback=self.parse_question)
            else:
                yield scrapy.Request(url, headers=self.headers, callback=self.parse)

    def parse_question(self, response):
        item_loader = ItemLoader(item=ZhihuQuestionItem(), response=response)
        item_loader.add_css("title", ".QuestionHeader-title::text")
        # item_loader.add_xpath("title", "//*[@class='QuestionHeader-title']/text()")
        item_loader.add_css("content", ".QuestionHeader-detail")
        item_loader.add_value("url", response.url)
        match_obj = re.match("(.*zhihu.com/question/(\d+))(/|$).*", response.url)
        if match_obj:
            question_id = int(match_obj.group(2))
        item_loader.add_value("zhihu_id", question_id)
        item_loader.add_css("answer_num", ".List-headerText span::text")
        item_loader.add_css("comments_num", ".QuestionHeaderActions button::text")
        item_loader.add_css("watch_user_num", ".NumberBoard-itemValue ::text")
        item_loader.add_css("topics", ".QuestionHeader-topics .Popover div::text")

        question_item = item_loader.load_item()
        yield scrapy.Request(self.start_answer_url.format(question_id, 20, 0), headers=self.headers, callback=self.parse_answer)
        yield question_item

    # 处理question的answer
    def parse_answer(self, response):
        ans_json = json.loads(response.text)
        is_end = ans_json["paging"]["is_end"]
        next_url = ans_json["paging"]["next"]

        # 提取answer的具体字段
        for answer in ans_json["data"]:
            answer_item = ZhihuAnswerItem()
            answer_item["zhihu_id"] = answer["id"]
            answer_item["url"] = answer["url"]
            answer_item["question_id"] = answer["question"]["id"]
            answer_item["author_id"] = answer["author"] if "id" in answer["author"] else None
            answer_item["content"] = answer["content"] if "content" in answer else None
            answer_item["praise_num"] = answer["voteup_count"]
            answer_item["comments_num"] = answer["comment_count"]
            answer_item["create_time"] = answer["created_time"]
            answer_item["update_time"] = answer["updated_time"]
            answer_item["crawl_time"] = datetime.now()

            yield answer_item

        if not is_end:
            yield scrapy.Request(next_url, headers=self.headers, callback=self.parse_answer)
        pass

    # def start_requests(self):
    #     from selenium.webdriver.chrome.options import Options
    #     from selenium.webdriver.common.keys import Keys
    #     chrome_option = Options()
    #     chrome_option.add_argument("--disable-extensions")
    #     chrome_option.add_experimental_option("debuggerAddress", "127.0.0.1:9222")
    #
    #     browser = webdriver.Chrome(executable_path="D:/tools/chromedriver.exe", chrome_options=chrome_option)
    #     try:
    #         browser.maximize_window()
    #     except:
    #         pass
    #
    #     browser.get("https://www.zhihu.com/signin?next=%2F")
    #     browser.find_element_by_css_selector(".SignFlow-tabs div:nth-child(2)").click()
    #     browser.find_element_by_css_selector(".SignFlow-accountInput.Input-wrapper input").send_keys(Keys.CONTROL + "a")
    #     browser.find_element_by_css_selector(".SignFlow-accountInput.Input-wrapper input").send_keys("17720203983")
    #     browser.find_element_by_css_selector(".SignFlow-password input").send_keys(Keys.CONTROL + "a")
    #     browser.find_element_by_css_selector(".SignFlow-password input").send_keys("ygmm123...")
    #     browser.find_element_by_css_selector(".SignFlow-submitButton").click()
    #     time.sleep(10)
    #     login_success = False
    #     while login_success:
    #         try:
    #             notify_ele = browser.find_element_by_class_name("Popover PushNotifications AppHeader-notifications")
    #             login_success = True
    #         except:
    #             pass
    #
    #         try:
    #             english_captcha_element = browser.find_element_by_class_name("Captcha-englishImg")
    #         except:
    #             english_captcha_element = None
    #
    #         try:
    #             chinese_captcha_element = browser.find_element_by_class_name("Captcha-chineseImg")
    #         except:
    #             chinese_captcha_element = None
    #
    #         if chinese_captcha_element:
    #             ele_position = chinese_captcha_element.location
    #             x_relative = ele_position["x"]
    #             y_relative = ele_position["y"]
    #             # 地址栏高度
    #             browser_navigation_panel_height = browser.execute_script('return window.outerHeight - window.innerHeight;')
    #             base64_text = chinese_captcha_element.get_attribute("src")
    #             import base64
    #             code = base64_text.replace("data:image/jpg;base64,", "").replace("%OA", "")
    #             fh = open("yzm_cn.jpeg", "wb")
    #             fh.write(base64.b64decode(code))
    #             fh.close()
    #
    #             # 处理倒立文字
    #             from zheye import zheye
    #             z = zheye()
    #             positions = z.Recognize('yzm_cn.jpeg')
    #             last_position = []
    #             if len(positions) == 2:
    #                 if positions[0][1] > positions[1][1]:
    #                     last_position.append([positions[1][1], positions[1][0]])
    #                     last_position.append([positions[0][1], positions[0][0]])
    #                 else:
    #                     last_position.append([positions[0][1], positions[0][0]])
    #                     last_position.append([positions[1][1], positions[1][0]])
    #                 # 取第0个元素x轴，y轴除以2
    #                 first_position = [int(last_position[0][0] / 2), int(last_position[0][1] / 2)]
    #                 second_position = [int(last_position[1][0] / 2), int(last_position[1][1] / 2)]
    #                 move(x_relative + first_position[0],
    #                      y_relative + browser_navigation_panel_height + first_position[1])
    #                 click()
    #                 time.sleep(3)
    #                 move(x_relative + second_position[0],
    #                      y_relative + browser_navigation_panel_height + second_position[1])
    #                 click()
    #             else:
    #                 last_position.append([positions[0][1], positions[0][0]])
    #                 first_position = [int(last_position[0][0] / 2), int(last_position[0][1] / 2)]
    #                 move(x_relative + first_position[0],
    #                      y_relative + browser_navigation_panel_height + first_position[1])
    #                 click()
    #
    #             browser.find_element_by_css_selector(".SignFlow-accountInput.Input-wrapper input").send_keys(
    #                 Keys.CONTROL + "a")
    #             browser.find_element_by_css_selector(".SignFlow-accountInput.Input-wrapper input").send_keys(
    #                 "17720203983")
    #             browser.find_element_by_css_selector(".SignFlow-password input").send_keys(Keys.CONTROL + "a")
    #             browser.find_element_by_css_selector(".SignFlow-password input").send_keys("ygmm123...")
    #             browser.find_element_by_css_selector(".SignFlow-submitButton").click()
    #
    #         if english_captcha_element:
    #             base64_text = chinese_captcha_element.get_attribute("src")
    #             import base64
    #             code = base64_text.replace("data:image/jpg;base64,", "").replace("%OA", "")
    #             fh = open("yzm_en.jpeg", "wb")
    #             fh.write(base64.b64decode(code))
    #             fh.close()
    #
    #             browser.find_element_by_css_selector(".SignFlow-accountInput.Input-wrapper input").send_keys(
    #                 Keys.CONTROL + "a")
    #             browser.find_element_by_css_selector(".SignFlow-accountInput.Input-wrapper input").send_keys(
    #                 "17720203983")
    #             browser.find_element_by_css_selector(".SignFlow-password input").send_keys(Keys.CONTROL + "a")
    #             browser.find_element_by_css_selector(".SignFlow-password input").send_keys("ygmm123...")
    #             browser.find_element_by_css_selector(".SignFlow-submitButton").click()

    def start_requests(self):  # 复写此方法， 此方法是Spider的入口方法，复写后用于登录
        # 手动启动chromedriver
        """
        1.启动chrome(启动之前确保所有的chrome实例已经关闭)
        :return:
        """
        cookies = pickle.load(open("C:/Users/yang1/ArticleSpider/cookies/zhihu.cookie", "rb"))
        cookie_dict = {}
        for cookie in cookies:
            cookie_dict[cookie["name"]] = cookie["value"]
        return [scrapy.Request(url=self.start_urls[0], dont_filter=True, cookies=cookie_dict)]

        # from selenium.webdriver.chrome.options import Options
        # from selenium.webdriver.common.keys import Keys
        #
        # chrome_option = Options()
        # chrome_option.add_argument("--disable-extensions")
        # chrome_option.add_experimental_option("debuggerAddress", "127.0.0.1:9222")
        #
        # browser = webdriver.Chrome(executable_path="D:/tools/chromedriver.exe", chrome_options=chrome_option)

        # browser.get("https://www.zhihu.com/signin?next=%2F")
        # browser.find_element_by_css_selector(".SignFlow-tabs div:nth-child(2)").click()
        # # 选中输入区域再输入可以清除
        # browser.find_element_by_css_selector(".SignFlow-accountInput.Input-wrapper input").send_keys(Keys.CONTROL + "a")
        # browser.find_element_by_css_selector(".SignFlow-accountInput.Input-wrapper input").send_keys("17720203983")
        # browser.find_element_by_css_selector(".SignFlow-password input").send_keys(Keys.CONTROL + "a")
        # browser.find_element_by_css_selector(".SignFlow-password input").send_keys("ygmm123...")
        # # move(895, 803)
        # # click()
        # browser.find_element_by_css_selector(".SignFlow-submitButton").click()

        # browser.get("https://www.zhihu.com/")
        # cookies = browser.get_cookies()
        # pickle.dump(cookies, open("C:/Users/yang1/ArticleSpider/cookies/zhihu.cookie", "wb"))
        # cookie_dict = {}
        # for cookie in cookies:
        #     cookie_dict[cookie["name"]] = cookie["value"]
        # return [scrapy.Request(url=self.start_urls[0], dont_filter=True, cookies=cookie_dict)]