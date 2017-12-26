# -*- coding: utf-8 -*-
'''
Created on 2017-12-26 16:01
---------
@summary: csr 热点聚类
---------
@author: Boris
'''

import sys
sys.path.append('..')
import init

import utils.tools as tools
from db.elastic_search import ES
from cluster.compare_text import compare_text
from utils.log import log

MIN_SIMILARITY = 0.5 # 相似度阈值
SLEEP_TIME = int(tools.get_conf_value('config.conf', 'sync', 'sleep_time'))
STO_CURRENT_ID_FILE = '.current_id'

class NewsCluster():
    def __init__(self):
        self._es = ES()
        self._current_csr_res_id = tools.read_file(STO_CURRENT_ID_FILE)
        self._current_csr_res_id = self._current_csr_res_id and int(self._current_csr_res_id) or 0

    def _get_same_day_hots(self, text, start_time):
        news_day_time = start_time[:start_time.find(' ')]
        body = {
          "query": {
            "filtered": {
              "filter": {
                "range": {
                   "start_time": {
                        "gte": news_day_time + ' 00:00:00',
                        'lte': news_day_time + ' 59:59:59'
                    }
                }
              },
              "query": {
                "multi_match": {
                    "query": text,
                    "fields": [
                        "csr_content"
                    ],
                    "operator": "or",
                    "minimum_should_match": "{percent}%".format(percent = int(MIN_SIMILARITY * 100)) # 匹配到的关键词占比
                }
              }
            }
          },
          "_source": [
                "hot_id",
                "csr_res_ids",
                "csr_content",
                'hot'
          ],
          "highlight": {
                "fields": {
                    "csr_content": {}
                }
          }
        }

        # 默认按照匹配分数排序
        hots = self._es.search('tab_news_csr_hot', body)
        # print(tools.dumps_json(hots))

        return hots.get('hits', {}).get('hits', [])

    def _save_current_id(self):
        '''
        @summary: 保存做到的id， 下次接着做
        ---------
        ---------
        @result:
        '''

        tools.write_file(STO_CURRENT_ID_FILE, str(self._current_csr_res_id))

    def deal_news(self):
        '''
        @summary: 取tab_news_csr_result信息
        ---------
        ---------
        @result:
        '''
        while True:
            body = {
                "query": {
                    "filtered": {
                        "filter": {
                            "range": {
                               "csr_res_id": { # 查询大于该csr_res_id 的信息
                                    "gt": self._current_csr_res_id
                                }
                        }
                      }
                    }
                },
                "_source": [
                    "csr_res_id",
                    "csr_content",
                    "start_time"
                ],
                "sort":[{"csr_res_id":"asc"}]
            }

            news_json = self._es.search('tab_news_csr_result', body)
            news_list = news_json.get('hits', {}).get('hits', [])

            if not news_list:
                log.debug('tab_news_csr_result 表中无大于%s的csr_res_id\nsleep %s...'%(self._current_csr_res_id, SLEEP_TIME))
                tools.delay_time(SLEEP_TIME)
                continue

            for news_info in news_list:
                news = news_info.get('_source')
                csr_res_id = news.get('csr_res_id')
                csr_content = news.get('csr_content')
                start_time = news.get('start_time')

                log.debug('''
                    处理 tab_news_csr_result
                    csr_res_id  %s
                    start_time  %s
                    csr_content %s
                    '''%(csr_res_id, start_time, csr_content))

                # 找相似文章
                similar_hot = None
                hots = self._get_same_day_hots(csr_content, start_time)

                # 遍历相似的文章，比较相似度
                for hot_info in hots:
                    hot = hot_info.get('_source')
                    hot_text = hot.get('csr_content')

                    temp_similarity = compare_text(csr_content, hot_text)
                    if temp_similarity > MIN_SIMILARITY:
                        similar_hot = hot

                    break #hots 按照匹配值排序后，第一个肯定是最相似的，无需向后比较

                # 如果找到相似的文章，追加csr_res_id和hot值， 否则将该条信息最为新的热点
                if similar_hot:# 找到相似的热点
                    log.debug('找到所属热点：%s'%similar_hot.get('csr_content'))

                    data = {}

                    # 更新热点的热度及追加文章的id
                    data["hot"] = similar_hot["hot"] + 1
                    data["csr_res_ids"] = similar_hot["csr_res_ids"] + ',' + csr_res_id

                    # 更新热点
                    self._es.update_by_id("tab_news_csr_hot", data_id = similar_hot.get("hot_id"), data = data)

                else: # 没有找到相似的热点， 将当前文章作为热点
                    log.debug('无所属热点')

                    hot_info = {
                        'hot_id' : csr_res_id,
                        'hot' : 1,
                        'start_time' : start_time,
                        'csr_res_ids' : csr_res_id,
                        'csr_content' : csr_content
                    }
                    self._es.add('tab_news_csr_hot', hot_info, data_id = csr_res_id)

                # 保存当前的id
                self._current_csr_res_id = csr_res_id
                self._save_current_id()

if __name__ == '__main__':
    news_cluster = NewsCluster()
    news_cluster.deal_news()