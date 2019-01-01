#!/usr/bin/env/ python

from oauth2client.service_account import ServiceAccountCredentials
from apiclient.discovery import build
import pprint
import re

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly'] 
KEY_FILE_LOCATION = 'dashboard-ce334551fc25.json'
YHR_VIEW_ID = "60921314"
ROLLUP_VIEW_ID = "101310609"

class GAConnection:

    def __init__(self, key_file, scope):
        self.credentials = ServiceAccountCredentials.from_json_keyfile_name( key_file, scope )
        self.service = build('analyticsreporting', 'v4', credentials=self.credentials)

    def build_query(self, view_id, s_day="today", e_day="today", dimensions=[], d_filter=[], metrics=[], m_filter=[], orderby=[], pagesize=1000):
        body = {
            'viewId': view_id,
            'dateRanges': [{'startDate':s_day, 'endDate':e_day}]
        }
        if len(dimensions) > 0:
            dimensionsBuilder = [{"name":d} for d in dimensions]
            body['dimensions'] = dimensionsBuilder
        if len(d_filter.keys()) > 0:
            d_filterBuilder = {}
            filter_d = {}
            filters_list = []
            for k, v in d_filter.items():
                filter_d[k] = v
                filters_list.append(filter_d)
            d_filterBuilder["filters"] = filters_list
            body['dimensionFilterClauses'] = d_filterBuilder
        if len(metrics) > 0:
            metricBuilder = [{"expression":m} for m in metrics]
            body['metrics'] = metricBuilder
        if len(orderby) > 0:
            o = {
                "fieldName": orderby[0],
                'sortOrder': orderby[1]
            }
            body['orderBys'] = o
        body['pageSize'] = pagesize
        print(body)
        return body

    def get_reports(self, body):
        t = self.service.reports().batchGet(
            body={
                'reportRequests':[
                    body
                ]
            }
        ).execute()
        return(t)

class Page:

    def __init__(self, url, title, metrics):
        self.url = url
        self.title = title.split('|')[0].strip()
        
        split_url = url.split('www.yakimaherald.com')
        split_url = split_url[-1]
        split_url = split_url.split('#')[0]
        page_type = split_url.split('/')[1]
        uid = split_url.split('/')[-1]
        uid = uid.split('.')[0]

        self.uid = uid
        self.page_type = page_type
        self.metrics = metrics

    def __str__(self):
        return "{0}".format(self.title)

class Story:

    def __init__(self, uid):
        self.uid = uid
        self.urls = []
        self.titles = []
        self.pageviews = 0
        self.uniques = 0
        self.seconds = 0
        
        
    def add_page(self, page_url, page_title, page_type, metrics):
        self.urls.append(page_url)
        self.titles.append([page_title, page_type])
        self.uniques += int(metrics[0])
        self.pageviews += int(metrics[1])
        self.seconds += float(metrics[2])


    def __str__(self):
        return "{0!s}, {1!s}, {2!s}, {3!s}, {4!s}".format(self.uid, self.titles, self.pageviews, self.uniques, self.seconds)


if __name__ == '__main__':
    


    def articleQuery(connector, view_id, start_day, end_day):
        from operator import attrgetter
        articleQuery = connector.build_query(view_id, s_day=start_day, e_day=end_day, dimensions=["ga:pagePath", "ga:pageTitle"], metrics=['ga:pageviews', 'ga:Uniquepageviews', "ga:timeOnPage"], orderby=["ga:uniquePageviews", "DESCENDING"], d_filter={'dimensionName':"ga:pagePath", 'not':'false', 'operator':"PARTIAL", 'expressions':["article_"], 'caseSensitive':'false'}, pagesize=50)
        articleAnalyticsRaw = connector.get_reports(articleQuery)
        z = articleAnalyticsRaw.get('reports', [])[0].get('data', []).get('rows')
        stories = {}
        for row in z:
            dims = row['dimensions']
            mets = row['metrics'][0].get('values',[])
            p = Page(dims[0], dims[1], mets)
            if p.uid not in stories.keys():
                new_story = Story(p.uid)
                new_story.add_page(p.url, p.title, p.page_type, p.metrics)
                stories[p.uid] = new_story
                #story_order.append(p.uid)
            else:
                stories[p.uid].add_page(p.url, p.title, p.page_type, p.metrics)
        story_order = [v for k, v in stories.items()]
        story_order_sorted = sorted(story_order, key=attrgetter('pageviews'), reverse=True)
        outfile_name_string = str(start_day) + str(end_day) + ".csv"
        articles = open(outfile_name_string, 'w')
        for story in story_order_sorted:
            f = attrgetter("titles")
            a = f(story)
            print(a)
            slugs = [s[0].strip().replace(",", "") for s in story.titles]
            print(slugs)
            slugs = ";".join(slugs)
            articles.write("{0!s}, {1!s}, {2!s}, {3!s}, {4!s}".format(slugs, story.pageviews, story.uniques, story.seconds, story.urls))
            articles.write('\n')
        articles.close()

    def totalQuery(connector, view_id, start_day, end_day):
        t = connector.service.reports().batchGet(
            body={
                'reportRequests':[
                    {
                        'viewId':view_id,
                        'dateRanges': [{'startDate':start_day, 'endDate':end_day}],
                        'metrics':[{"expression":"ga:uniquePageviews"}, {"expression":"ga:sessions"}, {"expression":"ga:pageviews"}]
                    }
                ]
            }
        ).execute()
        return(t)

    ga_reporting = GAConnection(KEY_FILE_LOCATION, SCOPES)

    #weekend = articleQuery(ga_reporting, YHR_VIEW_ID, "2018-12-28", "2018-12-30")
    #today = articleQuery(ga_reporting, YHR_VIEW_ID, "today", "today")
    all2018 = articleQuery(ga_reporting, YHR_VIEW_ID, "2018-01-01", "2018-12-30")
    #yesterday = articleQuery(ga_reporting, YHR_VIEW_ID, "yesterday", "yesterday")
    #print(today)
    #print(yesterday)
    #print(totalQuery(ga_reporting,YHR_VIEW_ID,"today","today"))
    #print(totalQuery(ga_reporting,YHR_VIEW_ID,"2018-12-28","2018-12-30"))
    



