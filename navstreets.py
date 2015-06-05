#!/usr/bin/env python
# encoding: utf-8

import sys
from os import path as op
from collections import defaultdict

import ogr

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk


def p(fname):
    return op.join(sys.argv[1], fname)


def ogr_features(fname):
    f = ogr.Open(p(fname))
    l = f.GetLayer()
    for i in l:
        yield i


points = defaultdict(list)
streets = {}

for feature in ogr_features('PointAddress.shp'):
    link_id = feature['LINK_ID']
    lon, lat = feature.geometry().GetPoints()[0]
    points[link_id].append({
        'address': feature['ADDRESS'],
        'loc': {'lon': lon, 'lat': lat},
    })

for feature in ogr_features('Streets.shp'):
    link_id = feature['LINK_ID']
    if link_id in points:
        streets[link_id] = feature['ST_NAME']


def documents():
    for link_id, pts in points.iteritems():
        for pt in pts:
            yield {
                '_index': 'navstreets',
                '_type': 'address_loc',
                'address': ', '.join([streets[link_id], pt['address']]),
                # XXX: 'area':
                'loc': pt['loc']
            }

es = Elasticsearch()

es.indices.create('navstreets', {
    "mappings": {
        "address_loc": {
            "_all": {"analyzer": "russian_morphology"},
            "properties": {
                "loc": {"type": "geo_point"}
            }
        }
    },
    "settings": {
        "analysis": {
            "analyzer": {
                "my_analyzer": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": ["word_delimiter", "lowercase",
                               "russian_morphology", "english_morphology",
                               "my_stopwords"]
                }
            },
            "filter": {
                "my_stopwords": {
                    "type": "stop",
                    "stopwords": (
                        "а,без,более,бы,был,была,были,было,быть,в,вам,вас,весь,во,вот,все,всего,всех,вы,где,"
                        "да,даже,для,до,его,ее,если,есть,еще,же,за,здесь,и,из,или,им,их,к,как,ко,когда,кто,"
                        "ли,либо,мне,может,мы,на,надо,наш,не,него,нее,нет,ни,них,но,ну,о,об,однако,он,она,"
                        "они,оно,от,очень,по,под,при,с,со,так,также,такой,там,те,тем,то,того,тоже,той,только,"
                        "том,ты,у,уже,хотя,чего,чей,чем,что,чтобы,чье,чья,эта,эти,это,я,"
                        "a,an,and,are,as,at,be,but,by,for,if,in,into,is,it,no,not,of,on,or,such,that,"
                        "the,their,then,there,these,they,this,to,was,will,with"
                    )
                }
            }
        }
    },
})

bulk(es, documents())
