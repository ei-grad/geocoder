#!/usr/bin/env python
# encoding: utf-8

from elasticsearch import Elasticsearch

from flask import Flask, request, current_app
from flask.json import dumps

app = Flask(__name__)


@app.route("/")
def home():
    return "GET /get/street?name=Москва, Кремль\nGET /get/latlon?lat=55.752561&lon=37.618686\n"


def jsonify(*args, **kwargs):
    indent = None
    if current_app.config['JSONIFY_PRETTYPRINT_REGULAR'] and not request.is_xhr:
        indent = 2
    return current_app.response_class(
        dumps(dict(*args, **kwargs), indent=indent),
        mimetype='application/json; charset=utf-8'
    )


@app.route("/by_name")
def by_streetname():

    name = request.args.get("name")

    assert name

    tokens = current_app.es.indices.analyze('navstreets', body=name, analyzer='my_analyzer')['tokens']

    res = current_app.es.search('navstreets', 'address_loc', {"query": {"fuzzy_like_this": {
        "like_text": ' '.join(i['token'] for i in tokens),
        "fuzziness": 0.45,
    }}})

    return jsonify(result=[i['_source'] for i in res['hits']['hits']])


@app.route("/by_latlon")
def by_latlon():

    assert request.args.get('lat') and request.args.get('lon')

    loc = {"lat": float(request.args.get("lat")),
           "lon": float(request.args.get("lon"))}

    res = current_app.es.search('navstreets', 'address_loc', {
        "query": {
            "filtered": {
                "filter": {
                    "geo_distance": {
                        "distance": "500m",
                        "loc": loc
                    }
                }
            }
        },
        "sort": [{
            "_geo_distance": {
                "loc": loc,
                "order": "asc",
                "unit": "m"
            }
        }],
    })
    return jsonify(result=[i['_source'] for i in res['hits']['hits']])


if __name__ == "__main__":
    app.es = Elasticsearch()
    app.debug = True
    app.config['JSON_AS_ASCII'] = False
    app.run(host='0.0.0.0')
