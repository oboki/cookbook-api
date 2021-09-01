from hashlib import blake2b
from datetime import datetime


def create_hash_id(s):
    h = blake2b(digest_size=10)
    h.update(s.encode('utf-8'))
    return h.hexdigest()


def current_ts_isof():
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

import logging
logger = logging.getLogger("cookbook-api")

from elasticsearch import Elasticsearch
es_client = Elasticsearch(
    'localhost:9200',
    timeout=10
)

class BaseDocumentModel:

    def __init__(
        self,
        index,
        id=None,
        **kwargs
    ):
        self.current_ts = current_ts_isof()
        self.index = index
        self.kwrags = kwargs

        if id:
            self.id = id
            self.source = es_client.search(
                index=index,
                body={
                    "query": {
                        "ids": {
                            "values": [id]
                        }
                    }
                }
            )['hits']['hits'][0]['_source']

        else:
            self.doc = kwargs['doc']
            self.doc.update({
                'created_ts': self.current_ts,
                'modified_ts': self.current_ts
            })
            if index == 'codes':
                self.id = create_hash_id(''.join([
                    kwargs['doc']['column_name'],
                    kwargs['doc']['code']
                ]))
            elif index == 'comments':
                self.id = create_hash_id(''.join([
                    kwargs['doc']['author'],
                    self.current_ts
                ]))


    def show(self, **kwargs):
        return self.source

    def update(self, **kwargs):
        es_client.update(
            index=self.index,
            id=self.id,
            body={'doc': kwargs['kwargs']}
        )

    def delete(self, **kwargs):
        es_client.delete(
            index=self.index,
            id=self.id
        )

    def refresh(self):
        es_client.indices.refresh(
            index=self.index
        )

    def create(self):
        es_client.index(
            index=self.index,
            id=self.id,
            doc_type='_doc',
            body=self.doc,
        )
        self.refresh()


class BaseSearchResultModel:

    def __init__(
        self,
        query,
        index,
        size=4,
        offset=0
    ):
        self.index = index
        self.body = {
            "size": size,
            "from": offset,
            "query": {
                "multi_match": {
                    "query": query,
                    "fields": {
                        "tables": ['table_name', 'entity_name', 'description'],
                        "columns": ['column_name', 'attribute_name', 'description'],
                        "codes": ['code', 'description'],
                        "comments": ['comment'],
                        "autocomplete_keywords": ['keyword']
                    }[index]
                }
            }
        }

    def get_result(self, **kwargs):
        return es_client.search(
            index=self.index,
            body=self.body
        )['hits']


class MatchAllSearchResultModel(BaseSearchResultModel):

    def __init__(
        self,
        index,
        size=10,
        offset=0
    ):
        self.index= index
        self.body = {
            "size": size,
            "from": offset,
            "query": {
                "match_all" : {}
            },
            "sort": [
                {"created_ts": {"order": "desc"}}
            ]
        }


class WildcardSearchResultModel(BaseSearchResultModel):

    def __init__(
        self,
        query,
        index,
        size=10,
        offset=0
    ):
        self.index= index
        self.body = {
            "size": size,
            "from": offset,
            "query": {
                "wildcard" :
                    { "keyword.keyword" : { "value" : "".join(['*',query,'*']) }
                }
            }
        }


class SearchByParentIdResultModel(BaseSearchResultModel):

    def __init__(
        self,
        query,
        index,
        size=1000,
        offset=0
    ):
        self.index= index
        self.body = {
            "size": size,
            "from": offset,
            "query": {
                "term": { "parent_id": { "value": query } }
            },
            "sort": [{{
                "columns": "position",
                "codes": "code.keyword",
                "comments": "created_ts",
                "tables": "created_ts",
            }[index]: {"order": "asc"}}]
        }


class SearchByColumnNameResultModel(BaseSearchResultModel):

    def __init__(
        self,
        query,
        index,
        size=1000,
        offset=0
    ):
        self.index= index
        self.body = {
            "size": size,
            "from": offset,
            "query": {
                "term": { "column_name.keyword": { "value": query } }
            },
            "sort": [{{
                "columns": "position",
                "codes": "code.keyword",
                "comments": "created_ts",
                "tables": "created_ts",
            }[index]: {"order": "asc"}}]
        }


class SearchByAuthorResultModel(BaseSearchResultModel):

    def __init__(
        self,
        query,
        index,
        size=10,
        offset=0
    ):
        self.index= index
        self.body = {
            "size": size,
            "from": offset,
            "query": {
                "term": { "author": { "value": query } }
            },
            "sort": [{"created_ts" : {"order": "desc"}}]
        }


class ExactSearchResultForTableModel(BaseSearchResultModel):
    pass

class BaseUserModel:
    pass
