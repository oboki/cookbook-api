import logging
logger = logging.getLogger("cookbook-api")

from elasticsearch import Elasticsearch
es_client = Elasticsearch(
    'localhost:9200',
    timeout=10
)

class BaseDetailModel:

    def __init__(
        self,
        index,
        id=None
    ):
        self.id = id
        self.index = index
        self.source = es_client.search(
            index=index,
            body={
                "query": {
                    "ids": {
                        "values": [self.id]
                    }
                }
            }
        )['hits']['hits'][0]['_source']

    def show(self, **kwargs):
        return self.source

    def update(self, **doc):
        es_client.update(
            index=self.index,
            id=self.id,
            body=doc
        )

    def refresh(self):
        es_client.indices.refresh(
            index=self.index
        )

    def create(self, **kwargs):
        pass


class BaseSearchResultModel:

    def __init__(
        self,
        query,
        index,
        size=4,
        offset=0
    ):
        body = {
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
            # ,
            # "sort": [
            #     {"_score": {"order": "desc"}}
            # ]
        }

        self.result = es_client.search(
            index=index,
            body=body
        )['hits']

    def get_result(self, **kwargs):
        return self.result


class WildcardSearchResultModel(BaseSearchResultModel):

    def __init__(
        self,
        query,
        index,
        size=10,
        offset=0
    ):
        body = {
            "size": size,
            "from": offset,
            "query": {
                "wildcard" :
                    { "keyword.keyword" : { "value" : "".join(['*',query,'*']) }
                }
            }
        }

        logger.info(body)

        self.result = es_client.search(
            index=index,
            body=body
        )['hits']


class SearchByParentIdResultModel(BaseSearchResultModel):

    def __init__(
        self,
        query,
        index,
        size=1000,
        offset=0
    ):
        body = {
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

        self.result = es_client.search(
            index=index,
            body=body
        )['hits']


class SearchByColumnNameResultModel(BaseSearchResultModel):

    def __init__(
        self,
        query,
        index,
        size=1000,
        offset=0
    ):
        body = {
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

        self.result = es_client.search(
            index=index,
            body=body
        )['hits']


class ExactSearchResultForTableModel(BaseSearchResultModel):
    pass

class BaseUserModel:
    pass
