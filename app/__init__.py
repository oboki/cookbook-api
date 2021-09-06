from .model import (
    BaseDocumentModel, BaseSearchResultModel, MatchAllSearchResultModel,
    SearchByAuthorResultModel, SearchByColumnNameResultModel,
    SearchByParentIdResultModel,WildcardSearchResultModel
)

from flask import (
    jsonify, request,
    session as flask_session, g,
)
from flask_appbuilder import expose, has_access, permission_name
from flask_appbuilder import BaseView as AppBuilderBaseView
from flask_appbuilder.security.decorators import has_access_api

from airflow.utils.db import provide_session
from airflow.www_rbac import utils as wwwutils
from airflow.www_rbac.app import csrf
from airflow.plugins_manager import AirflowPlugin

from urllib.parse import unquote
from copy import deepcopy
import json
import logging
logger = logging.getLogger("cookbook-api")


class CookbookApi(AppBuilderBaseView):

    default_view = 'index'

    @provide_session
    @expose('/')
    def index(self, session=None):
        return ''

    # @has_access_api
    # @permission_name("list")
    @provide_session
    @expose('/v1/<index>')
    def match_all(self, index, session=None):
        search = MatchAllSearchResultModel(index)
        return wwwutils.json_response(search.get_result())


    # @has_access_api
    # @permission_name("list")
    @provide_session
    @csrf.exempt
    @expose('/v1/<index>/search', methods=['GET', 'POST'])
    def search(self, index, session=None):
        size = int(unquote(request.args.get('size', '4')))
        page = int(unquote(request.args.get('page', '0')))

        payload = None
        if request.method == 'POST':
            payload = deepcopy(request.get_json()['data'])

        query = unquote(request.args.get('s', ''))
        if not query:
            search = MatchAllSearchResultModel(
                index,
                size=size,
                offset=page*size,
                advanced=payload
            )
            return wwwutils.json_response(search.get_result())

        wildcard = unquote(request.args.get('wildcard', ''))
        if wildcard:
            search = WildcardSearchResultModel(query, index)
            return wwwutils.json_response(search.get_result())

        by_parent_id = unquote(request.args.get('by-parent-id', ''))
        if by_parent_id:
            search = SearchByParentIdResultModel(
                query, # parent_id
                index
            )
            return wwwutils.json_response(search.get_result())

        by_column_name = unquote(request.args.get('by-column-name', ''))
        if by_column_name:
            search = SearchByColumnNameResultModel(
                query, # column_name
                index
            )
            return wwwutils.json_response(search.get_result())

        by_author = unquote(request.args.get('by-author', ''))
        if by_author:
            search = SearchByAuthorResultModel(
                query,
                index,
                size=size,
                offset=page*size
            )
            return wwwutils.json_response(search.get_result())

        search = BaseSearchResultModel(
            query,
            index,
            size=size,
            offset=page*size,
            advanced=payload
        )
        return wwwutils.json_response(search.get_result())

    #@has_access
    #@permission_name("list")

    @provide_session
    @expose('/v1/<index>/<id>')
    def detail(self, index, id, session=None):
        detail = BaseDocumentModel(index, id)
        return wwwutils.json_response(detail.show())

    #@has_access
    #@permission_name("list")

    @provide_session
    @expose('/v1/whoami')
    def get_current_user(self, session=None):
        if g and hasattr(g, 'user') and g.user:
            try:
                username = g.user.username
            except AttributeError:
                username = 'Anonymous'

        return wwwutils.json_response({'username': username})


    #@has_access
    #@permission_name("edit")

    @provide_session
    @csrf.exempt
    @expose('/v1/<index>/add', methods=['POST'])
    def create(self, index, session=None):
        req = request.get_json()

        doc = BaseDocumentModel(index, doc=req['data'])
        doc.create()

        return jsonify({
            'created_doc_id': doc.id
        })


    #@has_access
    #@permission_name("edit")

    @provide_session
    @csrf.exempt
    @expose('/v1/<index>/edit/<id>', methods=['POST'])
    def edit(self, index, id, session=None):
        doc = request.get_json()['data']

        document = BaseDocumentModel(index, id=id)
        document.update(kwargs=doc)

        return jsonify({
           'status': 'success'
        })


    #@has_access
    #@permission_name("edit")

    @provide_session
    @csrf.exempt
    @expose('/v1/<index>/delete/<id>', methods=['POST'])
    def delete(self, index, id, session=None):
        document = BaseDocumentModel(index, id=id)
        document.delete()

        return jsonify({
           'status': 'success'
        })


v_appbuilder_view = CookbookApi()
v_appbuilder_package = {
    "name": "CookbookApi",
    "category": "Analytics",
    "icon": "fa-th",
    "view": v_appbuilder_view}


class CookbookPlugin(AirflowPlugin):
    name = "analytics"
    appbuilder_views = [v_appbuilder_package]
