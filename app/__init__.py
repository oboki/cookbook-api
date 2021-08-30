from flask_appbuilder.security.decorators import has_access_api
from .model import BaseDocumentModel, BaseSearchResultModel, SearchByColumnNameResultModel, SearchByParentIdResultModel, WildcardSearchResultModel
import logging
import json
from urllib.parse import unquote

from flask import (
    Markup, Response, escape, flash, jsonify, make_response, redirect, render_template, request,
    session as flask_session, url_for, g,
)
from flask_appbuilder import BaseView, ModelView, expose, has_access, permission_name
from flask_appbuilder import BaseView as AppBuilderBaseView

import airflow
from airflow.exceptions import AirflowException
from airflow.utils.db import provide_session
from airflow.www_rbac import utils as wwwutils
from airflow.www_rbac.app import app, appbuilder, csrf
from airflow.www_rbac.decorators import action_logging, gzipped, has_dag_access
from airflow.plugins_manager import AirflowPlugin

from pathlib import Path

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
    @expose('/v1/<index>/search')
    def search(self, index, session=None):
        size = int(unquote(request.args.get('size', '4')))
        page = int(unquote(request.args.get('page', '0')))

        query = unquote(request.args.get('s', ''))
        if not query:
            return wwwutils.json_response([])

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

        search = BaseSearchResultModel(
            query,
            index,
            size=size,
            offset=page*size
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
    @expose('/v1/user')
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
        import json
        req = request.get_json()
        parent = BaseDocumentModel('tables', req['data']['parent_id'])

        if index == 'codes':
            doc = {
                'column_name': req['data']['column_name'],
                'code': req['data']['code'],
                'description': req['data']['description'],
                'parent_id': req['data']['parent_id'],
            }
        elif index == 'comments':
            doc = {
                'author': req['data']['author'],
                'comment': req['data']['comment'],
                'parent_id': req['data']['parent_id'],
                'db_name': parent.db_name,
                'table_name': parent.table_name
            }

        code = BaseDocumentModel(index, doc=doc)
        code.create()

        return jsonify({
            'created_doc_id': code.id
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


v_appbuilder_view = CookbookApi()
v_appbuilder_package = {
    "name": "CookbookApi",
    "category": "Analytics",
    "icon": "fa-th",
    "view": v_appbuilder_view}


class CookbookPlugin(AirflowPlugin):
    name = "analytics"
    appbuilder_views = [v_appbuilder_package]
