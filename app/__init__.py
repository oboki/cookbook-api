from flask_appbuilder.security.decorators import has_access_api
from .model import BaseDetailModel, BaseSearchResultModel, SearchByColumnNameResultModel, SearchByParentIdResultModel, WildcardSearchResultModel
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
        detail = BaseDetailModel(index, id)
        return wwwutils.json_response(detail.show())

    #@has_access
    #@permission_name("edit")

    @provide_session
    @csrf.exempt
    @expose('/v1/<index>/add', methods=['POST'])
    def create(self, index, session=None):
        import json
        req = request.get_json()
        code = BaseDetailModel(
            index, doc={
                'codes': {
                    'column_name': req['data']['column_name'],
                    'code_name': req['data']['code_name'],
                    'description': req['data']['description'],
                }
            }[index]
        )
        code.create()
        return jsonify(0)


v_appbuilder_view = CookbookApi()
v_appbuilder_package = {
    "name": "CookbookApi",
    "category": "Analytics",
    "icon": "fa-th",
    "view": v_appbuilder_view}


class CookbookPlugin(AirflowPlugin):
    name = "analytics"
    appbuilder_views = [v_appbuilder_package]
