from .model import BaseDetailModel, BaseSearchResultModel, ExactSearchResultForTableModel
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
from airflow.www_rbac.app import app, appbuilder
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

    #@has_access
    #@permission_name("list")

    @provide_session
    @expose('/v1/<index>/search')
    def search(self, index, session=None):
        size = int(unquote(request.args.get('size', '4')))
        page = int(unquote(request.args.get('page', '0')))

        query = unquote(request.args.get('s', ''))
        if not query:
            return wwwutils.json_response([])

        exact = unquote(request.args.get('exact', ''))
        if exact:
            search = ExactSearchResultForTableModel(
                query.split(".")[0],  # db_name
                query.split(".")[1],  # table_name
                index,
                size=size, offset=page*size

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


v_appbuilder_view = CookbookApi()
v_appbuilder_package = {
    "name": "CookbookApi",
    "category": "Analytics",
    "icon": "fa-th",
    "view": v_appbuilder_view}


class CookbookPlugin(AirflowPlugin):
    name = "analytics"
    appbuilder_views = [v_appbuilder_package]
