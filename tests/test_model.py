import pytest

from app.model import BaseDetailModel, BaseSearchResultModel

import json
import random
import string


def test_show_column_detail_model():
    id = '6e2ddf1c905aef37b8a6'
    column = BaseDetailModel('columns', id)

    assert column.show()['column_name'] == 'COL_A'


def test_update_description():
    random_string = ''.join(
        [random.choice(string.ascii_lowercase) for _ in range(10)])

    id = '0dc1edd8f731a8acf784'
    table = BaseDetailModel('tables', id)

    table.update(doc={'description': random_string})
    table.refresh()

    table = BaseDetailModel('tables', id)

    assert table.show()['description'] == random_string
