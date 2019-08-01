import os
import unittest

import mock
import pytz
import simplejson

from tap_jira.context import Context
from unittest.mock import Mock, MagicMock
from tap_jira.streams import Issues, Projects, Users, ProjectTypes, Stream
from tap_jira.httpJira import Paginator, Client
from datetime import datetime

class TestLocalizedRequests(unittest.TestCase):

    def load_file(self,filename):
        myDir = os.path.dirname(os.path.abspath(__file__))
        path = os.path.join(myDir, "data_test", filename)
        with open(path) as file:
            return simplejson.load(file)

    def setUp(self):
        self.tzname = 'Europe/Volgograd'
        Context.update_start_date_bookmark = Mock(return_value=datetime(2018,12,12,1,2,3, tzinfo=pytz.UTC))
        Context.retrieve_timezone = Mock(return_value=self.tzname)
        Context.bookmark = Mock()
        Context.set_bookmark = Mock()
        Context.client = Client(self.load_file("config.json"))
        Paginator.pages = Mock(return_value=[])
        Context.is_selected = Mock(return_value= False)
        Stream.write_page = Mock()

    def test_issues_local_timezone_in_request(self):
        issues = Issues('issues', ['pk_fields'])
        issues.sync()

        user_tz = pytz.timezone(self.tzname)
        expected_start_date = (datetime(2018, 12, 12, 1, 2, tzinfo=pytz.UTC)
                               .astimezone(user_tz)
                               .strftime("%Y-%m-%d %H:%M"))
        params = {"fields": "*all",
                  "expand": "changelog,transitions",
                  "validateQuery": "strict",
                  "jql": "updated >= '{}' order by updated asc".format(expected_start_date)}
        Paginator.pages.assert_called_once_with('issues','GET','/rest/api/2/search',params=params)

    '''
    def test_projects(self):
        projects = Projects('projects', ['pk_fields'])
        projects.sync()'''


    def test_users(self):
        users = Users('users', ['pk_fields'])
        params = {"username": "%",
                  "includeInactive": "true",
                  "maxResults": 2}
        users.sync()
        Paginator.pages.assert_called_once_with('users', 'GET', '/rest/api/2/user/search', params=params)

    def test_project_types(self):
        project_type = ProjectTypes('project_types', ['pk_fields'])
        with mock.patch('tap_jira.Client.request', return_value=[]) as patch:
            project_type.sync()
            patch.assert_called_once_with('project_types', 'GET', '/rest/api/2/project/type')

    def test_projects(self):
        project = Projects('projects', ['pk_fields'])
        with mock.patch('tap_jira.Client.request', return_value=[]) as patch:
            project.sync()
            params = {"expand": "description,lead,url,projectKeys"}
            patch.assert_called_with('projects', 'GET', '/rest/api/2/project', params=params)


if __name__ == '__main__':
    unittest.main()






