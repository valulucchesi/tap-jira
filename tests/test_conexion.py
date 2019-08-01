import os
import unittest

import mock
import simplejson
from more_itertools import side_effect
from singer import utils, catalog, Schema
from singer.catalog import CatalogEntry, Catalog

from tap_jira import load_schema, streams, discover, generate_metadata, Context, output_schema, sync
from tap_jira.streams import Stream, Projects, validate_dependencies, DependencyException, Issues
from unittest.mock import Mock


class TestTapJira(unittest.TestCase):

    def load_stream(self, path):
        with open(path) as file:
            for line in file:
                return eval(line)


    def get_abs_path(self, path):
        myDir = os.path.dirname(os.path.abspath(__file__))
        return os.path.join(myDir, "data_test", path)

    def test_load_schemas(self):
        schema = utils.load_json(self.get_abs_path("schema_spec.json"))
        resp = load_schema("projects")
        self.assertEqual(resp, schema)

    def test_discover(self):
        stream = Projects("projects", ["id"])
        record = []
        record.append(stream)
        catalog = Catalog([])
        mdata = [{'breadcrumb': (), 'metadata': {'table-key-properties': ['id']}}, {'breadcrumb': ('properties', 'simplified'), 'metadata': {'inclusion': 'available'}}, {'breadcrumb': ('properties', 'expand'), 'metadata': {'inclusion': 'available'}}, {'breadcrumb': ('properties', 'self'), 'metadata': {'inclusion': 'available'}}, {'breadcrumb': ('properties', 'fields_json'), 'metadata': {'inclusion': 'available'}}, {'breadcrumb': ('properties', 'id'), 'metadata': {'inclusion': 'automatic'}}, {'breadcrumb': ('properties', 'key'), 'metadata': {'inclusion': 'available'}}, {'breadcrumb': ('properties', 'description'), 'metadata': {'inclusion': 'available'}}, {'breadcrumb': ('properties', 'lead'), 'metadata': {'inclusion': 'available'}}, {'breadcrumb': ('properties', 'components'), 'metadata': {'inclusion': 'available'}}, {'breadcrumb': ('properties', 'issueTypes'), 'metadata': {'inclusion': 'available'}}, {'breadcrumb': ('properties', 'url'), 'metadata': {'inclusion': 'available'}}, {'breadcrumb': ('properties', 'email'), 'metadata': {'inclusion': 'available'}}, {'breadcrumb': ('properties', 'assigneeType'), 'metadata': {'inclusion': 'available'}}, {'breadcrumb': ('properties', 'name'), 'metadata': {'inclusion': 'available'}}, {'breadcrumb': ('properties', 'roles'), 'metadata': {'inclusion': 'available'}}, {'breadcrumb': ('properties', 'avatarUrls'), 'metadata': {'inclusion': 'available'}}, {'breadcrumb': ('properties', 'projectKeys'), 'metadata': {'inclusion': 'available'}}, {'breadcrumb': ('properties', 'projectCategory'), 'metadata': {'inclusion': 'available'}}, {'breadcrumb': ('properties', 'projectTypeKey'), 'metadata': {'inclusion': 'available'}}]
        param = CatalogEntry(
            stream="projects",
            tap_stream_id="projects",
            key_properties=["id"],
            schema= Schema.from_dict(utils.load_json(self.get_abs_path("schema_discover.json"))),
            metadata=mdata)
        catalog.streams.append(param)
        with mock.patch('tap_jira.streams.ALL_STREAMS', record):
            resp = discover()
            self.assertEqual(resp, catalog)

    def test_generate_metadata(self):
        mdata = self.load_stream(self.get_abs_path("mdata.stream"))
        stream = Projects("projects", ["id"])
        schema = Schema.from_dict(utils.load_json(self.get_abs_path("schema_spec.json")))
        resp = generate_metadata(stream, schema)
        self.assertEqual(resp, mdata)

    def side_effect(self, value):
        if value == 'versions':
            return True
        return False


    def test_validate_dependencies(self):
        Context.is_selected = Mock(return_value=True)
        Context.catalog = Catalog([])
        try:
            validate_dependencies()
        except:
            self.fail("invalid raise exception")
        Context.catalog.streams.append(Stream("versions", ["id"], indirect_stream=True))
        self.assertRaises(DependencyException, validate_dependencies)
        Context.catalog = Catalog([Stream("versions", ["id"], indirect_stream=True), Projects("projects", ["id"])])
        try:
            validate_dependencies()
        except:
            self.fail("invalid raise exception")
        Context.catalog = Catalog([Stream("changelogs", ["id"], indirect_stream=True), Stream("issue_comments", ["id"], indirect_stream=True), Stream("issue_transitions", ["id"], indirect_stream=True)])
        self.assertRaises(DependencyException, validate_dependencies)
        Context.catalog.streams.append(Issues("issues", ["id"]))
        try:
            validate_dependencies()
        except:
            self.fail("invalid raise exception")


    def test_output_schema(self):
        schema = utils.load_json(self.get_abs_path("schema_spec.json"))
        stream_id = "projects"
        stream_pk = ["id"]
        with mock.patch('singer.write_schema') as patch:
            output_schema(Projects('projects', ["id"]))
            patch.assert_called_once_with(stream_id, schema, stream_pk)

    def test_sync(self):
        streams.validate_dependencies = Mock()
        streams.ALL_STREAMS = []
        Context.state = {}
        with mock.patch('singer.write_state') as patch:
            sync()
            patch.assert_called_once_with({"currently_syncing": None})
            patch.reset_mock()
            streams.ALL_STREAMS = [Projects('projects', ["id"])]
            with mock.patch('tap_jira.streams.Projects.sync'):
                Context.is_selected = Mock(return_value=True)
                sync()
                count = patch.call_count
                self.assertEqual(count, 2)
