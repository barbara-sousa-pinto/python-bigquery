import io
import mock
import unittest
from google.api_core.exceptions import Unauthorized, AlreadyExists
from bigquery.tools import BigQuery
from datetime import datetime, date
from google.cloud.bigquery import SchemaField
from google.cloud.exceptions import BadRequest
import textwrap


def _make_credentials():
    from google.oauth2.service_account import Credentials

    return mock.Mock(spec=Credentials)


def _make_bigquery_client(*args, **kw):
    from google.cloud.bigquery.client import Client

    return Client(*args, **kw)


def _make_connection(*responses):
    from google.cloud.bigquery import _http
    from google.cloud.exceptions import NotFound

    mock_conn = mock.create_autospec(_http.Connection)
    mock_conn.user_agent = "testing 1.2.3"
    mock_conn.api_request.side_effect = list(responses) + [NotFound("miss")]
    return mock_conn

class TestGoogleCloudBigQuery(unittest.TestCase):
    PROJECT = "PROJECT"
    DS_ID = "DATASET_ID"
    TABLE_ID = "TABLE_ID"
    JOB_ID = "job-id"

    def setUp(self):
        self.http = object()
        self.credentials = _make_credentials()
        self.dataset_resource = {
            "id": "%s:%s" % (self.PROJECT, self.DS_ID),
            "datasetReference": {
                "projectId": self.PROJECT, "datasetId": self.DS_ID
            },
        }
        self.client = _make_bigquery_client(project=self.PROJECT,
                                            credentials=self.credentials,
                                            _http=self.http)

    def _make_table_resource(self):
        return {
            "id": "%s:%s:%s" % (self.PROJECT, self.DS_ID, self.TABLE_ID),
            "tableReference": {
                "projectId": self.PROJECT,
                "datasetId": self.DS_ID,
                "tableId": self.TABLE_ID,
            },
        }

    # def test_DefaultCredentials(self):
    #     credentials_patch = mock.patch("google.auth.default",
    #                                    return_value=(self.credentials,
    #                                                  self.PROJECT))
    #     with self.assertRaises(Unauthorized):
    #         with credentials_patch:
    #             BigQuery(self.DS_ID)

    def test_if_existing_dataset_passed_is_gotten(self):
        path = "projects/%s/datasets/%s" % (self.PROJECT, self.DS_ID)

        conn = self.client._connection = _make_connection(self.dataset_resource)
        mock_client = mock.patch("google.cloud.bigquery.Client",
                                 return_value=self.client)

        with mock_client:
            bq = BigQuery(self.DS_ID)

        self.assertEqual(bq.dataset.dataset_id, self.DS_ID)
        conn.api_request.assert_called_once_with(method="GET", path="/%s" % path, timeout=None)

    def test_if_no_existing_dataset_passed_is_created(self):
        from google.cloud.exceptions import NotFound

        path = "projects/%s/datasets" % self.PROJECT

        conn = self.client._connection = _make_connection(NotFound("dataset not found"),
                                                          self.dataset_resource)
        mock_client = mock.patch("google.cloud.bigquery.Client",
                                 return_value=self.client)

        with mock_client:
            bq = BigQuery(self.DS_ID)

        created_dataset = bq.dataset

        self.assertEqual(created_dataset.dataset_id, self.DS_ID)
        self.assertEqual(created_dataset.project, self.PROJECT)
        self.assertEqual(created_dataset.full_dataset_id,
                         self.dataset_resource["id"])

        conn.api_request.assert_called_with(
            method="POST",
            path="/%s" % path,
            data={
                "datasetReference": {
                    "projectId": self.PROJECT,
                    "datasetId": self.DS_ID,
                },
                "labels": {},
            },
            timeout=None
        )

    def test_get_table_records(self):

        query_get_records = f'SELECT * FROM {self.DS_ID}.{self.TABLE_ID}'

        job_resource = {
            'jobReference': {
                'projectId': self.PROJECT,
                'jobId': self.JOB_ID
            },
            'configuration': {
                'query': {
                    'destinationTable': {
                        'projectId': self.PROJECT,
                        'datasetId': self.DS_ID,
                        'tableId': self.TABLE_ID
                    },
                    'query': query_get_records
                }
            },
            'status': {'state': 'DONE'}
        }

        schema_fields = [
            {'name': 'id', 'type': 'INTEGER'},
            {'name': 'name', 'type': 'STRING'},
            {'name': 'email', 'type': 'STRING'},
            {'name': 'inserted_at', 'type': 'DATETIME'},
            {'name': 'settings', 'type': 'STRING'}
        ]

        query_resource = {
            "jobReference": {"projectId": self.PROJECT, "jobId": self.JOB_ID},
            "schema": {"fields": schema_fields},
            "totalRows": "2",
        }

        def _row_data(row):
            r = []
            for key, value in row.items():
                if value is not None:
                    if isinstance(value, datetime):
                        r += [{"v": str(value).replace(' ', 'T') + '.000000'}]
                    else:
                        r += [{"v": str(value)}]
            return r

        pg_records = [
            {'id': 1,
             'name': 'Barbara Sousa',
             'email': 'barbara.sousa@email.com.br',
             'inserted_at': datetime(2019, 3, 20, 16, 33, 59),
             'settings': '{"onboarding": true}'},
            {'id': 2,
             'name': 'Fake Name',
             'email': 'fake.name@email.com.br',
             'inserted_at': datetime(2019, 3, 25, 13, 27, 48),
             'settings': '{"onboarding": true}'}
        ]

        bq_rows = [
            {"f": _row_data(row)}
            for i, row in enumerate(pg_records)
        ]

        tabledata_resource = {
            "rows": bq_rows,
        }

        self.client._connection = _make_connection(self.dataset_resource,
                                                   job_resource,
                                                   query_resource,
                                                   tabledata_resource)

        mock_client = mock.patch("google.cloud.bigquery.Client",
                                 return_value=self.client)

        mock_output = mock.patch('sys.stdout', new_callable=io.StringIO)

        with mock_client:
            with mock_output as fake_output:
                bq = BigQuery(self.DS_ID)
                table_records = bq.get_table_records(self.TABLE_ID)
                output = fake_output.getvalue().strip().replace('  ', '')

        assert textwrap.fill(output) == query_get_records.strip()

        self.assertIsInstance(table_records, list)
        self.assertEqual(len(table_records), 2)

        rows = list(table_records)
        self.assertEqual(rows[0]['id'], 1)
        self.assertEqual(rows[0]['name'], 'Barbara Sousa')
        self.assertEqual(rows[0]['email'], 'barbara.sousa@email.com.br')
        self.assertEqual(rows[0]['inserted_at'], datetime(2019, 3, 20, 16, 33, 59))
        self.assertEqual(rows[0]['settings'], '{"onboarding": true}')

    def test_if_get_table_records_return_none_if_table_not_found(self):

        self.client._connection = _make_connection(self.dataset_resource)

        mock_client = mock.patch("google.cloud.bigquery.Client",
                                 return_value=self.client)

        with mock_client:
            bq = BigQuery(self.DS_ID)
            table_records = bq.get_table_records(self.TABLE_ID)

        self.assertIsNone(table_records)

    def test_create_partitioned_table(self):
        from google.cloud.bigquery import SchemaField

        path = "projects/%s/datasets/%s/tables" % (self.PROJECT, self.DS_ID)

        schema_fields = [
            {'mode': 'NULLABLE',
             'name': 'id',
             'type': 'INTEGER',
             'description': None},
            {'mode': 'NULLABLE',
             'name': 'name',
             'type': 'STRING',
             'description': None},
            {'mode': 'NULLABLE',
             'name': 'loaded_at',
             'type': 'DATETIME'},
            {'mode': 'NULLABLE',
             'name': 'to_remove',
             'type': 'BOOLEAN'}
        ]

        table_resource = {
            'tableReference': {
                'projectId': self.PROJECT,
                'datasetId': self.DS_ID,
                'tableId': self.TABLE_ID
            },
            'labels': {},
            'schema': {
                'fields': schema_fields
            },
            'timePartitioning': {
                "type": "DAY"}
        }

        bq_schema = [SchemaField('id', 'INTEGER', 'NULLABLE', None, ()),
                     SchemaField('name', 'STRING', 'NULLABLE', None, ())]

        conn = self.client._connection = _make_connection(self.dataset_resource,
                                                          table_resource)
        mock_client = mock.patch("google.cloud.bigquery.Client",
                                 return_value=self.client)

        mock_output = mock.patch('sys.stdout', new_callable=io.StringIO)
        with mock_client:
            with mock_output as fake_output:
                bq = BigQuery(self.DS_ID)
                table = bq.create_partitioned_table(self.TABLE_ID, bq_schema)
                output = fake_output.getvalue().strip()

        assert output == f"Created table {self.PROJECT}:{self.DS_ID}.{self.TABLE_ID}"

        self.assertEqual(table.table_id, self.TABLE_ID)
        self.assertEqual(table.time_partitioning.type_, "DAY")
        self.assertEqual(table.time_partitioning.field, "_PARTITIONTIME")

        conn.api_request.assert_called_with(
            method="POST",
            path="/%s" % path,
            data=table_resource,
            timeout=None
        )

    def test_create_partitioned_table(self):
        from google.cloud.bigquery import SchemaField

        path = "projects/%s/datasets/%s/tables" % (self.PROJECT, self.DS_ID)

        schema_fields = [
            {'mode': 'NULLABLE',
             'name': 'id',
             'type': 'INTEGER',
             'description': None},
            {'mode': 'NULLABLE',
             'name': 'name',
             'type': 'STRING',
             'description': None},
            {'mode': 'NULLABLE',
             'name': 'loaded_at',
             'type': 'DATETIME'},
            {'mode': 'NULLABLE',
             'name': 'to_remove',
             'type': 'BOOLEAN'}
        ]

        table_resource = {
            'tableReference': {
                'projectId': self.PROJECT,
                'datasetId': self.DS_ID,
                'tableId': self.TABLE_ID
            },
            'labels': {},
            'schema': {
                'fields': schema_fields
            },
            'timePartitioning': {"type": "DAY"}
        }

        bq_schema = [SchemaField('id', 'INTEGER', 'NULLABLE', None, ()),
                     SchemaField('name', 'STRING', 'NULLABLE', None, ())]

        conn = self.client._connection = _make_connection(self.dataset_resource,
                                                          table_resource)
        mock_client = mock.patch("google.cloud.bigquery.Client",
                                 return_value=self.client)

        mock_output = mock.patch('sys.stdout', new_callable=io.StringIO)
        with mock_client:
            with mock_output as fake_output:
                bq = BigQuery(self.DS_ID)
                table = bq.create_partitioned_table(self.TABLE_ID, bq_schema)
                output = fake_output.getvalue().strip()

        assert output == f"Created table {self.PROJECT}:{self.DS_ID}.{self.TABLE_ID}"

        self.assertEqual(table.table_id, self.TABLE_ID)
        self.assertEqual(table.time_partitioning.type_, "DAY")

        conn.api_request.assert_called_with(
            method="POST",
            path="/%s" % path,
            data=table_resource,
            timeout=None
        )

    def test_create_partitioned_table_with_partition_by(self):
        from google.cloud.bigquery import SchemaField

        path = "projects/%s/datasets/%s/tables" % (self.PROJECT, self.DS_ID)

        schema_fields = [
            {'mode': 'NULLABLE',
             'name': 'id',
             'type': 'INTEGER',
             'description': None},
            {'mode': 'NULLABLE',
             'name': 'name',
             'type': 'STRING',
             'description': None},
            {'mode': 'NULLABLE',
             'name': 'loaded_at',
             'type': 'DATETIME'},
            {'mode': 'NULLABLE',
             'name': 'to_remove',
             'type': 'BOOLEAN'}
        ]

        table_resource = {
            'tableReference': {
                'projectId': self.PROJECT,
                'datasetId': self.DS_ID,
                'tableId': self.TABLE_ID
            },
            'labels': {},
            'schema': {
                'fields': schema_fields
            },
            'timePartitioning': {
                "type": "DAY",
                "field": "loaded_at"
            }
        }

        bq_schema = [SchemaField('id', 'INTEGER', 'NULLABLE', None, ()),
                     SchemaField('name', 'STRING', 'NULLABLE', None, ())
                     ]

        conn = self.client._connection = _make_connection(self.dataset_resource,
                                                          AlreadyExists("table already exists"),
                                                          table_resource)
        mock_client = mock.patch("google.cloud.bigquery.Client",
                                 return_value=self.client)

        mock_output = mock.patch('sys.stdout', new_callable=io.StringIO)
        with mock_client:
            with mock_output as fake_output:
                bq = BigQuery(self.DS_ID)
                table = bq.create_partitioned_table(
                    self.TABLE_ID, bq_schema, partition_by='loaded_at'
                )
                output = fake_output.getvalue().strip()

        assert output == f"Table {self.PROJECT}:{self.DS_ID}.{self.TABLE_ID} already exists."

        self.assertEqual(table.table_id, self.TABLE_ID)
        self.assertEqual(table.time_partitioning.type_, "DAY")
        self.assertEqual(table.time_partitioning.field, "loaded_at")

        conn.api_request.assert_called_with(
            method="POST",
            path="/%s" % path,
            data=table_resource,
            timeout=None
        )

    def test_create_partitioned_table_with_conflit_exist_ok_true(self):
        from google.cloud.bigquery import SchemaField

        path = "projects/%s/datasets/%s/tables" % (self.PROJECT, self.DS_ID)

        schema_fields = [
            {'mode': 'NULLABLE',
             'name': 'id',
             'type': 'INTEGER',
             'description': None},
            {'mode': 'NULLABLE',
             'name': 'name',
             'type': 'STRING',
             'description': None},
            {'mode': 'NULLABLE',
             'name': 'loaded_at',
             'type': 'DATETIME'},
            {'mode': 'NULLABLE',
             'name': 'to_remove',
             'type': 'BOOLEAN'}
        ]

        table_resource = {
            'tableReference': {
                'projectId': self.PROJECT,
                'datasetId': self.DS_ID,
                'tableId': self.TABLE_ID
            },
            'labels': {},
            'schema': {
                'fields': schema_fields
            },
            'timePartitioning': {"type": "DAY"}
        }

        bq_schema = [SchemaField('id', 'INTEGER', 'NULLABLE', None, ()),
                     SchemaField('name', 'STRING', 'NULLABLE', None, ())]

        conn = self.client._connection = _make_connection(
            self.dataset_resource,
            AlreadyExists("table already exists")
        )
        mock_client = mock.patch("google.cloud.bigquery.Client",
                                 return_value=self.client)

        mock_output = mock.patch('sys.stdout', new_callable=io.StringIO)
        with mock_client:
            with mock_output as fake_output:
                bq = BigQuery(self.DS_ID)
                table = bq.create_partitioned_table(self.TABLE_ID, bq_schema)
                output = fake_output.getvalue().strip()

        assert output == f"Table {self.PROJECT}:{self.DS_ID}.{self.TABLE_ID} already exists."

        self.assertEqual(table.table_id, self.TABLE_ID)
        self.assertEqual(table.time_partitioning.type_, "DAY")

        conn.api_request.assert_called_with(
            method="POST",
            path="/%s" % path,
            data=table_resource,
            timeout=None
        )

    def test_create_partitioned_table_with_conflit_exist_ok_false(self):
        from google.cloud.bigquery import SchemaField

        path = "projects/%s/datasets/%s/tables" % (self.PROJECT, self.DS_ID)

        path_delete_table = "projects/%s/datasets/%s/tables/%s" % (
            self.PROJECT,
            self.DS_ID,
            self.TABLE_ID,
        )

        schema_fields = [
            {'mode': 'NULLABLE',
             'name': 'id',
             'type': 'INTEGER',
             'description': None},
            {'mode': 'NULLABLE',
             'name': 'name',
             'type': 'STRING',
             'description': None},
            {'mode': 'NULLABLE',
             'name': 'loaded_at',
             'type': 'DATETIME'},
            {'mode': 'NULLABLE',
             'name': 'to_remove',
             'type': 'BOOLEAN'}
        ]

        table_resource = {
            'tableReference': {
                'projectId': self.PROJECT,
                'datasetId': self.DS_ID,
                'tableId': self.TABLE_ID
            },
            'labels': {},
            'schema': {
                'fields': schema_fields
            },
            'timePartitioning': {"type": "DAY"}
        }

        bq_schema = [SchemaField('id', 'INTEGER', 'NULLABLE', None, ()),
                     SchemaField('name', 'STRING', 'NULLABLE', None, ())]

        conn = self.client._connection = _make_connection(self.dataset_resource,
                                                          AlreadyExists("table already exists"),
                                                          *([{}]),
                                                          table_resource)
        mock_client = mock.patch("google.cloud.bigquery.Client",
                                 return_value=self.client)

        mock_output = mock.patch('sys.stdout', new_callable=io.StringIO)
        with mock_client:
            with mock_output as fake_output:
                bq = BigQuery(self.DS_ID)
                table = bq.create_partitioned_table(self.TABLE_ID, bq_schema, exists_ok=False)
                output = fake_output.getvalue().strip().split('\n')

        assert output[0] == "Deleted table {}:{}.{}".format(self.PROJECT,
                                                            self.DS_ID,
                                                            self.TABLE_ID)

        assert output[1] == "Created table {}:{}.{}".format(self.PROJECT,
                                                            self.DS_ID,
                                                            self.TABLE_ID)

        self.assertEqual(table.table_id, self.TABLE_ID)
        self.assertEqual(table.time_partitioning.type_, "DAY")

        conn.api_request.assert_called_with(
            method="POST",
            path="/%s" % path,
            data=table_resource,
            timeout=None
        )

        conn.api_request.assert_any_call(
            method="DELETE",
            path="/%s" % path_delete_table,
            timeout=None
        )

    def test_create_partitioned_table_with_coloumn_to_remove(self):
        pass

    @mock.patch('bigquery.tools.datetime')
    def test_insert_row_by_stream(self, mock_datetime):

        PATH = "projects/%s/datasets/%s/tables/%s/insertAll" % (
            self.PROJECT,
            self.DS_ID,
            self.TABLE_ID,
        )

        schema_fields = [
            {'name': 'acessosid', 'type': 'INTEGER'},
            {'name': 'acessosno', 'type': 'STRING'},
            {'name': 'acessos_datainicio', 'type': 'DATE'},
            {'name': 'cod_externo', 'type': 'STRING'},
            {'name': 'loaded_at', 'type': 'DATETIME'},
            {'name': 'to_remove', 'type': 'BOOLEAN'}
        ]

        ROW = {
            "acessosid": 1427162,
            "acessosno": "ACE173343",
            "acessos_datainicio": "2019-10-18",
            "cod_externo": "null",
            "to_remove": False
        }

        table_resource = {
            'tableReference': {
                'projectId': self.PROJECT,
                'datasetId': self.DS_ID,
                'tableId': self.TABLE_ID
            },
            'schema': {
                'fields': schema_fields
            },
            'timePartitioning': {"type": "DAY"}
        }

        conn = self.client._connection = _make_connection(self.dataset_resource,
                                                          table_resource,
                                                          {})

        mock_client = mock.patch("google.cloud.bigquery.Client",
                                 return_value=self.client)

        def _row_data(row):

            row_data = {}

            for field in schema_fields:
                key = field['name']
                data_type = field['type']
                if key == 'loaded_at':
                    row_data[key] = '2019-10-16T21:08:06.519383'
                else:
                    value = row[key]
                    if data_type == 'DATETIME':
                        row_data[key] = str(value).replace(' ', 'T') + '.000000'
                    elif data_type == 'BOOLEAN':
                        row_data[key] = str(value).lower()
                    else:
                        row_data[key] = str(value)

            return row_data

        SENT = {
            "rows": [
                {"json": _row_data(ROW), "insertId": str(0)}
            ]
        }

        mock_datetime.now.return_value = '2019-10-16T21:08:06.519383'

        mock_output = mock.patch('sys.stdout', new_callable=io.StringIO)
        with mock.patch("uuid.uuid4", side_effect=map(str, range(len(ROW)))):
            with mock_client:
                with mock_output as fake_output:
                    bq = BigQuery(self.DS_ID)
                    errors = bq.insert_row_by_stream(self.TABLE_ID, ROW)
                    output = fake_output.getvalue().strip()

        assert output == f'Row inserted in table {self.TABLE_ID}.'
        self.assertEqual(len(errors), 0)
        conn.api_request.assert_called_with(method="POST",
                                            path="/%s" % PATH,
                                            data=SENT,
                                            timeout=None)

    @mock.patch('bigquery.tools.datetime')
    def test_insert_rows_list_by_stream(self, mock_datetime):
        PATH = "projects/%s/datasets/%s/tables/%s/insertAll" % (
            self.PROJECT,
            self.DS_ID,
            self.TABLE_ID,
        )
        client = _make_bigquery_client(
            project=self.PROJECT,
            credentials=self.credentials,
            _http=self.http)

        schema_fields = [
            {'name': 'acessosid', 'type': 'INTEGER'},
            {'name': 'acessosno', 'type': 'STRING'},
            {'name': 'acessos_datainicio', 'type': 'DATE'},
            {'name': 'cod_externo', 'type': 'STRING'},
            {'name': 'loaded_at', 'type': 'DATETIME'},
            {'name': 'to_remove', 'type': 'BOOLEAN'}
        ]

        ROWS = [
            {
                "acessosid": 1427162,
                "acessosno": "ACE173343",
                "acessos_datainicio": "2019-10-18",
                "cod_externo": "null",
                "to_remove": False
            },
            {
                "acessosid": 1427165,
                "acessosno": "ACE173243",
                "acessos_datainicio": "2019-10-08",
                "cod_externo": "null",
                "to_remove": True
            },

        ]

        table_resource = {
            'tableReference': {
                'projectId': self.PROJECT,
                'datasetId': self.DS_ID,
                'tableId': self.TABLE_ID
            },
            'labels': {},
            'schema': {
                'fields': schema_fields
            },
            'timePartitioning': {"type": "DAY"}
        }

        conn = client._connection = _make_connection(self.dataset_resource,
                                                     table_resource,
                                                     {})

        mock_client = mock.patch("google.cloud.bigquery.Client",
                                 return_value=client)

        def _row_data(row):

            row_data = {}

            for field in schema_fields:
                key = field['name']
                data_type = field['type']
                if key == 'loaded_at':
                    row_data[key] = '2019-10-16T21:08:06.519383'
                else:
                    value = row[key]
                    if data_type == 'DATETIME':
                        row_data[key] = str(value).replace(' ', 'T') + '.000000'
                    elif data_type == 'BOOLEAN':
                        row_data[key] = str(value).lower()
                    else:
                        row_data[key] = str(value)

            return row_data

        SENT = {
            "rows": [
                {"json": _row_data(row), "insertId": str(i)}
                for i, row in enumerate(ROWS)
            ]
        }

        mock_datetime.now.return_value = '2019-10-16T21:08:06.519383'

        mock_output = mock.patch('sys.stdout', new_callable=io.StringIO)
        with mock.patch("uuid.uuid4", side_effect=map(str, range(len(ROWS)))):
            with mock_client:
                with mock_output as fake_output:
                    bq = BigQuery(self.DS_ID)
                    errors = bq.insert_rows_list_by_stream(self.TABLE_ID, ROWS)
                    output = fake_output.getvalue().strip()

        assert output == f'Inserted 2 rows  in table {self.TABLE_ID}.'
        self.assertEqual(len(errors), 0)
        conn.api_request.assert_called_with(method="POST",
                                            path="/%s" % PATH,
                                            data=SENT,
                                            timeout=None)

    @mock.patch('bigquery.tools.datetime')
    def test_insert_row_by_stream_with_errors(self, mock_datetime):

        PATH = "projects/%s/datasets/%s/tables/%s/insertAll" % (
            self.PROJECT,
            self.DS_ID,
            self.TABLE_ID,
        )

        schema_fields = [
            {'name': 'acessosid', 'type': 'INTEGER'},
            {'name': 'acessosno', 'type': 'STRING'},
            {'name': 'acessos_datainicio', 'type': 'DATE'},
            {'name': 'cod_externo', 'type': 'STRING'},
            {'name': 'loaded_at', 'type': 'DATETIME'},
            {'name': 'to_remove', 'type': 'BOOLEAN'}
        ]

        table_resource = {
            'tableReference': {
                'projectId': self.PROJECT,
                'datasetId': self.DS_ID,
                'tableId': self.TABLE_ID
            },
            'schema': {
                'fields': schema_fields
            },
            'timePartitioning': {"type": "DAY"}
        }

        errors_resource = {
            "kind": 'string',
            "insertErrors": [
                {
                    "index": 1,
                    "errors": [
                        {
                            "reason": 'string',
                            "location": 'string',
                            "debugInfo": 'string',
                            "message": 'string'
                        }
                    ]
                }
            ]
        }

        conn = self.client._connection = _make_connection(self.dataset_resource,
                                                          table_resource,
                                                          errors_resource)
        mock_client = mock.patch("google.cloud.bigquery.Client",
                                 return_value=self.client)

        ROWS = [{}]

        SENT = {'rows': [{
            'json': {'loaded_at': '2019-10-16T21:08:06.519383'},
            'insertId': '0'}
        ]}

        mock_datetime.now.return_value = '2019-10-16T21:08:06.519383'

        mock_output = mock.patch('sys.stdout', new_callable=io.StringIO)
        with mock_client:
            with mock_output as fake_output:
                with mock.patch("uuid.uuid4", side_effect=map(str, range(len(ROWS)))):
                    bq = BigQuery(self.DS_ID)
                    errors = bq.insert_rows_list_by_stream(self.TABLE_ID, ROWS)
                    output = fake_output.getvalue().strip()

        assert errors == errors_resource['insertErrors']
        assert output == f"errors: {errors_resource['insertErrors']}"
        conn.api_request.assert_called_with(method="POST",
                                            path="/%s" % PATH,
                                            data=SENT,
                                            timeout=None)

    def test_get_dataset_tables(self):
        client = _make_bigquery_client(project=self.PROJECT,
                                       credentials=self.credentials,
                                       _http=self.http)

        TABLE_1 = "table_one"
        TABLE_2 = "table_two"
        table_iterator_resource = {
            "tables": [
                {
                    "kind": "bigquery#table",
                    "id": "%s:%s.%s" % (self.PROJECT, self.DS_ID, TABLE_1),
                    "tableReference": {
                        "tableId": TABLE_1,
                        "datasetId": self.DS_ID,
                        "projectId": self.PROJECT,
                    },
                    "type": "TABLE",
                },
                {
                    "kind": "bigquery#table",
                    "id": "%s:%s.%s" % (self.PROJECT, self.DS_ID, TABLE_2),
                    "tableReference": {
                        "tableId": TABLE_2,
                        "datasetId": self.DS_ID,
                        "projectId": self.PROJECT,
                    },
                    "type": "TABLE",
                },
            ],
        }

        client._connection = _make_connection(self.dataset_resource,
                                              table_iterator_resource)
        mock_client = mock.patch("google.cloud.bigquery.Client",
                                 return_value=client)

        with mock_client:
            bq = BigQuery(self.DS_ID)
            tables_list = bq.get_dataset_tables()

        assert tables_list == ['table_one', 'table_two']

    def test__copy_table(self):
        from google.cloud.bigquery.client import TableReference

        client = _make_bigquery_client(project=self.PROJECT,
                                       credentials=self.credentials,
                                       _http=self.http)

        dataset_copy_id = 'DATASET_COPY_ID'

        source_id = "source_table"
        source_table_ref = {
            "projectId": self.PROJECT,
            "datasetId": self.DS_ID,
            "tableId": source_id,
        }
        source_table_resource = {
            'tableReference': source_table_ref,
            "numRows": 100
        }

        destination_id = "destination_table"
        destination_table_ref = {
            "projectId": self.PROJECT,
            "datasetId": dataset_copy_id,
            "tableId": destination_id,
        }
        destination_table_resource = {
            'tableReference': destination_table_ref,
            "numRows": 100
        }

        job = "job_name"
        copy_job_resource = {
            "jobReference": {"projectId": self.PROJECT, "jobId": job},
            "configuration": {
                "copy": {
                    "sourceTables": source_table_ref,
                    "destinationTable": destination_table_ref,
                }
            },
            "status": {
                "state": "DONE"
            },
        }

        source_ref = TableReference.from_api_repr(source_table_ref)
        destination_ref = TableReference.from_api_repr(destination_table_ref)

        client._connection = _make_connection(self.dataset_resource,
                                              copy_job_resource,
                                              destination_table_resource,
                                              source_table_resource)

        mock_client = mock.patch("google.cloud.bigquery.Client",
                                 return_value=client)

        mock_output = mock.patch('sys.stdout', new_callable=io.StringIO)
        with mock_client:
            with mock_output as fake_output:
                bq = BigQuery(self.DS_ID)
                table_copy = bq._copy_table(source_ref, destination_ref)
                output = fake_output.getvalue().strip()

        self.assertEqual(table_copy.table_id, destination_id)
        assert output == 'Table source_table copied to destination_table'

    @mock.patch.object(BigQuery, '_copy_table')
    def test_copy_dataset(self, mock_copy_table):
        from google.cloud.bigquery.client import TableReference, DatasetReference

        client = _make_bigquery_client(project=self.PROJECT,
                                       credentials=self.credentials,
                                       _http=self.http)

        DS_ID = 'DATASET_COPY_ID'
        copy_dataset_resource = {
            "id": "%s:%s" % (self.PROJECT, DS_ID),
            "datasetReference": {
                "projectId": self.PROJECT, "datasetId": DS_ID
            },
        }

        TABLE_1 = "table_one"
        TABLE_2 = "table_two"
        table_iterator_resource = {
            "tables": [
                {
                    "kind": "bigquery#table",
                    "id": "%s:%s.%s" % (self.PROJECT, self.DS_ID, TABLE_1),
                    "tableReference": {
                        "tableId": TABLE_1,
                        "datasetId": self.DS_ID,
                        "projectId": self.PROJECT,
                    },
                    "type": "TABLE",
                },
                {
                    "kind": "bigquery#table",
                    "id": "%s:%s.%s" % (self.PROJECT, self.DS_ID, TABLE_2),
                    "tableReference": {
                        "tableId": TABLE_2,
                        "datasetId": self.DS_ID,
                        "projectId": self.PROJECT,
                    },
                    "type": "TABLE",
                },
            ],
        }

        client._connection = _make_connection(self.dataset_resource,
                                              copy_dataset_resource,
                                              table_iterator_resource)

        mock_client = mock.patch("google.cloud.bigquery.Client",
                                 return_value=client)

        mock_output = mock.patch('sys.stdout', new_callable=io.StringIO)
        with mock_client:
            with mock_output as fake_output:
                bq = BigQuery(self.DS_ID)
                dataset_copy = bq.copy_dataset(DS_ID)
                output = fake_output.getvalue().strip()

        today = str(date.today()).replace('-', '_')
        table_ref_1 = TableReference(
            DatasetReference('PROJECT', 'DATASET_ID'),
            'table_one'
        )
        table_ref_1_old = TableReference(
            DatasetReference('PROJECT', 'DATASET_COPY_ID'),
            f'table_one_{today}'
        )
        mock_copy_table.assert_any_call(table_ref_1, table_ref_1_old)

        table_ref_2 = TableReference(
            DatasetReference('PROJECT', 'DATASET_ID'),
            'table_two'
        )
        table_ref_2_old = TableReference(
            DatasetReference('PROJECT', 'DATASET_COPY_ID'),
            f'table_two_{today}'
        )
        mock_copy_table.assert_called_with(table_ref_2, table_ref_2_old)

        assert output == 'Dataset DATASET_ID copied to DATASET_COPY_ID'

        self.assertEqual(dataset_copy.dataset_id, DS_ID)

    def test_delete_table(self):

        path = "projects/%s/datasets/%s/tables/%s" % (
            self.PROJECT,
            self.DS_ID,
            self.TABLE_ID,
        )

        client = _make_bigquery_client(project=self.PROJECT,
                                       credentials=self.credentials,
                                       _http=self.http)

        conn = client._connection = _make_connection(self.dataset_resource,
                                                     *([{}]))
        mock_client = mock.patch("google.cloud.bigquery.Client",
                                 return_value=client)

        mock_output = mock.patch('sys.stdout', new_callable=io.StringIO)
        with mock_client:
            with mock_output as fake_output:
                bq = BigQuery(self.DS_ID)
                bq.delete_table(self.TABLE_ID)
                output = fake_output.getvalue().strip()

        assert output == "Deleted table {}:{}.{}".format(self.PROJECT,
                                                         self.DS_ID,
                                                         self.TABLE_ID)

        conn.api_request.assert_any_call(
            method="DELETE", path="/%s" % path, timeout=None
        )

    def test_make_schema(self):
        from google.cloud.bigquery import SchemaField

        expect_result = [
            SchemaField('id', 'INTEGER', 'NULLABLE', None, ()),
            SchemaField('name', 'STRING', 'NULLABLE', None, ()),
            SchemaField('datetime', 'DATETIME', 'NULLABLE', None, ())
        ]

        schema = [
            {"name": "id", "type": 'INTEGER'},
            {"column_name": "name", "column_type": "STRING"},
            {"field_name": "datetime", "field_type": "DATETIME"}
        ]

        client = _make_bigquery_client(project=self.PROJECT,
                                       credentials=self.credentials,
                                       _http=self.http)

        client._connection = _make_connection(self.dataset_resource)
        mock_client = mock.patch("google.cloud.bigquery.Client",
                                 return_value=client)

        with mock_client:
            bigquery_schema = BigQuery(self.DS_ID).make_schema(schema)

        self.assertEqual(bigquery_schema, expect_result)

    def test_convert_mysql_schema_to_bigquery(self):

        mysql_schema = [
            {'column_name': 'int', 'column_type': 'int(10)'},
            {'column_name': 'tinyint', 'column_type': 'tinyint(1)'},
            {'column_name': 'smallint', 'column_type': 'smallint'},
            {'column_name': 'mediumint', 'column_type': 'mediumint'},
            {'column_name': 'bigint', 'column_type': 'bigint'},
            {'column_name': 'decimal', 'column_type': 'decimal'},
            {'column_name': 'float', 'column_type': 'float'},
            {'column_name': 'double', 'column_type': 'double'},
            {'column_name': 'bit', 'column_type': 'bit'},
            {'column_name': 'char', 'column_type': 'char'},
            {'column_name': 'varchar', 'column_type': 'varchar(255)'},
            {'column_name': 'BLOB', 'column_type': 'BLOB'},
            {'column_name': 'text', 'column_type': 'text'},
            {'column_name': 'tinytext', 'column_type': 'tinytext'},
            {'column_name': 'mediumtext', 'column_type': 'mediumtext'},
            {'column_name': 'longtext', 'column_type': 'longtext'},
            {'column_name': 'enum', 'column_type': 'enum'},
            {'column_name': 'binary', 'column_type': 'binary'},
            {'column_name': 'date', 'column_type': 'date'},
            {'column_name': 'time', 'column_type': 'time'},
            {'column_name': 'timestamp', 'column_type': 'timestamp'},
            {'column_name': 'year', 'column_type': 'year'},
            {'column_name': 'geometry', 'column_type': 'geometry'},
            {'column_name': 'json', 'column_type': 'json'}
        ]

        expect_result = [
            SchemaField('int', 'INTEGER', 'NULLABLE', None, ()),
            SchemaField('tinyint', 'INTEGER', 'NULLABLE', None, ()),
            SchemaField('smallint', 'INTEGER', 'NULLABLE', None, ()),
            SchemaField('mediumint', 'INTEGER', 'NULLABLE', None, ()),
            SchemaField('bigint', 'INTEGER', 'NULLABLE', None, ()),
            SchemaField('decimal', 'FLOAT', 'NULLABLE', None, ()),
            SchemaField('float', 'FLOAT', 'NULLABLE', None, ()),
            SchemaField('double', 'FLOAT', 'NULLABLE', None, ()),
            SchemaField('bit', 'BYTES', 'NULLABLE', None, ()),
            SchemaField('char', 'STRING', 'NULLABLE', None, ()),
            SchemaField('varchar', 'STRING', 'NULLABLE', None, ()),
            SchemaField('BLOB', 'STRING', 'NULLABLE', None, ()),
            SchemaField('text', 'STRING', 'NULLABLE', None, ()),
            SchemaField('tinytext', 'STRING', 'NULLABLE', None, ()),
            SchemaField('mediumtext', 'STRING', 'NULLABLE', None, ()),
            SchemaField('longtext', 'STRING', 'NULLABLE', None, ()),
            SchemaField('enum', 'STRING', 'NULLABLE', None, ()),
            SchemaField('binary', 'STRING', 'NULLABLE', None, ()),
            SchemaField('date', 'DATE', 'NULLABLE', None, ()),
            SchemaField('time', 'TIME', 'NULLABLE', None, ()),
            SchemaField('timestamp', 'TIMESTAMP', 'NULLABLE', None, ()),
            SchemaField('year', 'DATE', 'NULLABLE', None, ()),
            SchemaField('geometry', 'GEOGRAPHY', 'NULLABLE', None, ()),
            SchemaField('json', 'STRING', 'NULLABLE', None, ())
        ]

        bigquery_schema = BigQuery.convert_mysql_schema_to_bigquery(mysql_schema)

        self.assertEqual(bigquery_schema, expect_result)

    def test_convert_postgres_schema_to_bigquery(self):

        pg_schema = [
            {'column_name': 'array', 'data_type': 'array'},
            {'column_name': 'abstime', 'data_type': 'abstime'},
            {'column_name': 'anyarray', 'data_type': 'anyarray'},
            {'column_name': 'boolean', 'data_type': 'boolean'},
            {'column_name': 'integer', 'data_type': 'integer'},
            {'column_name': 'bigint', 'data_type': 'bigint'},
            {'column_name': 'numeric', 'data_type': 'numeric'},
            {'column_name': 'smallint', 'data_type': 'smallint'},
            {'column_name': 'decimal', 'data_type': 'decimal'},
            {'column_name': 'real', 'data_type': 'real'},
            {'column_name': 'double precision', 'data_type': 'double precision'},
            {'column_name': 'smallserial', 'data_type': 'smallserial'},
            {'column_name': 'serial', 'data_type': 'serial'},
            {'column_name': 'bigserial', 'data_type': 'bigserial'},
            {'column_name': 'char', 'data_type': 'char'},
            {'column_name': 'varchar', 'data_type': 'varchar'},
            {'column_name': 'character', 'data_type': 'character'},
            {'column_name': 'character varying', 'data_type': 'character varying'},
            {'column_name': 'text', 'data_type': 'text'},
            {'column_name': 'json', 'data_type': 'json'},
            {'column_name': 'jsonb', 'data_type': 'jsonb'},
            {'column_name': 'date', 'data_type': 'date'},
            {'column_name': 'time', 'data_type': 'time'},
            {'column_name': 'datetime', 'data_type': 'datetime'},
            {'column_name': 'timestamp', 'data_type': 'timestamp'},
            {'column_name': 'timestamp with time zone', 'data_type': 'timestamp with time zone'},
            {'column_name': 'timestamp without time zone', 'data_type': 'timestamp without time zone'},
            {'column_name': 'bytea', 'data_type': 'bytea'},
            {'column_name': 'inet', 'data_type': 'inet'},
            {'column_name': 'interval', 'data_type': 'interval'},
            {'column_name': 'name', 'data_type': 'name'},
            {'column_name': 'oid', 'data_type': 'oid'},
            {'column_name': 'pg_lsn', 'data_type': 'pg_lsn'},
            {'column_name': 'pg_node_tree', 'data_type': 'pg_node_tree'},
            {'column_name': 'regproc', 'data_type': 'regproc'},
            {'column_name': 'xid', 'data_type': 'xid'},
        ]

        expect_result = [
            SchemaField('array', 'STRING', 'NULLABLE', None, ()),
            SchemaField('abstime', 'STRING', 'NULLABLE', None, ()),
            SchemaField('anyarray', 'STRING', 'NULLABLE', None, ()),
            SchemaField('boolean', 'BOOLEAN', 'NULLABLE', None, ()),
            SchemaField('integer', 'INTEGER', 'NULLABLE', None, ()),
            SchemaField('bigint', 'INTEGER', 'NULLABLE', None, ()),
            SchemaField('numeric', 'NUMERIC', 'NULLABLE', None, ()),
            SchemaField('smallint', 'INTEGER', 'NULLABLE', None, ()),
            SchemaField('decimal', 'FLOAT', 'NULLABLE', None, ()),
            SchemaField('real', 'FLOAT', 'NULLABLE', None, ()),
            SchemaField('double precision', 'FLOAT', 'NULLABLE', None, ()),
            SchemaField('smallserial', 'INTEGER', 'NULLABLE', None, ()),
            SchemaField('serial', 'INTEGER', 'NULLABLE', None, ()),
            SchemaField('bigserial', 'INTEGER', 'NULLABLE', None, ()),
            SchemaField('char', 'STRING', 'NULLABLE', None, ()),
            SchemaField('varchar', 'STRING', 'NULLABLE', None, ()),
            SchemaField('character', 'STRING', 'NULLABLE', None, ()),
            SchemaField('character varying', 'STRING', 'NULLABLE', None, ()),
            SchemaField('text', 'STRING', 'NULLABLE', None, ()),
            SchemaField('json', 'STRING', 'NULLABLE', None, ()),
            SchemaField('jsonb', 'STRING', 'NULLABLE', None, ()),
            SchemaField('date', 'DATE', 'NULLABLE', None, ()),
            SchemaField('time', 'TIME', 'NULLABLE', None, ()),
            SchemaField('datetime', 'DATETIME', 'NULLABLE', None, ()),
            SchemaField('timestamp', 'TIMESTAMP', 'NULLABLE', None, ()),
            SchemaField('timestamp with time zone', 'TIMESTAMP', 'NULLABLE', None, ()),
            SchemaField('timestamp without time zone', 'TIMESTAMP', 'NULLABLE', None, ()),
            SchemaField('bytea', 'STRING', 'NULLABLE', None, ()),
            SchemaField('inet', 'STRING', 'NULLABLE', None, ()),
            SchemaField('interval', 'STRING', 'NULLABLE', None, ()),
            SchemaField('name', 'STRING', 'NULLABLE', None, ()),
            SchemaField('oid', 'STRING', 'NULLABLE', None, ()),
            SchemaField('pg_lsn', 'STRING', 'NULLABLE', None, ()),
            SchemaField('pg_node_tree', 'STRING', 'NULLABLE', None, ()),
            SchemaField('regproc', 'STRING', 'NULLABLE', None, ()),
            SchemaField('xid', 'STRING', 'NULLABLE', None, ())
        ]

        bigquery_schema = BigQuery.convert_postgres_schema_to_bigquery(pg_schema)

        self.assertEqual(bigquery_schema, expect_result)

    def test_convert_python_schema_to_bigquery(self):

        python_schema = [
            {'column_name': 'str', 'column_type': 'str'},
            {'column_name': 'int', 'column_type': 'int'},
            {'column_name': 'float', 'column_type': 'float'},
            {'column_name': 'complex', 'column_type': 'complex'},
            {'column_name': 'bool', 'column_type': 'bool'},
            {'column_name': 'date', 'column_type': 'date'},
            {'column_name': 'time', 'column_type': 'time'},
            {'column_name': 'datetime', 'column_type': 'datetime'},
            {'column_name': 'timedelta', 'column_type': 'timedelta'},
            {'column_name': 'tzinfo', 'column_type': 'tzinfo'},
            {'column_name': 'timezone', 'column_type': 'timezone'},
            {'column_name': 'list', 'column_type': 'list'},
            {'column_name': 'dict', 'column_type': 'dict'},
            {'column_name': 'tuple', 'column_type': 'tuple'},
            {'column_name': 'range', 'column_type': 'range'},
            {'column_name': 'bytes', 'column_type': 'bytes'},
            {'column_name': 'bytearray', 'column_type': 'bytearray'},
            {'column_name': 'memoryview', 'column_type': 'memoryview'},
            {'column_name': 'list', 'column_type': 'list'},

        ]
        expect_result = [
            SchemaField('str', 'STRING', 'NULLABLE', None, ()),
            SchemaField('int', 'INTEGER', 'NULLABLE', None, ()),
            SchemaField('float', 'FLOAT', 'NULLABLE', None, ()),
            SchemaField('complex', 'STRING', 'NULLABLE', None, ()),
            SchemaField('bool', 'BOOLEAN', 'NULLABLE', None, ()),
            SchemaField('date', 'DATE', 'NULLABLE', None, ()),
            SchemaField('time', 'TIME', 'NULLABLE', None, ()),
            SchemaField('datetime', 'DATETIME', 'NULLABLE', None, ()),
            SchemaField('timedelta', 'STRING', 'NULLABLE', None, ()),
            SchemaField('tzinfo', 'STRING', 'NULLABLE', None, ()),
            SchemaField('timezone', 'STRING', 'NULLABLE', None, ()),
            SchemaField('list', 'STRING', 'NULLABLE', None, ()),
            SchemaField('dict', 'STRING', 'NULLABLE', None, ()),
            SchemaField('tuple', 'STRING', 'NULLABLE', None, ()),
            SchemaField('range', 'STRING', 'NULLABLE', None, ()),
            SchemaField('bytes', 'STRING', 'NULLABLE', None, ()),
            SchemaField('bytearray', 'STRING', 'NULLABLE', None, ()),
            SchemaField('memoryview', 'STRING', 'NULLABLE', None, ()),
            SchemaField('list', 'STRING', 'NULLABLE', None, ())
        ]

        bigquery_schema = BigQuery.convert_python_schema_to_bigquery(python_schema)

        self.assertEqual(bigquery_schema, expect_result)