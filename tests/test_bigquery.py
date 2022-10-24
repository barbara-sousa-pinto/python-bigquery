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
             'type': 'DATETIME',
             'description': None},
            {'mode': 'NULLABLE',
             'name': 'to_remove',
             'type': 'BOOLEAN',
             'description': None}
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
             'type': 'DATETIME',
             'description': None},
            {'mode': 'NULLABLE',
             'name': 'to_remove',
             'type': 'BOOLEAN',
             'description': None}
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
             'type': 'DATETIME',
             'description': None},
            {'mode': 'NULLABLE',
             'name': 'to_remove',
             'type': 'BOOLEAN',
             'description': None}
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
             'type': 'DATETIME',
             'description': None},
            {'mode': 'NULLABLE',
             'name': 'to_remove',
             'type': 'BOOLEAN',
             'description': None}
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
             'type': 'DATETIME',
             'description': None},
            {'mode': 'NULLABLE',
             'name': 'to_remove',
             'type': 'BOOLEAN',
             'description': None}
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