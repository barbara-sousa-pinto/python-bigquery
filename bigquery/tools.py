from datetime import datetime, date
from google.api_core.exceptions import Conflict, NotFound
from google.cloud import bigquery
from google.cloud.exceptions import BadRequest


class BigQuery:
    """Class for interacting with the Google Cloud Python Client.

    Args:
        dataset_name (str): The class dataset name.
        creates_ok (bool):
            Defaults to ``True``.
            If ``True``, if the dataset does not exist, BigQuery creates the dataset.
            If ``False``, if the dataset does not exist, a ‘notFound’ error is returned.
        project (str):  Project ID for the project which the client acts on behalf of.
            Will be passed when creating a dataset / job.
            If not passed, falls back to the default inferred from the environment.

    Raises:
        google.auth.exceptions.DefaultCredentialsError:
            Raised if ``credentials`` is not specified and the library fails
            to acquire default credentials.
    """

    def __init__(self, dataset_name, creates_ok=True, project=None):

        self.client = bigquery.Client(project)
        self.project = self.client.project
        self.dataset_name = dataset_name
        self.creates_ok = creates_ok
        self.dataset = self._get_dataset()

    def _get_dataset(self):

        try:
            dataset = self.client.get_dataset(self.dataset_name)
        except NotFound:
            if self.creates_ok:
                dataset = self.client.create_dataset(self.dataset_name)
                print("Created dataset {}:{}".format(self.project,
                                                     dataset.dataset_id))
            else:
                raise

        return dataset

    def get_table_records(self, table_name):
        """Get all records from a table by name.

        Args:
            table_name (str): The table name.

        Return:
            A list of dictionaries. Each element is a row from the table.
        """

        get_records_query = f"SELECT * FROM {self.dataset_name}.{table_name}"
        # Getting table contents
        try:
            query_job = self.client.query(get_records_query)  # API request
            print(get_records_query)
            result = query_job.result()  # Waits for query to finish
            return [(dict(record)) for record in result]
        except NotFound:
            return None

    @staticmethod
    def make_schema(field_list):
        """Create a schema in the format that BigQuery accepts.

        Args:
            field_list (Iterable[Dict[str, str]]):
                Schema data to be transformed in a bigquery schema.
                Each row describe a single field within a table schema.


        Returns:
            List[google.cloud.bigquery.schema.SchemaField]
            Table schema as BigQuery accepts.
            A list with the fields of a BigQuery table.
        """
        bigquery_schema = []
        for field in field_list:
            field_values = list(field.values())
            name = field_values[0]
            field_type = field_values[1]
            bigquery_schema.append(bigquery.SchemaField(name, field_type))

        return bigquery_schema

    @staticmethod
    def convert_mysql_schema_to_bigquery(mysql_schema):
        """Take the schema of a MySQL table and transform it in a BigQuery schema.

        Args:
            mysql_schema (List[Dict[str, str]]):
                Each list item is a dictionary that describes a field within a table schema.
                The first dictionary element is the column name and the second is the column type.

        Returns:
            List[google.cloud.bigquery.schema.SchemaField]:
                The table’s schema in BigQuery format.
        """

        bigquery_schema = []

        for line in mysql_schema:
            mysql_bigquery_types = {
                "int": "INTEGER",
                "tinyint": "INTEGER",
                "smallint": "INTEGER",
                "mediumint": "INTEGER",
                "bigint": "INTEGER",
                "decimal": "FLOAT",
                "float": "FLOAT",
                "double": "FLOAT",
                "bit": "BYTES",
                "char": "STRING",
                "varchar": "STRING",
                "BLOB": "STRING",
                "text": "STRING",
                "tinytext": "STRING",
                "mediumtext": "STRING",
                "longtext": "STRING",
                "enum": "STRING",
                "binary": "STRING",
                "date": "DATE",
                "time": "TIME",
                "datetime": "DATETIME",
                "timestamp": "TIMESTAMP",
                "year": "DATE",
                "geometry": "GEOGRAPHY",
                "json": "STRING"
            }

            column_name = line['column_name']
            mysql_type = line['column_type'].split('(')[0]
            column_type = mysql_bigquery_types[mysql_type]

            bigquery_schema.append(bigquery.SchemaField(column_name, column_type))

        return bigquery_schema

    @staticmethod
    def convert_postgres_schema_to_bigquery(postgres_schema):
        """Take the schema of a Postgres table and transform it in a BigQuery schema.

        Args:
            postgres_schema (List[Dict[str, str]]):
                Each list item is a dictionary that describes a field within a table schema.
                The first dictionary element is the column name and the second is the column type.

        Returns:
            List[google.cloud.bigquery.schema.SchemaField]:
                The table’s schema in BigQuery format.
        """

        postgres_bigquery_types = {
            "array": "STRING",
            "abstime": "STRING",
            "anyarray": "STRING",
            "boolean": "BOOLEAN",
            "integer": "INTEGER",
            "bigint": "INTEGER",
            "numeric": "NUMERIC",
            "smallint": "INTEGER",
            "decimal": "FLOAT",
            "real": "FLOAT",
            "double precision": "FLOAT",
            "smallserial": "INTEGER",
            "serial": "INTEGER",
            "bigserial": "INTEGER",
            "char": "STRING",
            "varchar": "STRING",
            "character": "STRING",
            "character varying": "STRING",
            "text": "STRING",
            "json": "STRING",
            "jsonb": "STRING",
            "date": "DATE",
            "time": "TIME",
            "datetime": "DATETIME",
            "timestamp": "TIMESTAMP",
            "timestamp with time zone": "TIMESTAMP",
            "timestamp without time zone": "TIMESTAMP",
            "bytea": "STRING",
            "inet": "STRING",
            "interval": "STRING",
            "name": "STRING",
            "oid": "STRING",
            "pg_lsn": "STRING",
            "pg_node_tree": "STRING",
            "regproc": "STRING",
            "xid": "STRING",
        }

        bigquery_schema = []

        for column in postgres_schema:
            column_name = column['column_name']
            postgres_type = column['data_type']
            column_type = postgres_bigquery_types.setdefault(postgres_type, 'STRING')

            bigquery_schema.append(bigquery.SchemaField(column_name, column_type))

        return bigquery_schema

    @staticmethod
    def convert_python_schema_to_bigquery(python_schema):
        """Take the schema of a table with the column types in python datatype
        and transform it in a BigQuery schema.

        Args:
            python_schema (List[Dict[str, str]]):
                Each list item is a dictionary that describes a field within a table schema.
                The first dictionary element is the column name and the second is the column type.

        Returns:
            List[google.cloud.bigquery.schema.SchemaField]:
                The table’s schema in BigQuery format.
        """

        python_bigquery_types = {
            'str': 'STRING',
            'int': 'INTEGER',
            'float': 'FLOAT',
            'complex': 'STRING',
            'bool': 'BOOLEAN',
            'date': 'DATE',
            'time': 'TIME',
            'datetime': 'DATETIME',
            'timedelta': 'STRING',
            'tzinfo': 'STRING',
            'timezone': 'STRING',
            'list': 'STRING',
            'dict': 'STRING',
            'tuple': 'STRING',
            'range': 'STRING',
            'bytes': 'STRING',
            'bytearray': 'STRING',
            'memoryview': 'STRING',
        }

        bigquery_schema = []
        for column in python_schema:
            column_name = column['column_name']
            column_type = python_bigquery_types[column['column_type']]
            bigquery_schema.append(bigquery.SchemaField(column_name, column_type))

        return bigquery_schema

    @staticmethod
    def create_schema_field(column_name, column_type):
        """Describe a single field within a bigquery table schema.

        Args:
            column_name (str): The name of the field.
            column_type (str): the type of the field. See
            https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#TableFieldSchema.FIELDS.type

        Returns:
            google.cloud.bigquery.schema.SchemaField

        """

        return bigquery.SchemaField(column_name, column_type)

    def load_table_from_dict(self, table_name, rows_to_load):
        """Upload the contents of a table from a dict.

        Args:
            table_name (str): The table name
            rows_to_load (Iterable[Dict[str, Any]]):
                Row data to be inserted. Keys must match the table schema fields
                and values must be JSON-compatible representations.

        Returns:
            google.cloud.bigquery.job.LoadJob: A new load job.

        """

        if rows_to_load:
            for row in rows_to_load:
                row['loaded_at'] = str(datetime.now())
                # row['to_remove'] = 0
            table_ref = self.dataset.table(table_name)
            table = self.client.get_table(table_ref)
            job_config = bigquery.LoadJobConfig()
            job_config.write_disposition = 'WRITE_APPEND'
            job_config.schema = table.schema
            load_job = self.client.load_table_from_json(
                rows_to_load,
                table,
                location="US",  # Location must match that of the destination dataset.
                job_config=job_config
            )  # API request

            load_job.result()  # Waits for table load to complete.
            print("Loaded {} rows in table {}.{}".format(load_job.output_rows,
                                                         self.dataset_name,
                                                         table_name))
            return load_job
        else:
            print('No rows to load.')

    def insert_row_by_stream(self, table_name, row_to_insert):
        """Insert a single row into a table via the streaming API.

        Args:
            table_name (str): The destination table name for the row data.
            row_to_insert (dict):
                Row data to be inserted. The keys must include all required
                fields in the schema. Keys which do not correspond to a field
                in the schema are ignored.

        Returns:
            dict:
                If row with insert errors: the "index" key
                identifies the row, and the "errors" key contains a list of
                the mappings describing one or more problems with the row.
                If no errors, returns a empty dict.
        """

        table_ref = self.dataset.table(table_name)
        table = self.client.get_table(table_ref)

        row_to_insert['loaded_at'] = datetime.now()
        errors = self.client.insert_rows(table, [row_to_insert])

        if not errors:
            print(f'Row inserted in table {table_name}.')
        else:
            print('errors:', errors)

        return errors

    def insert_rows_list_by_stream(self, table_name, rows_to_insert):
        """Insert rows into a table via the streaming API.

        Args:
            table_name (str): The destination table name for the row data.
            rows_to_insert (List[Dict[str, Any]]):
                Row data to be inserted. The keys must include all required
                fields in the schema. Keys which do not correspond to a field
                in the schema are ignored.

        Returns:
            Sequence[Mappings]:
                One mapping per row with insert errors: the "index" key
                identifies the row, and the "errors" key contains a list of
                the mappings describing one or more problems with the row.
        """

        table_ref = self.dataset.table(table_name)
        table = self.client.get_table(table_ref)

        for row in rows_to_insert:
            row['loaded_at'] = str(datetime.now())

        errors = self.client.insert_rows(table, rows_to_insert)

        if not errors:
            print(f'Inserted {len(rows_to_insert)} rows  in table {table_name}.')
        else:
            print('errors:', errors)

        return errors

    def create_partitioned_table(
            self,
            table_name,
            schema,
            exists_ok=True,
            column_to_remove=True,
            partition_by=None
    ):
        """Create a ingestion-time partitioned table in the class dataset.
        Table is partitioned based on the data's ingestion (load) time.
        A partitioned table is a special table that is divided into segments,
        called partitions, that make it easier to manage and query its data.

        See
        https://cloud.google.com/bigquery/docs/partitioned-tables

        Args:
            table_name (str): The ID of the table.
            schema (List [google.cloud.bigquery.schema.SchemaField]):
                The table’s schema.
            exists_ok (bool):
                If ``True``, ignore “already exists” errors when creating the table.
                If ``False`` and the table already exists, the table is deleted and a new table is created.
            column_to_remove (bool):
                Optional. Defaults to ``True``.
                If ``True``, add the field ``to_remove`` in table schema.
            partition_by (str, optional): Field in the table to use for partitioning.
            If set, the table is partitioned by this field.
            If not set, the table is partitioned by pseudo column ``_PARTITIONTIME``.
            The field must be a top-level ``TIMESTAMP`` or ``DATE`` field. Its mode must
            be ``NULLABLE`` or ``REQUIRED``.

        Returns:
            google.cloud.bigquery.table.Table:
                A new ``Table`` returned from the service.
        """

        table_ref = self.dataset.table(table_name)
        schema.append(bigquery.SchemaField('loaded_at', 'DATETIME'))
        if column_to_remove:
            schema.append(bigquery.SchemaField('to_remove', 'BOOLEAN'))
        table = bigquery.Table(table_ref, schema=schema)

        try:
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=partition_by
            )
            table = self.client.create_table(table)
            print(f"Created table {table.project}:{table.dataset_id}.{table.table_id}")
            return table
        except Conflict:
            if exists_ok:
                print(f"Table {table.project}:{table.dataset_id}.{table.table_id} already exists.")
            else:
                self.client.delete_table(table, not_found_ok=True)
                print(f"Deleted table {table.project}:{table.dataset_id}.{table.table_id}")
                self.client.create_table(table)
                print(f"Created table {table.project}:{table.dataset_id}.{table.table_id}")
            return table

    def tables_exists(self, table_name):
        """Check if a table exists in the class dataset.

        Args:
            table_name (str): The name of the table.

        Returns:
            bool: True if table exists, False otherwise.
        """

        try:
            full_table_id = f'{self.project}.{self.dataset_name}.{table_name}'
            self.client.get_table(full_table_id)
            return True
        except NotFound:
            return False

    def get_dataset_tables(self):
        """List tables in the class dataset.

        Returns:
            List[str]:
            A list with the name of the tables present in the class dataset.
        """

        tables = self.client.list_tables(self.dataset_name)
        tables_list = []

        for table in tables:
            tables_list.append(table.table_id)

        return tables_list

    def copy_table(
            self,
            source_table_name,
            destination_table_name,
            destination_dataset_name,
            write_disposition='WRITE_APPEND'
    ):
        """Make a copy of a table from the class dataset.

        Args:
            source_table_name (str): The table name from which data is to be loaded.
            destination_table_name (str): The table name into which data is to be loaded.
            destination_dataset_name (str): The dataset name into which data is to be loaded.
            write_disposition (str): Action that occurs if the destination table already exists.
        See
            https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationTableCopy.FIELDS.write_disposition
        """

        source_table_ref = self.dataset.table(source_table_name)
        destination_table = self.client.get_table(
            f'{self.project}.{destination_dataset_name}.{destination_table_name}'
        )

        job_config = bigquery.CopyJobConfig()
        job_config.write_disposition = write_disposition
        job = self.client.copy_table(sources=source_table_ref,
                                     destination=destination_table,
                                     job_config=job_config)
        job.result()
        assert job.state == 'DONE'

        print(f'Table {source_table_ref.table_id} copied to {destination_table.table_id}')

    def _copy_table(self, source_table_ref, destination_table_ref):
        """Make a copy of a table in the class dataset.

        Args:
            source_table_ref (google.cloud.bigquery.table.TableReference):
            destination_table_ref (google.cloud.bigquery.table.TableReference):
        """

        job_config = bigquery.CopyJobConfig()
        job_config.write_disposition = 'WRITE_TRUNCATE'
        job = self.client.copy_table(sources=source_table_ref,
                                     destination=destination_table_ref,
                                     job_config=job_config)

        job.result()
        assert job.state == 'DONE'

        destination_table = self.client.get_table(destination_table_ref)
        source_table = self.client.get_table(source_table_ref)

        assert destination_table.num_rows == source_table.num_rows  # validation 2

        print(f'Table {source_table.table_id} copied to {destination_table.table_id}')

        return destination_table

    def copy_dataset(self, dataset_name_to, add_date=True):

        dataset_to = self.client.create_dataset(dataset=dataset_name_to, exists_ok=True)

        for table in self.client.list_tables(dataset=self.dataset):

            source_table_ref = table.reference
            if add_date:
                today = str(date.today()).replace('-', '_')
                destination_table_id = f'{source_table_ref.table_id}_{today}'
            else:
                destination_table_id = f'{source_table_ref.table_id}'
            destination_table_ref = dataset_to.table(destination_table_id)

            self._copy_table(source_table_ref, destination_table_ref)

        print(f'Dataset {self.dataset_name} copied to {dataset_name_to}')

        return dataset_to

    def delete_dataset(self, delete_contents=True):
        """Delete the class dataset.

        Args
            delete_contents (boolean):
                (Optional) If True, delete all the tables in the dataset. If
                False and the dataset contains tables, the request will fail.
                Default is True.
        """
        self.client.delete_dataset(self.dataset, delete_contents)
        print(f'Deleted dataset {self.dataset.full_dataset_id}')

    def delete_table(self, table_name):
        table_ref = self.dataset.table(table_name)
        self.client.delete_table(table_ref)
        print('Deleted table {}:{}.{}'.format(table_ref.project,
                                              table_ref.dataset_id,
                                              table_ref.table_id))

    def delete_rows(self, table_name, rows_to_delete):
        if rows_to_delete:

            try:

                table_ref = self.dataset.table(table_name)
                table = self.client.get_table(table_ref)
                ids_to_delete = [str(row['id']) for row in rows_to_delete]
                ids_to_delete = ','.join(ids_to_delete)

                sql_delete_query = f"""
                DELETE FROM {table.dataset_id}.{table.table_id}
                WHERE id IN ({ids_to_delete})
                """

                query_job = self.client.query(sql_delete_query)  # API request
                query_job.result()
                print(sql_delete_query)
                print('num_dml_affected_rows:', query_job.num_dml_affected_rows)
                return query_job

            except BadRequest as error:

                print(error)
                print('NOT deleted:', len(rows_to_delete))
                return error

        else:

            print('No rows to delete.')

    def run_query(self, sql):
        """Run a SQL query, wait for it to complete and get the result.

        Args:
            sql (str):
                SQL query to be executed. Defaults to the standard SQL
                dialect. Use the ``job_config`` parameter to change dialects.

        Returns:
            google.cloud.bigquery.table.RowIterator:
                Iterator of row data :class:`~google.cloud.bigquery.table.Row`.
                The iterator will have the ``total_rows`` attribute set, which
                counts the total number of rows **in the result set**.

        Raises:
            google.cloud.exceptions.GoogleCloudError:
                If the job failed.
            concurrent.futures.TimeoutError:
                If the job did not complete in the given timeout.
        """
        query_job = self.client.query(sql)
        rows = query_job.result()
        return rows

    def get_table_schema(self, table_name):
        """Get table schema.

        Args:
            table_name (str): The name of the table to get the schema.

        Returns:
            List [google.cloud.bigquery.schema.SchemaField]:
                The table’s schema in BigQuery format.
            None: If table does not exist.
        """
        table_ref = self.dataset.table(table_name)
        try:
            schema = self.client.get_table(table_ref).schema
        except NotFound:
            return []

        return schema

    def add_column_in_table(self, table_name, new_column_name, new_column_type='STRING'):
        """Add an empty column to an existing table.

        Args:
            table_name (str): The name of the table to add the column in schema.
            new_column_name (str): The name of the field to be added.
            new_column_type (str): The type of the field to be added.
        """

        table_ref = self.dataset.table(table_name)
        table = self.client.get_table(table_ref)  # Make an API request.

        original_schema = table.schema
        new_schema = original_schema[:]  # Creates a copy of the schema.
        new_schema.append(bigquery.SchemaField(new_column_name, new_column_type))

        table.schema = new_schema
        table = self.client.update_table(table, ["schema"])  # Make an API request.

        if len(table.schema) == len(original_schema) + 1 == len(new_schema):
            print(f"Column '{new_column_name}' has been added in table {table_name}.")
        else:
            print("The column has not been added.")
