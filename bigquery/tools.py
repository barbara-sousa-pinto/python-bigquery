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