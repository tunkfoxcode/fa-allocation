from google.cloud import bigquery
from google.oauth2 import service_account


class BigQueryConnector:
    def __init__(self, credentials_path=None, project_id=None):
        """
        Khởi tạo kết nối tới Google BigQuery

        Args:
            credentials_path: Đường dẫn tới file JSON service account key
            project_id: ID của Google Cloud Project
        """
        if credentials_path:
            credentials = service_account.Credentials.from_service_account_file(
                credentials_path,
                scopes=["https://www.googleapis.com/auth/bigquery"]
            )
            self.client = bigquery.Client(credentials=credentials, project=project_id)
        else:
            self.client = bigquery.Client(project=project_id)

        print(f"✓ Đã kết nối thành công tới BigQuery project: {self.client.project}")

    def execute_query(self, query):
        """
        Thực thi query và trả về kết quả

        Args:
            query: SQL query string

        Returns:
            DataFrame chứa kết quả query
        """
        try:
            query_job = self.client.query(query)
            results = query_job.result()
            df = results.to_dataframe()
            return df
        except Exception as e:
            print(f"✗ Error when executing query: {str(e)}")
            raise

    def list_datasets(self):
        """Liệt kê tất cả datasets trong project"""
        datasets = list(self.client.list_datasets())
        if datasets:
            print(f"Datasets trong project {self.client.project}:")
            for dataset in datasets:
                print(f"  - {dataset.dataset_id}")
        else:
            print(f"Project {self.client.project} không có dataset nào")
        return datasets

    def list_tables(self, dataset_id):
        """Liệt kê tất cả tables trong một dataset"""
        tables = list(self.client.list_tables(dataset_id))
        if tables:
            print(f"Tables trong dataset {dataset_id}:")
            for table in tables:
                print(f"  - {table.table_id}")
        else:
            print(f"Dataset {dataset_id} không có table nào")
        return tables

    def get_table_schema(self, dataset_id, table_id):
        """Lấy schema của một table"""
        table_ref = f"{self.client.project}.{dataset_id}.{table_id}"
        table = self.client.get_table(table_ref)
        print(f"Schema của table {table_ref}:")
        for field in table.schema:
            print(f"  - {field.name}: {field.field_type}")
        return table.schema

    def insert_row(self, dataset_id, table_id, row_data):
        """
        Insert một row vào BigQuery table

        Args:
            dataset_id: Dataset ID
            table_id: Table ID
            row_data: Dictionary hoặc dataclass instance chứa dữ liệu cần insert

        Returns:
            True nếu insert thành công, False nếu có lỗi
        """
        try:
            from decimal import Decimal

            table_ref = f"{self.client.project}.{dataset_id}.{table_id}"
            table = self.client.get_table(table_ref)

            if hasattr(row_data, '__dataclass_fields__'):
                from dataclasses import asdict
                row_dict = asdict(row_data)
            else:
                row_dict = row_data

            def convert_decimals(obj):
                from datetime import datetime, date
                if isinstance(obj, dict):
                    return {k: convert_decimals(v) for k, v in obj.items()}
                elif isinstance(obj, list):
                    return [convert_decimals(item) for item in obj]
                elif isinstance(obj, Decimal):
                    return round(float(obj), 9)
                elif isinstance(obj, float):
                    return round(obj, 9)
                elif isinstance(obj, datetime):
                    return obj.isoformat()
                elif isinstance(obj, date):
                    return obj.isoformat()
                else:
                    return obj

            row_dict = convert_decimals(row_dict)

            errors = self.client.insert_rows_json(table, [row_dict])

            if errors:
                print(f"✗ Errors occurred while inserting row: {errors}")
                return False
            else:
                print(f"✓ Successfully inserted 1 row into {table_ref}")
                return True

        except Exception as e:
            print(f"✗ Error when inserting row: {str(e)}")
            return False
