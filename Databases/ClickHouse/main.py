import pandas as pd
import time

from loguru import logger

from clickhouse_db import ClickHouseDB


def create_table():
    """
    Create the nuwa_feedback table in ClickHouse if it doesn't exist.
    """
    try:
        ch_db = ClickHouseDB().get_instance()
        create_table_query = \
            """
                CREATE TABLE IF NOT EXISTS default.nuwa_feedback
                (
                    `id` UInt64,
                    `user_id` Int64,
                    `content_id` Int64,
                    `feedback_type` String,
                    `db_create_time` DateTime64(3),
                    `extra_info` String DEFAULT ''
                )
                ENGINE = MergeTree
                PARTITION BY toYYYYMM(db_create_time)
                ORDER BY (id, db_create_time, user_id, feedback_type)
                SETTINGS index_granularity = 8192
            """
        ch_db.client.execute(create_table_query)
        logger.info("[create_table] Table nuwa_feedback created or already exists.")
    except Exception as e:
        logger.error(f"[create_table] Error creating table: {str(e)}")
        raise

def upload_csv_to_clickhouse(csv_filepath, batch_size=10000):
    """
    Upload data from a CSV file to the nuwa_feedback table in ClickHouse.
    
    Args:
        csv_filepath (str): Path to the CSV file to upload
        batch_size (int): Number of rows to insert in each batch
        
    Returns:
        int: Number of rows uploaded
    """
    try:
        # Initialize ClickHouseDB instance
        ch_db = ClickHouseDB().get_instance()
        
        # Read CSV file
        start_time = time.time()
        logger.info(f"[Clickhouse] Reading CSV file from {csv_filepath}")
        df = pd.read_csv(csv_filepath)
        
        # Pre-process datetime columns in one vectorized operation
        if 'db_create_time' in df.columns:
            # 先解析为 pandas 的 Timestamp
            df['db_create_time'] = pd.to_datetime(df['db_create_time'], utc=False)
            # 去掉任何时区信息，得到“naive” Timestamp
            df['db_create_time'] = df['db_create_time'].dt.tz_localize(None)
            # 转成 Python datetime.datetime 对象
            df['db_create_time'] = df['db_create_time'].dt.to_pydatetime()
        
        # Get column names from the dataframe
        columns = df.columns.tolist()
        columns_str = ', '.join(columns)
        
        # Prepare insert query
        insert_query = f"INSERT INTO default.nuwa_feedback ({columns_str}) VALUES"

        # Process in batches to avoid memory issues
        total_rows = len(df)
        rows_uploaded = 0
        
        for i in range(0, total_rows, batch_size):
            batch_df = df.iloc[i:i+batch_size]
            
            # Convert the batch to a list of tuples (more efficient than iterrows)
            batch_data = list(batch_df.itertuples(index=False, name=None))

            # Execute the insert query
            ch_db.client.execute(insert_query, batch_data)
            
            rows_uploaded += len(batch_data)
            logger.info(f"[upload_csv_to_clickhouse] Uploaded {rows_uploaded}/{total_rows} rows")
        
        elapsed_time = time.time() - start_time
        logger.info(f"[upload_csv_to_clickhouse] Upload completed. {rows_uploaded} "
                   f"rows inserted in {elapsed_time:.2f} seconds")
        
        return rows_uploaded
        
    except Exception as e:
        logger.error(f"[upload_csv_to_clickhouse] Error uploading CSV data: {str(e)}")
        raise


if __name__ == "__main__":
    # create_table()
    upload_csv_to_clickhouse(
        csv_filepath="sample_data/2025_05_16_11_37_30_S202505161125507256_prop.csv"
    )