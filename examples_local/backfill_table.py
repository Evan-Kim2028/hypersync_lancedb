import asyncio
import hypersync
import polars as pl
import time

from hypersync import ColumnMapping, DataType, TransactionField, BlockField
from lancedb_tables.lance_table import LanceTable


async def historical_blocks_txs_sync():
    """
    Use hypersync to query blocks and transactions and write to a LanceDB table. Assumes existence of a previous LanceDB table to
    query for the latest block number to resume querying.
    """
    # hypersync client
    client = hypersync.HypersyncClient("https://eth.hypersync.xyz")

    # Write blocks and transactions data into lancedb tables.
    uri = "data"  # set to a local folder such as data. This is where LanceDB tables will be stored
    blocks_table_name = "blocks"
    txs_table_name = "transactions"
    index: str = "block_number"

    lance_tables = LanceTable()

    # open the database and get the latest block_number
    blocks_table = lance_tables.open_table(
        uri=uri, table=blocks_table_name)

    # set to_block and from_block to query the desired block range.
    from_block: int = 19_800_000
    to_block: int = blocks_table.to_polars().select('block_number').with_columns(
        pl.col('block_number').min()).collect()['block_number'][0]

    # batch size for processing the hypersync query and writing table to lancedb
    db_batch_size: int = 2_500

    while from_block < to_block:
        current_to_block = min(from_block + db_batch_size, to_block)
        print(
            f"Processing blocks {from_block} to {current_to_block}")

        # add +/-1 to the block range because the query is exclusive to the block number
        query = client.preset_query_blocks_and_transactions(
            from_block-1, current_to_block+1)
        # Setting this number lower reduces client sync console error messages.
        query.max_num_transactions = 1_000  # for troubleshooting

        config = hypersync.ParquetConfig(
            path=uri,
            hex_output=True,
            batch_size=250,
            concurrency=10,
            retry=True,
            column_mapping=ColumnMapping(
                transaction={
                    TransactionField.GAS_USED: DataType.FLOAT64,
                    TransactionField.MAX_FEE_PER_BLOB_GAS: DataType.FLOAT64,
                    TransactionField.MAX_PRIORITY_FEE_PER_GAS: DataType.FLOAT64,
                    TransactionField.GAS_PRICE: DataType.FLOAT64,
                    TransactionField.CUMULATIVE_GAS_USED: DataType.FLOAT64,
                    TransactionField.EFFECTIVE_GAS_PRICE: DataType.FLOAT64,
                    TransactionField.NONCE: DataType.INT64,
                    TransactionField.GAS: DataType.FLOAT64,
                },
                block={
                    BlockField.GAS_LIMIT: DataType.FLOAT64,
                    BlockField.GAS_USED: DataType.FLOAT64,
                    BlockField.SIZE: DataType.FLOAT64,
                    BlockField.BLOB_GAS_USED: DataType.FLOAT64,
                    BlockField.EXCESS_BLOB_GAS: DataType.FLOAT64,
                    BlockField.BASE_FEE_PER_GAS: DataType.FLOAT64
                },
            )
        )

        await client.create_parquet_folder(query, config)
        from_block = current_to_block  # Update from_block for the next batch

        # Write blocks and transactions data into lancedb tables.
        blocks_table_name = "blocks"
        txs_table_name = "transactions"
        index: str = "block_number"

        # load the dataframe into a polars dataframe and insert into lancedb
        blocks_df = pl.read_parquet(
            f"data/{blocks_table_name}.parquet").rename({'number': 'block_number'})
        txs_df = pl.read_parquet(f"data/{txs_table_name}.parquet")

        lance_tables = LanceTable()

        # write blocks
        lance_tables.write_table(uri=uri, table=blocks_table_name, data=blocks_df, merge_on=index
                                 )

        # write txs
        lance_tables.write_table(uri=uri, table=txs_table_name, data=txs_df, merge_on=index
                                 )


start_time = time.time()
asyncio.run(historical_blocks_txs_sync())
end_time = time.time()

print(f"Time taken: {end_time - start_time}")
