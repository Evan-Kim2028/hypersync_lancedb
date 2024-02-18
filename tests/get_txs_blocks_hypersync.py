import asyncio
import hypersync
from hypersync import BlockField, TransactionField
import time


QUERY = hypersync.Query(
    from_block=18000000,
    to_block=18100000,
    include_all_blocks=True,
    field_selection=hypersync.FieldSelection(
        block=[
            BlockField.NUMBER,
            BlockField.TIMESTAMP,
            BlockField.HASH,
            BlockField.GAS_USED,
        ],
        transaction=[
            TransactionField.BLOCK_NUMBER,
            TransactionField.TRANSACTION_INDEX,
            TransactionField.HASH,
            TransactionField.FROM,
            TransactionField.TO,
            TransactionField.VALUE,
            TransactionField.INPUT,
            TransactionField.GAS,
            TransactionField.GAS_PRICE,
            TransactionField.MAX_PRIORITY_FEE_PER_GAS,
            TransactionField.MAX_FEE_PER_GAS,
            TransactionField.TYPE
        ]
    )
)


async def query_txs_blocks():
    client = hypersync.HypersyncClient()
    start_time = time.time()

    await client.create_parquet_folder(
        QUERY, hypersync.ParquetConfig(
            path="data",
            retry=True,
            hex_output=True,
        )
    )
    execution_time = (time.time() - start_time)
    print(f"create_parquet_folder time: {execution_time}")


async def main():
    await query_txs_blocks()


asyncio.run(main())

print('done')
