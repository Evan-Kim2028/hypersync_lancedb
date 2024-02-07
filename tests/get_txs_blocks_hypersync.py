import asyncio
import hypersync
import time  # Import the time module


async def main():
    # Create hypersync client using the mainnet hypersync endpoint
    client = hypersync.hypersync_client(
        "https://eth.hypersync.xyz",
    )

    height = await client.get_height()
    print("Height:", height)

    query = {
        # start from block 0 and go to the end of the chain (we don't specify a toBlock).
        "from_block": 1800000,
        "to_block": 1900000,
        "block": [{}],
        "transactions": [{}],

        # Select the fields we are interested in
        "field_selection": {
            "block": ["number", "timestamp", "hash"],
            "transaction": [
                "block_number",
                "transaction_index",
                "hash",
                "from",
                "to",
                "value",
                "input",
            ],
        },
    }

    # Start timing before the query
    start_time = time.time()

    # run the query once
    res = await client.send_req(query)
    # print("res: ", res)

    # End timing after the query
    end_time = time.time()

    # Calculate the duration
    duration = end_time - start_time
    print(f"Query took {duration} seconds")

    # Create a parquet folder by running this query and writing the contents to disk
    print("write data to parquet...")
    await client.create_parquet_folder(query, "data")
    print("finished writing parquet folder")

asyncio.run(main())
