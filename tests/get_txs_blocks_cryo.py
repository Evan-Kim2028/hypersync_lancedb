import cryo
import time

# Start timing before the function call
start_time = time.time()

output: dict[str] = cryo.freeze(
    "blocks_and_transactions",
    blocks=["1800000:1900000"],
    hex=True,
    rpc="http://91.216.245.128:1131/eth-rpc",
    no_verbose=False,  # this doesn't seem to have any effect
    output_dir="data/raw/",
    subdirs=["datatype"],
    include_columns=["n_rlp_bytes"],
    # exclude_columns=["input", "value"],
    compression=["lz4"],  # bug, can't use zstd in cryo 0.3.0
)

# End timing after the function call
end_time = time.time()

# Calculate the duration
duration = end_time - start_time
print(f"The cryo.freeze function took {duration} seconds to execute")

print('done')
