### Getting Started
1. This repository uses rye to manage dependencies and the virtual environment. To install, refer to this link for instructions [here](https://rye-up.com/guide/installation/). 
2. Once rye is installed, run `rye sync` to install dependencies and setup the virtual environment, which has a default name of `.venv`. 
3. Activate the virtual environment with the command `source .venv/bin/activate`.

### Running the Pipeline
There are some script examples in the `examples_local` folder. These examples demonstrate the functionality of writing Hypersync data to a local Lance table. 

* Run `initial_table.py` to create a table from scratch for a given block range. 
* Run `update_table.py` to sync the database to the head of the chain. Assumes existing table exists.
* Run `backfill_table.py` to perform a backfill sync from the earliest block number. Assumes existing table exists.
* Run `hypersync_vanilla.py` as the vanilla Hypersync query that saves the queried data to a parquet file. 