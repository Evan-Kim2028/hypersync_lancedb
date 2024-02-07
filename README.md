### Installation 
Make a virtual environment and then install the dependencies via  `pip install -e .`

### Benchmarks Cryo vs Hypersync
50k block range for both blocks and transactions
Cryo time (hypersync rpc)
- total duration: 582.930 seconds
- total chunks: 100
    - chunks errored:     0 / 100 (0.0%)
    - chunks skipped:     0 / 100 (0.0%)
    - chunks collected: 100 / 100 (100.0%)
- blocks collected: 100,000
    - blocks per second:     171.5
    - blocks per minute:  10,292.8
    - blocks per hour:   617,569.7
    - blocks per day: 14,821,673.7
- rows written: 755,876
The cryo.freeze function took 583.34046626091 seconds to execute
about 1.2gb


Hypersync time - 3 seconds, took about 100mb