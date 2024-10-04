# IP Address Counter

Solution of the problem from [this](https://github.com/Ecwid/new-job/blob/master/IP-Addr-Counter-GO.md) repository.

## Algorithm
Main goal is to sort IPs to easily calculate unique addresses by sequentially reading.

### Order of actions
- Split input file of IPs into equal size segments. Count of segments is depends on `ipIteratorCount` configuration.
- Sequentially read data in each segment from beginning into end in different goroutines. Reading is configured via `ipReaderPageSize` and `ipReaderCacheSize`.
	- Prepare `ipIteratorCount` in memory btrees. Values are distributed into btrees linearly. So first btree will hold smallest IP values: between 0 and `MAX_IP_VALUE` / `ipIteratorCount`. Last btree will hold biggest IPs.
	- Read and insert `elementsToRead` count of IP into each btree.
	- If btree is filled, write it's data in ascending order into on-disk array.
	- repeat until end of segment.
- When all segments are read and written into sorted arrays it's time to read them and calculate unique count of IPs.
	- Arrays of single segment must be read at the same time from smallest from all over arrays to biggest. That logic is implemented in `util.MultiIterator` function.
	- Count of goroutines for previous step is configured via `parallelArrayReaderCount`. And for each array cache size is configured via `arrayIteratorCacheSize`.
	- After reading all arrays of each segment we have unique count of IPs.

## Editable Configurations

Program will use different amount of memory and execute faster or slower depending on configuration values below.

- `ipIteratorCount` - Parallel ip readers count. Each reader processes its own array files. Readers are distributed linearly between ipFile.
- `elementsToRead` - Count of elements to read for each iterator before processing to next stage.
- `ipReaderPageSize` - Min amount of data in bytes for single read operation while reading ipFile.
- `ipReaderCacheSize` - Max count of ip addresses to store cached in memory while reading ipFile.
- `btreeDegree` - Degree of intermediate btrees. More degree - less memory usage but slower insertion.
- `parallelArrayReaderCount` - Count of goroutines reading final array files. Must be less or equal to `ipIteratorCount`
- `arrayIteratorCacheSize` - Count of ips for single read operation when iterating through array
