# NRDtoParquetFrames
Ninjatrader addon that converts NRD files to Parquet 10ms frames with 10 depth.

Requires ParquetSharp dlls to be placed in NT bin/custom folder, use 471 net.

Default Parallel Thread Count is 2, I would recommend using 4 or 8 if you can afford it.

Place this file in /bin/custom/addons, then compile in NT editor
