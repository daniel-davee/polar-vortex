# Polar Vortex

Polar Vortex is column store database that uses Polars dataframes and either CSV or Parquet as the underlying storage.
It also provide a shelve interface to store KV pairs.
The database interface and ORM system is written in python and designed in way that
python can be thought of as the underlying query language.
When I say NoSql, I mean no sql(not really).
This is in fact a lie, as of the writing of this Polars has SQL interface
which is intended to for backwards compatibility, however the intended use is
to use the ORMs to interact with database.

The column data will be saved in CSV/Parquet files stored across cloud storage buckets.
Query will be executed lazily, and will execute in parallel on every file across the buckets.
This repo will be where the interfaces and ORM system will be developed.
But if you would like to use the full database please contact me at
daniel.v.davee+pv@gmail.com,
to setup a meeting.
Since this in early alpha, a lot of the details have not been fleshed out.
But if you use it during this time, since you are help testing it,
you won't be charged while in the alpha or beta;
and when we go live you be given special rates,
and given access to professional support for at least 6 months.
Plus you will able to request features directly from me.
