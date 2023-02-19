# Polar Vortex

Polar Vortex is column store database that uses Polars dataframes and either CSV or Parquet as the underlying storage.
It also provide a shelve interface to store KV pairs.
The database interface and ORM system is written in python and designed in way that
python can be thought of as the underlying query language.
When I say NoSql, I mean no sql(not really).
This is in fact a lie, as of the writing of this Polars has SQL interface
which is intended to for backwards compatibility, however the intended use is
to use the ORMs to interact with database.


