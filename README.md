# delta-lake-play

A repo for playing with some simple Delta Lake ideas

## delta-api

This is a simple example to show that delta lake and pyspark can be containerised, the table manipulated in pure Python, and exposed via an API (again in pure Python).

The API exposes some core delta functionality, by allowing navigation of versions, and getting the data form those versions (delta time-travel).

The Python manipulations of the delta table follow the DML - https://docs.delta.io/latest/api/python/index.html