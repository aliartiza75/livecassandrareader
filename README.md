# livecassandrareader

A script to read data from the Cassandra table in real time.

# Problem

The current version of the python-driver reads the data from the Cassandra table till a particular row but what if the table is updating in real-time, so in order to read the new data from a table we have to go through all the rows again.

For example:

A **USER** table have 100 rows in it. So using the script below:
```python
self.session.default_fetch_size = 10
result_set = self.session.execute("SELECT * FROM testKeyspace.USER")
    while(result_set.has_more_pages):
        for row in result_set.current_rows:
            print(row)
        page_state = result_set.paging_state
        result_set = self.session.execute("SELECT * FROM test3rf.test", paging_state=page_state)
```
we will be able to read the cassandra table till 100th. After some time the **USER** table gets updated with two new rows. So in order to read the two new rows we have to go through all the other 100 rows which is inefficient. 


# How to use it

It supports both python2 & 3 version.

Install the python cassandra driver using the command given below:

```
$ pip install cassandra-driver
```

LiveCassandraReader class need the following argument as an input:

```
configurations = {
    "cass_addr": "172.17.0.1",
    "keyspace_name": "dataingestion",
    "query": "SELECT * FROM dataingestion.power_source_status4",
    "protocol_version": 4,
    "retry": 5,
    "retry_time": 10,
    "default_fetch_size": 1
}
```

To run the script"
```
$ python livecassandrareader.py
```
