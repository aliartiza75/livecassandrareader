import sys
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
import time


class LiveCassandraReader():

    def __init__(self, configurations):
        '''
        Initializations will be done here
        '''
        # config vars
        self.count = 0
        self.cass_addr = configurations["cass_addr"]
        self.keyspace_name = configurations["keyspace_name"]
        self.query = configurations["query"]
        self.protocol_version = configurations['protocol_version']
        self.retry = configurations["retry"]
        self.retry_time = configurations["retry_time"]
        self.default_fetch_size = configurations["default_fetch_size"]
        # connection to the database
        self.cluster = Cluster(self.cass_addr.split(','), protocol_version=self.protocol_version)
        self.session = self.cluster.connect(self.keyspace_name)
        self.session.default_fetch_size = self.default_fetch_size
        self.page_state = None
        # application start
        self.run()

    def fetch_data(self, query=None):
        '''
        It will fetch the data from the cassandra table
        '''
        if self.page_state is None:
            result_set = self.session.execute(self.query)
        else:
            result_set = self.session.execute(self.query, paging_state=self.page_state)

        while(result_set.has_more_pages):
            for row in result_set.current_rows:
                ##############
                print(row)    # Process the row here
                ##############
                self.count += 1
            self.page_state = result_set.paging_state
            result_set = self.session.execute(self.query, paging_state=self.page_state)

    def retry_fetch_data(self):
        '''
        It will retry for self.retry times after self.retry_time(seconds) in order to
        detect the new row interted inside cassandra
        '''
        for _ in range(0, self.retry):
            result_set = self.session.execute(self.query, paging_state=self.page_state)
            if result_set.has_more_pages:
                return True
            time.sleep(self.retry_time)
        sys.exit(1)

    def run(self):
        '''
        It is the application starter
        '''
        while True:
            self.fetch_data()
            self.retry_fetch_data()


# configurations for the data
configurations = {
    "cass_addr": "172.17.0.1",
    "keyspace_name": "dataingestion",
    "query": "SELECT * FROM dataingestion.power_source_status4",
    "protocol_version": 4,
    "retry": 5,
    "retry_time": 10,
    "default_fetch_size": 1
}
LiveCassandraReader(configurations)
