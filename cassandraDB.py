from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy


# connect to the ks
def connect_key_space():
    ster = Cluster(contact_points=['127.0.0.1'],
                   port=9042,
                   load_balancing_policy=RoundRobinPolicy())
    # keyspace name
    keyspacename = "mykeyspace3"
    session = ster.connect(keyspace=keyspacename)
    return session

def exist_table(table_name, keyspacename):
    '''the table exists or not'''
    ster = Cluster(contact_points=['127.0.0.1'],
                   port=9042,
                   load_balancing_policy=RoundRobinPolicy())
    session = ster.connect()
    print(ster.metadata.keyspaces[keyspacename].tables.keys())
    session.shutdown()
    if table_name in ster.metadata.keyspaces[keyspacename].tables.keys():
        return True
    return False

def exist_keyspace(keyspace):
    '''the keyspace exists or not'''
    ster = Cluster(contact_points=['127.0.0.1'],
                   port=9042,
                   load_balancing_policy=RoundRobinPolicy())
    session = ster.connect()
    print(ster.metadata.keyspaces.keys())
    session.shutdown()
    if keyspace in ster.metadata.keyspaces.keys():
        return True
    return False

def keyandspace():
    ster = Cluster(contact_points=['127.0.0.1'],
                   port=9042,
                   load_balancing_policy=RoundRobinPolicy())
    session = ster.connect()
    if not exist_keyspace("mykeyspace3"):
        session.execute("CREATE KEYSPACE mykeyspace3 WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 3};")
    if not exist_table("mytable3", "mykeyspace3"):
        session.execute('create table mykeyspace3.mytable3(inserttime varchar,predictres varchar,filepath varchar primary key);')
    session.shutdown()

def insert_data(filepath, crtime, predictres):
    '''insert data'''
    keyandspace()
    session = connect_key_space()
    print(session)

    sql = 'insert into mytable3(filepath,inserttime,predictres) values(%s, %s, %s)'
    session.execute(sql, (filepath, crtime, predictres))
    print(sql)
    session.shutdown()

def showdata():
    # search data
    keyandspace()
    session = connect_key_space()
    sql = 'select * from mytable3'
    rs = session.execute(sql)
    session.shutdown()
    return rs

# test if the data have been inserted
rr = showdata()
print(rr.current_rows)