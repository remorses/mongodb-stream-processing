# https://gist.github.com/harveyconnor/518e088bad23a273cae6ba7fc4643549
# mongodb://host1,host2/?replicaSet=my-replicaset-name
set -ex 
mongod --config /usr/local/etc/mongod.conf --quiet --bind_ip_all --replSet rs0 &


echo '
rs.initiate(
  {
    _id : "rs0",
    members: [
      { _id : 0, host : "localhost:27017" },
    ]
  }
)


' | mongo