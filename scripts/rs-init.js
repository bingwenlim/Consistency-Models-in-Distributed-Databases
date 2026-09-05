// Initializes the 5-node replica set. Run inside mongo1.
// Uses container hostnames so nodes resolve each other over the Docker network.
rs.initiate({
  _id: "rs0",
  members: [
    { _id: 0, host: "mongo1:27017", priority: 2 },
    { _id: 1, host: "mongo2:27017", priority: 1 },
    { _id: 2, host: "mongo3:27017", priority: 1 },
    { _id: 3, host: "mongo4:27017", priority: 1 },
    { _id: 4, host: "mongo5:27017", priority: 1 }
  ]
});
