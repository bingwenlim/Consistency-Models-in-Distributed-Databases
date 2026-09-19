// Initializes the 5-node replica set. Run inside mongo1.
// Uses container hostnames so nodes resolve each other over the Docker network.
//
// mongo1 = priority 2 -> ALWAYS the primary.
// mongo3 = priority 1  -> the designated failover: when mongo1 is partitioned onto
//                          a minority side, mongo3 becomes primary on the majority side.
// mongo2/4/5 = priority 0 -> can never be elected.
// This makes the rollback experiments deterministic: mongo1 leads the losing side,
// mongo3 leads the winning side.

rs.initiate({
  _id: "rs0",
  members: [
    { _id: 0, host: "mongo1:27017", priority: 2 },
    { _id: 1, host: "mongo2:27017", priority: 0 },
    { _id: 2, host: "mongo3:27017", priority: 1 },
    { _id: 3, host: "mongo4:27017", priority: 0 },
    { _id: 4, host: "mongo5:27017", priority: 0 }
  ]
});
