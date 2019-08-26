# Etcd using joint consensus reconfiguration

This is the repository of a modified **version 3.3.0-rc.0** of Etcd distributed key-value store. The code of this repository was developed in the proceedings of my Master thesis. This repository differentiates to the original in three key points:

* There is added extra feature where user can choose to use **joint consensus reconfiguration** instead of the default one which is single server reconfiguration. In this way, a replica group can instantly be changed and be replaced by a completely different one. The CLI was extended by adding suitable command called reconfiguration. With reconfiguration command, someone can order reconfiguration to the cluster in a joint consensus way. Reconfiguration command syntax is: ***./etcdctl –endpoints=<existing node 1 ip:2379>, <existing node 2 ip:2379>, ...<existing node n ip:2379> reconfiguration <node 1 id>, <node 2 id>, ...<node n id>***.For this command, we only type the ids of the nodes that will participate to the new configuration.
* User can choose to add a learner except for member into an existing cluster. Till now, there was not ability to add learners into a cluster.  Adding learners is a prerequisite for our joint consensus implementation in order to horizontally scale a cluster. Learners are passive nodes that only learn what a quorum of members agrees to and get updated, so as to be ready to take part into joint consensus reconfiguration and finally become members when they will be asked for. Members that are not part of the new configuration finally revert to learners. A learner is added to an existing cluster with **learner add** command whose syntax is: ***./etcdctl –endpoints=endpoint 1, endpoint 2, ... endpoint n learner add learner_name –peer-urls= peer_url***. Learners can also be removed with learner remove command.
* Modified the Etcd benchmark to always find and target the leader node in a replica group, even when that node is not part of the initial configuration. Also modified to record throughput and latency per 1 sec interval for the needs of the evaluation process of the thesis.

Most of the code changes, additions and modifications refer to joint consensus algorithm implementation and mainly took place in etcd/raft package and files such as etcd/raft/raft.go and etcd/raft/log.go.

## Useful links:
 Some links that will help code comprehension <br/>
 [Raft]: https://raft.github.io/ <br/>
 [Raft paper]: https://www.usenix.org/system/files/conference/atc14/atc14-paper-ongaro.pdf <br/>
 [Raft thesis]: https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf <br/>
 [Etcd original repository]: https://github.com/etcd-io/etcd <br/>
 [MSc thesis of Dimitrios Valekardas]: http://olympias.lib.uoi.gr/jspui/handle/123456789/29259 <br/>
