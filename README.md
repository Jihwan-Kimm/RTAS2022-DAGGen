# DAGGen
Random DAG Generator

## Usage

### Accuracy Experiment

```
python3 main.py --exp acc
```

For `dag_num` = 10,000, 

### Density Experiment

```
python3 main.py --exp density
```

### Visualization

```
python3 main.py --exp acc
```

## Parameter

* `dag_num` (int): set the number of DAGs
* `instance_num` (int): set the number of instances
* `core_num` (int): set the number of cores
* `node_num` ([mean, dev]): set the number of nodes between `[mean-dev, mean+dev]`
* `depth` ([mean, dev]): set the depth of DAG between `[mean-dev, mean+dev]`
* `start_node` ([mean, dev]): set the number of start node (entry node) between `[mean-dev, mean+dev]`
* `exec_t` ([mean, dev]): set the execution time of task between `[mean-dev, mean+dev]`
* `edge_constraint` (bool): If True, use outbound_num as the number of outbound edge. Else, use extra_arc_ratio to 
* `outbound_num` ([mean, dev]): set the number of outbound edge (entry node) between `[mean-dev, mean+dev]`.
* `extra_arc_ratio` (float): make `(task_num) * (extra_arc_ratio)` of extra arc

Deadline of leaf node is set as `level+1 * (exec_t mean) * 2`
