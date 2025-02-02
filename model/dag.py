import copy
from random import randint, shuffle
import math
import csv

if __name__ == "__main__":
    from models import Node, DAG
else:
    from .models import Node, DAG

# get [mean, stdev] and return mean +- stdev random number
def randuniform(arr):
    if arr[1] < 0:
        raise ValueError('should pass positive stdev : %d, %d' % (arr[0], arr[1]))

    return randint(int(arr[0] - arr[1]), int(arr[0] + arr[1]))

def randarr(arr):
    return arr[randint(0, len(arr)-1)]

def argmax(value_list, index_list=None):
    if index_list is None :
        index_list = list(range(len(value_list)))
    max_index, max_value = index_list[0], value_list[index_list[0]]
    for i in index_list :
        if value_list[i] > max_value :
            max_index = i
            max_value = value_list[i]
    return max_index

def calc_lst(dag, task_idx):
    if dag.node_set[task_idx].lst == -1 :
        if len(dag.node_set[task_idx].pred)==0 :
            dag.node_set[task_idx].lst=0
            dag.node_set[task_idx].i=0
        else :
            lst=0
            for i in dag.node_set[task_idx].pred:
                if dag.node_set[i].lst == -1:
                    dag=calc_lst(dag, dag.node_set[i].tid)
                if dag.node_set[i].lst+dag.node_set[i].exec_t>lst:
                    lst=dag.node_set[i].lst+dag.node_set[i].exec_t
            dag.node_set[task_idx].lst=lst
            dag.node_set[task_idx].i=lst
    return dag

def calc_eft(dag, task_idx):
    if dag.node_set[task_idx].eft == -1 :
        if len(dag.node_set[task_idx].succ)==0 :
            dag.node_set[task_idx].eft=dag.critical_path_point[-1]
            dag.node_set[task_idx].f=dag.critical_path_point[-1]
        else :
            eft=dag.critical_path_point[-1]
            for i in dag.node_set[task_idx].succ:
                if dag.node_set[i].eft == -1:
                    dag=calc_eft(dag, dag.node_set[i].tid)
                if dag.node_set[i].eft-dag.node_set[i].exec_t<eft:
                    eft=dag.node_set[i].eft-dag.node_set[i].exec_t
            dag.node_set[task_idx].eft=eft
            dag.node_set[task_idx].f=eft
    return dag

def calculate_critical_path(dag):
    ready_queue = []
    is_complete = [False, ] * len(dag.node_set)
    longest = {}
    max_len = {}

    for node in dag.node_set:
        if len(node.pred) == 0:
            is_complete[node.tid] = True
            longest[node.tid] = [node.tid]
            max_len[node.tid] = node.exec_t

            for succ_idx in node.succ:
                if is_complete[succ_idx]:
                    isReady = False
                else:
                    isReady = True
                for pred_idx in dag.node_set[succ_idx].pred:
                    if not is_complete[pred_idx]:
                        isReady = False
            
                if isReady:
                    ready_queue.append(dag.node_set[succ_idx])
    
    while False in is_complete:
        node = ready_queue.pop()
        longest_idx = node.pred[0]
        longest_len = max_len[node.pred[0]]
        for pred_idx in node.pred[1:]:
            if longest_len < max_len[pred_idx]:
                longest_idx = pred_idx
                longest_len = max_len[pred_idx]
        
        longest[node.tid] = longest[longest_idx] + [node.tid]
        max_len[node.tid] = max_len[longest_idx] + node.exec_t

        is_complete[node.tid] = True

        for succ_idx in node.succ:
            if is_complete[succ_idx]:
                isReady = False
            else:
                isReady = True
            for pred_idx in dag.node_set[succ_idx].pred:
                if not is_complete[pred_idx]:
                    isReady = False
        
            if isReady:
                ready_queue.append(dag.node_set[succ_idx])

    longest_idx = argmax(max_len)
    return longest[longest_idx]

def print_dag(dag):
    for i in dag.node_set:
        print(i.pred, ", ", i.succ)

def generate_random_dag(**kwargs):
    node_num = randuniform(kwargs.get('node_num', [20, 3]))
    depth = randuniform(kwargs.get('depth', [3.5, 0.5]))
    _exec_t = kwargs.get('exec_t', [50.0, 30.0])
    start_node_num = randuniform(kwargs.get('start_node_num', [1, 0]))
    end_node_num = randuniform(kwargs.get('end_node_num', [1, 0]))
    _extra_arc_ratio = kwargs.get('extra_arc_ratio', 0.1)
    _dangling_node_ratio = kwargs.get('dangling_node_ratio', 0.2)

    dag = DAG()

    ### 1. Initialize node
    for i in range(node_num):
        node_param = {
            "name" : "node" + str(i)
        }

        dag.node_set.append(Node(**node_param))

    extra_arc_num = int(node_num * _extra_arc_ratio)
    
    ### 2. Classify node by randomly-select level
    level_arr = []
    for i in range(depth):
        level_arr.append([])

    # assign each level's node number
    level_node_num_arr = [1 for _ in range(depth)]
    level_node_num_arr[0] = start_node_num
    level_node_num_arr[-1] = end_node_num

    while sum(level_node_num_arr) < node_num:
        level = randint(1, depth-2)
        level_node_num_arr[level] += 1
    
    # put nodes in order of index
    idx = 0
    for level in range(depth):
        while len(level_arr[level]) < level_node_num_arr[level]:
            level_arr[level].append(idx)
            dag.node_set[idx].level = level
            idx += 1

    dag.start_node_idx = level_arr[0]

    for node_idx in level_arr[-1]:
        dag.node_set[node_idx].isLeaf = True

    ### 3. Assign critical path, dangling nodes
    # Add edge for critical path
    for level in range(len(level_arr)):
        dag.critical_path.append(randarr(level_arr[level]))
    
    for idx in range(len(dag.critical_path)-1):
        pred_node = dag.node_set[dag.critical_path[idx]]
        succ_node = dag.node_set[dag.critical_path[idx+1]]
        pred_node.succ.append(dag.critical_path[idx+1])
        succ_node.pred.append(dag.critical_path[idx])

    # Assign dangling nodes 

    dangling_node_num = round(node_num * _dangling_node_ratio)
    average_width = math.ceil(node_num / depth)
    dangling_len = math.ceil(dangling_node_num / average_width * 1.5)

    while min(math.ceil(depth/2), depth - dangling_len - 2) < 1:
        dangling_len -= 1

    start_level = randint(1, min(math.ceil(depth/2), depth - dangling_len - 2))
    end_level = start_level + dangling_len

    # Assign self-looping node
    sl_node_idx = dag.critical_path[start_level]
    dag.sl_node_idx = sl_node_idx
    dag.node_set[sl_node_idx].name='node_sl'

    dangling_dag = [sl_node_idx]
    dangling_level_dag = [[sl_node_idx]]
    
    for level in range(start_level+1, end_level+1):
        dangling_dag.append(dag.critical_path[level])
        dangling_level_dag.append([dag.critical_path[level]])

    failCnt = 0
    while len(dangling_dag) < dangling_node_num and failCnt < 10:
        level = randint(start_level+1, end_level)
        new_node = randarr(level_arr[level])
        if new_node not in dangling_level_dag[level - start_level]:
            dangling_dag.append(new_node)
            dangling_level_dag[level - start_level].append(new_node)
            failCnt = 0
        else:
            failCnt += 1

    # Make arc among dangling nodes
    for level in range(dangling_len, 0, -1):
        for succ_idx in dangling_level_dag[level]:
            succ_node = dag.node_set[succ_idx]
            if len(succ_node.pred) == 0:
                pred_idx = randarr(dangling_level_dag[level-1])
                pred_node = dag.node_set[pred_idx]
                succ_node.pred.append(pred_idx)
                pred_node.succ.append(succ_idx)

    dangling_dag.remove(sl_node_idx)
    dag.dangling_idx = dangling_dag

    ### 4. Make arc
    # make arc from last level
    for level in range(depth-1, 0, -1):
        for node_idx in level_arr[level]:
            if len(dag.node_set[node_idx].pred) == 0 :
                pred_idx = randarr(level_arr[randint(0, level-1)])
                dag.node_set[pred_idx].succ.append(node_idx)
                dag.node_set[node_idx].pred.append(pred_idx)

    # make sure all node have succ except sink node
    for level in range(0, depth-1):
        for node_idx in level_arr[level]:
            if len(dag.node_set[node_idx].succ) == 0 :
                randnum=randint(level+1, depth-1)
                succ_idx = level_arr[randnum][randint(0, len(level_arr[randnum])-1)]
                dag.node_set[node_idx].succ.append(succ_idx)
                dag.node_set[succ_idx].pred.append(node_idx)

    ### 5. Make critical path's length longest
    exec_t_arr = [randuniform(_exec_t) for _ in range(node_num)]
    exec_t_arr.sort()
    critical_list = [i for i in range(node_num) if i in dag.critical_path]
    non_critical_list = [i for i in range(node_num) if i not in dag.critical_path]
    shuffle(critical_list)
    shuffle(non_critical_list)

    for i in range(node_num):
        dag.node_set[(non_critical_list + critical_list)[i]].exec_t = exec_t_arr[i]

    # sort index
    for node in dag.node_set:
        node.succ.sort()
        node.pred.sort()

    ### 5. Saving DAG info
    # dag.dict["isBackup"] = False
    # dag.dict["node_num"] = node_num
    # dag.dict["start_node_idx"] = dag.start_node_idx
    # dag.dict["sl_node_idx"] = dag.sl_node_idx
    # dag.dict["dangling_idx"] = dag.dangling_idx
    # dag.dict["critical_path"] = dag.critical_path
    # dag.dict["exec_t"] = [node.exec_t for node in dag.node_set]
    # adj_matrix = []
    
    # for node in dag.node_set:
    #     adj_row = [0 for _ in range(node_num)]
    #     for succ_idx in node.succ:
    #         adj_row[succ_idx] = 1
        
    #     adj_matrix.append(adj_row)
    
    # dag.dict["adj_matrix"] = adj_matrix
    
    dag=cal_lst_eft(dag)

    return dag

def cal_lst_eft(dag):
    dag.node_lst=[]
    dag.critical_path=[]
    dag.critical_path_point=[]
    for i in dag.node_set:
        i.lst=-1
        i.eft=-1

    node_num=len(dag.node_set)
    for i in range(node_num):
        dag=calc_lst(dag, i)
    max_lst=0
    max_lst_i=0
    for i in range(node_num):
        dag.node_lst.append(dag.node_set[i].lst)
        if dag.node_lst[i]+dag.node_set[i].exec_t>max_lst:
            max_lst=dag.node_lst[i]+dag.node_set[i].exec_t
            max_lst_i=i

    dag.critical_path_point.append(max_lst)
    max_lst-=dag.node_set[max_lst_i].exec_t
    while (max_lst!=0) :
        dag.critical_path.append(max_lst_i)
        for j in dag.node_set[max_lst_i].pred:
            if dag.node_lst[j]+dag.node_set[j].exec_t==max_lst:
                max_lst_i=j
                dag.critical_path_point.append(max_lst)
                max_lst-=dag.node_set[j].exec_t
                break
    dag.critical_path_point.append(0)
    dag.critical_path.append(max_lst_i)
    dag.critical_path_point.reverse()
    dag.checkpoint=copy.deepcopy(dag.critical_path_point)
    dag.critical_path.reverse()

    for i in range(node_num):
        dag=calc_eft(dag, i)
    
    return dag

def generate_from_dict(dict):
    dag = DAG()
    dag.dict = dict
    node_num = dict["node_num"]

    ### 1. Initialize node
    for i in range(node_num):
        node_param = {
            "name" : "node" + str(i),
            "exec_t" : dict["exec_t"][i]
        }

        dag.node_set.append(Node(**node_param))

    dag.start_node_idx = dict["start_node_idx"]
    dag.critical_path = dict["critical_path"]
    dag.sl_node_idx = dict["sl_node_idx"]
    dag.dangling_idx = dict["dangling_idx"]

    for pred_idx in range(node_num):
        for succ_idx in range(node_num):
            if dict["adj_matrix"][pred_idx][succ_idx] == 1:
                dag.node_set[pred_idx].succ.append(succ_idx)
                dag.node_set[succ_idx].pred.append(pred_idx)

    for node in dag.node_set:
        if len(node.succ) == 0:
            node.isLeaf = True

    return dag

def import_dag_file(file):
    dag_dict = {}
    with open(file, 'r', newline='') as f:
        rd = csv.reader(f)

        dag_dict["node_num"] = int(next(rd)[0])
        dag_dict["start_node_idx"] = [int(e) for e in next(rd)]
        dag_dict["sl_node_idx"] = int(next(rd)[0])
        dag_dict["dangling_idx"] = [int(e) for e in next(rd)]
        dag_dict["critical_path"] = [int(e) for e in next(rd)]
        dag_dict["exec_t"] = [float(e) for e in next(rd)]
        dag_dict["deadline"] = int(next(rd)[0])
        dag_dict["backup_exec_t"] = float(next(rd)[0])
        adj = []
        for line in rd:
            adj.append([int(e) for e in line])
        dag_dict["adj_matrix"] = adj

    return dag_dict

def export_dag_file(dag, file_name):
    with open(file_name, 'w', newline='') as f:
        wr = csv.writer(f)
        wr.writerow([dag.dict["node_num"]])
        wr.writerow(dag.dict["start_node_idx"])
        wr.writerow([dag.dict["sl_node_idx"]])
        wr.writerow(dag.dict["dangling_idx"])
        wr.writerow(dag.dict["critical_path"])
        wr.writerow(dag.dict["exec_t"])
        wr.writerow(dag.dict["deadline"])
        wr.writerow(dag.dict["backup_exec_t"])
        wr.writerows(dag.dict["adj_matrix"])

def generate_backup_dag(dict, backup_ratio=0.5):
    backup_dict = {}

    dangling_idx = dict["dangling_idx"]

    # last node is backup_node
    node_list = [i for i in range(dict["node_num"]+1) if i not in dangling_idx]

    # remove cycle node (dangling -> A -> dangling)
    has_cycle = []
    for node_idx in node_list[:-1]:
        exist_outcoming = exist_incoming = False
        for dang_idx in dangling_idx:
            if dict["adj_matrix"][node_idx][dang_idx] == 1:
                exist_outcoming = True
            if dict["adj_matrix"][dang_idx][node_idx] == 1:
                exist_incoming = True
        
        if exist_incoming and exist_outcoming:
            has_cycle.append(node_idx)
    
    for node_idx in has_cycle:
        node_list.remove(node_idx)

    node_num = len(node_list)
    backup_dict["node_num"] = node_num
    backup_dict["start_node_idx"] = dict["start_node_idx"]
    backup_dict["sl_node_idx"] = dict["sl_node_idx"]
    backup_dict["dangling_idx"] = []

    # Make new critical path index
    c_p = []
    for (idx, node_idx) in enumerate(node_list):
        if node_idx == dict["sl_node_idx"]:
            c_p.append(idx)
            c_p.append(len(node_list)-1)
        elif node_idx in dict["critical_path"]:
            c_p.append(idx)

    backup_dict["critical_path"] = c_p

    # Calculate backup node execution time
    backup_exec_t = 0
    if "backup_exec_t" in dict:
        backup_exec_t = dict["backup_exec_t"]
    else:
        for idx in dangling_idx:
            backup_exec_t += dict["exec_t"][idx]
        backup_exec_t = round(backup_exec_t * backup_ratio)

    exec_t_arr = []
    for idx in node_list[:-1]:
        exec_t_arr.append(dict["exec_t"][idx])
    exec_t_arr.append(backup_exec_t)

    backup_dict["exec_t"] = exec_t_arr

    # Regenerate adj matrix
    adj_matrix = []
    for _ in range(node_num):
        adj_matrix.append([0, ] * node_num)

    for pred_idx in range(dict["node_num"]):
        for succ_idx in range(dict["node_num"]):
            if dict["adj_matrix"][pred_idx][succ_idx] == 1:
                new_p_idx = pred_idx
                new_c_idx = succ_idx
                if pred_idx in dangling_idx:
                    new_p_idx = dict["node_num"]
                if succ_idx in dangling_idx:
                    new_c_idx = dict["node_num"]
                
                if new_p_idx != new_c_idx and new_p_idx in node_list and new_c_idx in node_list:
                    i = node_list.index(new_p_idx)
                    j = node_list.index(new_c_idx)

                    # check if there 
                    adj_matrix[i][j] = 1
    
    backup_dict["adj_matrix"] = adj_matrix

    backup_dag = generate_from_dict(backup_dict)
    backup_dag.node_set[backup_dag.sl_node_idx].exec_t *= 10
    backup_dag.critical_path = calculate_critical_path(backup_dag)

    backup_dag.dict["backup_exec_t"] = backup_exec_t

    return backup_dag

if __name__ == "__main__":
    dag_param_1 = {
        "node_num" : [10, 0],
        "depth" : [4.5, 0.5],
        "exec_t" : [50.0, 30.0],
        "start_node" : [1, 0],
        "end_node" : [1, 0],
        "extra_arc_ratio" : 0.1,
        "dangling_node_ratio" : 0.2,
    }

    dag_param_2 = {
        "node_num" : [40, 5],
        "depth" : [5.5, 1.5],
        "exec_t" : [50.0, 30.0],
        "start_node" : [1, 0],
        "end_node" : [1, 0],
        "extra_arc_ratio" : 0.1,
        "dangling_node_ratio" : 0.2,
    }

    dag = generate_random_dag(**dag_param_1)
    dag2 = generate_from_dict(dag.dict)

    export_dag_file(dag2, 'hi.csv')

    dict_from_file = import_dag_file('hi.csv')

    dag3 = generate_from_dict(dict_from_file)

    print(dag)
    print(dag3)

