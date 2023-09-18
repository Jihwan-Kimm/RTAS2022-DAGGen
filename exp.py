import math
from model.dag import export_dag_file, generate_random_dag, generate_backup_dag, generate_from_dict, import_dag_file
from model.cpc import construct_cpc, assign_priority
from sched.fp import calculate_acc, check_acceptance, check_deadline_miss, sched_fp
from sched.classic_budget import classic_budget
from sched.cpc_budget import cpc_budget
import csv
from random import randint
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
import random

def check_depen(dag):
    for i in range(len(dag.node_set)):
        for j in dag.node_set[i].pred:
            if dag.node_set[i].i<dag.node_set[j].f:
                border=(dag.node_set[i].exec_t*dag.node_set[j].i+dag.node_set[j].exec_t*dag.node_set[i].f)/(dag.node_set[i].exec_t+dag.node_set[j].exec_t)
                if border<dag.node_set[i].est: border=dag.node_set[i].est
                if border>dag.node_set[j].ltc: border=dag.node_set[j].ltc
                dag.node_set[i].i=border
                dag.node_set[j].f=border

        for j in dag.node_set[i].succ:
            if dag.node_set[i].f>dag.node_set[j].i:
                border=(dag.node_set[j].exec_t*dag.node_set[i].i+dag.node_set[i].exec_t*dag.node_set[j].f)/(dag.node_set[i].exec_t+dag.node_set[j].exec_t)
                if border>dag.node_set[i].ltc: border=dag.node_set[i].ltc
                if border<dag.node_set[j].est: border=dag.node_set[j].est                
                dag.node_set[i].f=border
                dag.node_set[j].i=border
    return dag

def check_maxcore(dag, deadline):
    maxcore=-1
    for i in range(0, deadline, 5):
        core=0
        for j in range(len(dag.node_set)):
            if i>dag.node_set[j].i and i<dag.node_set[j].f:
                core+=1
        if core>maxcore:maxcore=core
    return maxcore

def syn_exp(**kwargs):
    dag_num = kwargs.get('dag_num', 100)
    core_num = kwargs.get('core_num', 4)
    node_num = kwargs.get('node_num', [40,10])
    depth = kwargs.get('depth', [6.5,1.5])
    exec_t = kwargs.get('exec_t', [40,10])
    sl_unit = kwargs.get('sl_unit', 5.0)
    sl_exp = kwargs.get('sl_exp', 30)
    sl_std = kwargs.get('sl_std', 1.0)
    A_acc = kwargs.get('A_acc', 0.95)
    base_loop_count = kwargs.get('base', [100, 200])
    density = kwargs.get('density', 0.3)
    extra_arc_ratio = kwargs.get('extra_arc_ratio', 0.1)
    dangling_ratio = kwargs.get('dangling_ratio', 0.2)

    dag_param = {
        "node_num": node_num,
        "depth": depth,
        "exec_t": exec_t,
        "start_node": [1, 0],
        "end_node": [1, 0],
        "extra_arc_ratio" : extra_arc_ratio,
        "dangling_node_ratio" : dangling_ratio
    }

    total_jw_budget = 0
    total_jh_budget = 0
    total_jh_core= 0

    dag_idx = 0
    failure=0
    while dag_idx < dag_num:
        ### Make DAG and backup DAG
        normal_dag = generate_random_dag(**dag_param)

        ### Make CPC model and assign priority
        normal_cpc = construct_cpc(normal_dag)

        ### Budget analysis
        deadline = int((exec_t[0] * len(normal_dag.node_set)) / (core_num * density))
        normal_dag.dict["deadline"] = deadline

        normal_dag.node_set[normal_dag.sl_node_idx].exec_t = sl_unit
        normal_classic_budget = classic_budget(normal_cpc, deadline, core_num)

        # If budget is less than 0, DAG is infeasible
        if check_deadline_miss(normal_dag, core_num, normal_classic_budget/sl_unit, sl_unit, deadline) or normal_classic_budget <= 0 :
            failure+=1
            continue

        total_jw_budget+=normal_classic_budget

        normal_dag.node_set[normal_dag.sl_node_idx].exec_t=deadline-normal_dag.checkpoint[-1]+normal_dag.node_set[normal_dag.sl_node_idx].ltc-normal_dag.node_set[normal_dag.sl_node_idx].est
        total_jh_budget+=normal_dag.node_set[normal_dag.sl_node_idx].exec_t
        
        normal_dag=check_depen(normal_dag)
        total_jh_core+=check_maxcore(normal_dag, deadline)
        dag_idx += 1
    return total_jw_budget/dag_num, total_jh_budget/dag_num, total_jh_core/dag_num