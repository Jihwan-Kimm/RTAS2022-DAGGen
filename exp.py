import math
from model.dag import export_dag_file, generate_random_dag, generate_backup_dag, generate_from_dict, import_dag_file, cal_lst_eft
from model.cpc import construct_cpc, assign_priority
from sched.fp import calculate_acc, check_acceptance, check_deadline_miss, sched_fp
from sched.classic_budget import classic_budget
from sched.cpc_budget import cpc_budget
import csv
from random import randint
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
import random

def show_def(dag, title):
    fig, ax = plt.subplots()
    for i in range(len(dag.critical_path_point)-1):
        ax.add_patch(Rectangle((dag.critical_path_point[i], 0), dag.critical_path_point[i+1]-dag.critical_path_point[i], 15, fill=False, color='black'))
        ax.text((dag.critical_path_point[i]+dag.critical_path_point[i+1]) / 2, 7, dag.node_set[dag.critical_path[i]].name, ha='center', va='center', color='black')
    for i in range(len(dag.node_set)):
        if i in dag.critical_path: continue
        ax.add_patch(Rectangle((dag.node_set[i].lst, 20+(i-len(dag.critical_path))*15), dag.node_set[i].eft-dag.node_set[i].lst, 15, fill=False, color='black'))
        ax.add_patch(Rectangle((dag.node_set[i].lst, 20+(i-len(dag.critical_path))*15), dag.node_set[i].exec_t, 15, color=dag.node_set[i].color))
        ax.text(dag.node_set[i].lst+20, 27+(i-len(dag.critical_path))*15, dag.node_set[i].name, ha='center', va='center', color='black')
    plt.axis('equal') 
    plt.savefig('res/'+title, dpi=300)
    plt.close()

def show_stretch(dag, title):
    fig, ax = plt.subplots()
    for i in range(1, len(dag.critical_path_point)-2):
        ax.add_patch(Rectangle((dag.critical_path_point[i], 0), dag.critical_path_point[i+1]-dag.critical_path_point[i], 35, fill=False, color='black'))
        ax.text((dag.critical_path_point[i]+dag.critical_path_point[i+1]) / 2, 17, dag.node_set[dag.critical_path[i]].name, ha='center', va='center', color='black')
    space=0
    for i in range(len(dag.node_set)):
        if i in dag.critical_path: 
            space+=1 
            continue
        j=i-space
        ax.add_patch(Rectangle((dag.node_set[i].lst,50+45*j), dag.node_set[i].eft-dag.node_set[i].lst, 45, fill=False, color='black'))
        ax.add_patch(Rectangle((dag.node_set[i].i, 50+45*j), dag.node_set[i].f-dag.node_set[i].i, 45*dag.node_set[i].exec_t/(dag.node_set[i].f-dag.node_set[i].i), color=dag.node_set[i].color))
        ax.text(dag.node_set[i].lst+45, 73+45*j, dag.node_set[i].name, ha='center', va='center', color='black')
    plt.axis('equal') 
    plt.savefig('res/'+title, dpi=300)
    plt.close()


def check_depen(dag):
    for i in range(len(dag.node_set)):
        for j in dag.node_set[i].pred:
            if dag.node_set[i].i<dag.node_set[j].f:
                border=(dag.node_set[i].exec_t*dag.node_set[j].i+dag.node_set[j].exec_t*dag.node_set[i].f)/(dag.node_set[i].exec_t+dag.node_set[j].exec_t)
                if border<dag.node_set[i].lst: border=dag.node_set[i].lst
                if border>dag.node_set[j].eft: border=dag.node_set[j].eft
                dag.node_set[i].i=border
                dag.node_set[j].f=border
                dag.checkpoint.append(border)

        for j in dag.node_set[i].succ:
            if dag.node_set[i].f>dag.node_set[j].i:
                border=(dag.node_set[j].exec_t*dag.node_set[i].i+dag.node_set[i].exec_t*dag.node_set[j].f)/(dag.node_set[i].exec_t+dag.node_set[j].exec_t)
                if border>dag.node_set[i].eft: border=dag.node_set[i].eft
                if border<dag.node_set[j].lst: border=dag.node_set[j].lst                
                dag.node_set[i].f=border
                dag.node_set[j].i=border
                dag.checkpoint.append(border)
    return dag

def check_maxcore(dag, deadline):
    maxcore=-1
    for i in dag.checkpoint:
        core=0
        for j in range(len(dag.node_set)):
            if j in dag.critical_path:
                continue
            if i>dag.node_set[j].i and i<dag.node_set[j].f:
                if (dag.node_set[j].exec_t/(dag.node_set[j].f-dag.node_set[j].i))>1:
                    print(dag.node_set[j].f-dag.node_set[j].i)
                    print(dag.node_set[j].exec_t)
                core+=dag.node_set[j].exec_t/(dag.node_set[j].f-dag.node_set[j].i)
        if math.ceil(core)>maxcore:maxcore=math.ceil(core)
    return maxcore+1

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
    jw_count = 0
    jh_count = 0
    while dag_idx < dag_num:
        ### Make DAG and backup DAG
        normal_dag = generate_random_dag(**dag_param)

        ### Make CPC model and assign priority
        normal_cpc = construct_cpc(normal_dag)

        ### Budget analysis
        # d1=int((exec_t[0] * len(normal_dag.node_set)) / (core_num * density))
        # d2=int((depth[0]+4)*exec_t[0])
        # if d1>d2:
        #     deadline=d1
        # else:
        #     # print("hoho")
        #     deadline=d2
        workload=0
        for i in range(1, len(normal_dag.node_set)-1):
            workload+=normal_dag.node_set[i].exec_t

        deadline=int((workload) / (core_num * density))+normal_dag.node_set[0].exec_t+normal_dag.node_set[-1].exec_t
        normal_dag.dict["deadline"] = deadline

        normal_dag.node_set[normal_dag.sl_node_idx].exec_t = sl_unit
        normal_classic_budget = classic_budget(normal_cpc, deadline, core_num)
        true_deadline=deadline-normal_dag.node_set[0].exec_t-normal_dag.node_set[-1].exec_t

        # If budget is less than 0, DAG is infeasible
        if (not check_deadline_miss(normal_dag, core_num, normal_classic_budget/sl_unit, sl_unit, deadline)) and normal_classic_budget > 0:
            total_jw_budget+=normal_classic_budget/true_deadline
            jw_count+=1
            

        new_budget=deadline-normal_dag.critical_path_point[-1]+normal_dag.node_set[normal_dag.sl_node_idx].eft-normal_dag.node_set[normal_dag.sl_node_idx].lst
        normal_dag.node_set[normal_dag.sl_node_idx].exec_t=new_budget
        
        if new_budget > 0:
            total_jh_budget+=new_budget/true_deadline
            jh_count+=1
            normal_dag=cal_lst_eft(normal_dag)
            normal_dag=check_depen(normal_dag)
            
            total_jh_core+=check_maxcore(normal_dag, deadline)

        dag_idx += 1
    print(density, jw_count, jh_count)
    if jw_count>0:
        jw_budget=total_jw_budget/jw_count
    else:
        jw_budget=0
    if jh_count>0:
        jh_budget=total_jh_budget/jh_count
        jh_core=total_jh_core/jh_count
    else:
        jh_budget=0
        jh_core=0
    # show_stretch(normal_dag, '%f %f.png' % (depth[0], density))
    return jw_budget, jh_budget, jh_core