import argparse
import os
import csv
import yaml
from exp import syn_exp
from datetime import datetime
from os import makedirs
from os.path import exists

if __name__ == '__main__':
    start_ts = datetime.now()
    parser = argparse.ArgumentParser(description='argparse for synthetic test')
    parser.add_argument('--config', '-c', type=str, help='config yaml file path', default='cfg.yaml')

    args = parser.parse_args()

    makedirs("res", exist_ok=True)

    with open('cfg/jihwan.yaml', 'r') as f:
        config_dict = yaml.load(f, Loader=yaml.FullLoader)
    exp_param = {
        "dag_num" : config_dict["dag_num"],
        "instance_num" : config_dict["instance_num"],
        "core_num" : config_dict["core_num"],
        "node_num" : config_dict["node_num"],
        "depth" : config_dict["depth"],
        "exec_t" : config_dict["exec_t"],
        "backup_ratio" : config_dict["backup_ratio"],
        "sl_unit" : config_dict["sl_unit"],
        "sl_exp" : config_dict["sl_exp"],
        "acceptance" : config_dict["acceptance_threshold"],
        "base" : config_dict["baseline"],
        "dangling" : config_dict["dangling_ratio"]
    }
    with open('res/jihwan.csv', 'w', newline='') as f:
        wr = csv.writer(f)
        wr.writerow(['density','jw_budget', 'jh_budget', 'jw_core', 'jh_core'])

        for i in range(6, 9):
            exp_param["depth"]=[i, 1]
            wr.writerow(["core num : "+str(exp_param["core_num"]) , "node num : "+str(exp_param["node_num"]), "depth : "+str(exp_param["depth"])])
            print(exp_param["core_num"], exp_param["node_num"], exp_param["depth"])
            for d in range(config_dict["density_range"][0], config_dict["density_range"][1], config_dict["density_range"][2]):
                d_f = round(d / 100, 2)
                print('Density %f start' % d_f)
                exp_param["density"] = d_f
                exp_param["sl_std"] = config_dict["sl_std"]
                jw, jh, core = syn_exp(**exp_param)
                wr.writerow([d_f, jw, jh, exp_param["core_num"], core])

        for i in range(1, 4):
            exp_param["depth"]=[7, 1]
            exp_param["dangling_ratio"]=i/10
            wr.writerow(["core num : "+str(exp_param["core_num"]) , "node num : "+str(exp_param["node_num"]), "depth : "+str(exp_param["depth"])])
            print(exp_param["core_num"], exp_param["node_num"], exp_param["depth"])
            for d in range(config_dict["density_range"][0], config_dict["density_range"][1], config_dict["density_range"][2]):
                d_f = round(d / 100, 2)
                print('Density %f start' % d_f)
                exp_param["density"] = d_f
                exp_param["sl_std"] = config_dict["sl_std"]
                jw, jh, core = syn_exp(**exp_param)
                wr.writerow([d_f, jw, jh, exp_param["core_num"], core])

    end_ts = datetime.now()
    print('[System] Execution time : %s' % str(end_ts - start_ts))