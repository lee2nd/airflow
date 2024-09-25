from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import time
import os
from os.path import isfile, join
import numpy as np
import pandas as pd
from bson.binary import Binary
import pickle
import gridfs
from pymongo import MongoClient
import gc


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.today().replace(hour=0, minute=0),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3, # -1 在失敗時將不斷重試直到成功為止
    'retry_delay': timedelta(minutes=3),
}

dag = DAG(
    'L4A_ETL',
    default_args=default_args,
    description='L4A_ETL',
    schedule=timedelta(minutes=20),
)


def process_charge_file(file_path):
    
    filetype = file_path.split('.')[-1]
    product = file_path.split('/')[2][:4]

    if product == "Y136":
        W = 1440
        H = 270
    elif product == "Y173":
        W = 3840
        H = 720
    elif product in ["V160","V161"]:
        W = 720
        H = 540   
    elif product == "Z300":
        W = 2070
        H = 156         
    elif product == "Z123":
        W = 4800
        H = 600

    if filetype in ['f1p','f20n']:
        C=0.002443
    elif filetype in ['f10p','f200n','f10u']:
        C=0.02443  
    elif filetype in ['f100p','f2u']:
        C=0.2443
    elif filetype in ['f1000p','f20u','f20up']:
        C=2.443
    else:
        C=0.2443

    in_f100p_a = np.fromfile(file_path,np.uint8)
    in_f100p_o = in_f100p_a[0::2]
    in_f100p_e = in_f100p_a[1::2]
    out_f100p_a = 0.256*in_f100p_o + 0.001*in_f100p_e
    out_f100p_a = out_f100p_a[4:(W*H)+4]*C*1000 

    chargemap_2d_b = np.reshape(out_f100p_a[0::3],(H,W//3))
    chargemap_2d_g = np.reshape(out_f100p_a[1::3],(H,W//3))
    chargemap_2d_r = np.reshape(out_f100p_a[2::3],(H,W//3))

    return chargemap_2d_b, chargemap_2d_g, chargemap_2d_r


def etl():

    # 選擇 client
    client = MongoClient('mongodb://wma:mamcb1@10.88.26.183:27017')
    # 選擇 Database
    db = client["AT"]
    # 選擇 collection
    collection_charge2d = db["L4A_charge2d"]
    collection_defectinfo = db["L4A_defectinfo"]
    # 選擇 gridfs
    fs_charge2d = gridfs.GridFS(db, collection="L4A_charge2d")
    fs_defectinfo = gridfs.GridFS(db, collection="L4A_defectinfo")
    # 創建 index 避免資料重複塞進資料庫
    collection_charge2d.create_index([("lm_time", 1),
                                        ("eqp_id", 1),
                                        ("op_id", 1),
                                        ("recipe_id", 1),
                                        ("chip_id", 1),
                                        ("chip_pos", 1),
                                        ("ins_cnt", 1),
                                        ("step", 1),
                                        ("charge_type", 1)], 
                                    unique=True)   
    collection_defectinfo.create_index([("lm_time", 1),
                                        ("chip_start_time", 1),
                                        ("chip_end_time", 1),
                                        ("eqp_id", 1),
                                        ("op_id", 1),
                                        ("recipe_id", 1),
                                        ("chip_id", 1),
                                        ("chip_pos", 1),
                                        ("ins_cnt", 1),
                                        ("BIN", 1),
                                        ("judgement", 1)], 
                                    unique=True)  

    for file in os.listdir("../../mnt/disk2/AT_DATA/l4a/file_log"):
        
        file_log_path = os.path.join("../../mnt/disk2/AT_DATA/l4a/file_log", file)
        filename = file_log_path.split("/")[-1]
            
        try:
            filedate = datetime.strptime(filename[9:17], "%Y%m%d")
        except:
            continue
        
        cond_1_1 = (filename[:3] == "UMC")
        cond_1_2 = (len(filename) == 21)
        cond_1_3 = (filedate >= (datetime.now()-timedelta(days=0)).replace(hour=0, minute=0, second=0, microsecond=0))
        
        if all([cond_1_1,cond_1_2,cond_1_3]):
            
            f = open(file_log_path, 'r')
            fileloglines = f.readlines()

            for i in range(1,len(fileloglines)):
                
                filelog = fileloglines[i].split(",")

                # 運行時間檢視
                current_time = datetime.now()
                log_time = datetime.strptime(filelog[0], "%Y/%m/%d %H:%M:%S.%f")
                time_difference = current_time - log_time
                print(log_time)
                
                # if time_difference.total_seconds()/60 <= 30:
                if 1:
                    
                    # 有些 filelog 裡面沒有 op_id
                    if len(filelog) == 11:
                        filelog.insert(2, "")
                        
                    # table schema
                    lm_time = filelog[0]
                    eqp_id = filelog[1]
                    op_id = filelog[2]
                    recipe_id = filelog[3]
                    chip_id = filelog[7]
                    file_name = filelog[9]
                    file_path = filelog[10]
                    file_path = file_path.replace("\\","/")[9:]
                    ins_cnt = filelog[11].replace("\n", "")

                    # dictionary Initialization
                    dict_pp = dict()
                    dict_ds = dict()
                    dict_do = dict()
                    dict_vs = dict()
                    dict_vo = dict()  

                    dict_pci3_1 = dict()
                    dict_pci3_2 = dict()
                    dict_pci3_3 = dict()
                    dict_pci3_4 = dict()
                    dict_pci3_5 = dict()
                    dict_pci3_6 = dict()
                    dict_pci3_7 = dict()
                    dict_pci3_8 = dict()
                    dict_pci3_9 = dict()
                    dict_pci3_10 = dict()
                    dict_pci3_11 = dict()
                    dict_pci3_12 = dict()
                    dict_pci3_13 = dict()
                    dict_pci3_14 = dict()
                    dict_pci3_15 = dict()
                    dict_pci3_16 = dict()
                    dict_pci3_17 = dict()
                    dict_pci3_18 = dict()

                    dict_pci_4 = dict()
                    dict_pci_5 = dict()
                    dict_pci_6 = dict()
                                                            
                    dict_pci_7o = dict()
                    dict_pci_7s = dict()

                    dict_co = dict()   

                    # Y136, Y173, V160, Z300, Z123 的 chip_id 長度為 6,15,17
                    if len(chip_id) in [6,15,17]:
                        
                        # ADR 解檔成 Defect Information
                        if "Adr" in file_name:
                            
                            chip_pos = filelog[9].replace(".Adr","")
                            # 處理路徑檢視
                            print(file_path)
                            
                            f = open(f"../../mnt/disk2/AT_DATA/l4a/{file_path}", 'r')
                            lines = f.readlines()

                            defect_info = []
                            color = {0:'B',1:'R',2:'G'}
                            Defect_code = ""

                            d_no = 0
                            
                            step_s = 0
                            step_pp_s, step_ds_s, step_do_s, step_vs_s, step_vo_s = 0, 0, 0, 0, 0
                            step_pci3_1_s, step_pci3_2_s, step_pci3_3_s, step_pci3_4_s, step_pci3_5_s, step_pci3_6_s, step_pci3_7_s, step_pci3_8_s, step_pci3_9_s = 0, 0, 0, 0, 0, 0, 0, 0, 0
                            step_pci3_10_s, step_pci3_11_s, step_pci3_12_s, step_pci3_13_s, step_pci3_14_s, step_pci3_15_s, step_pci3_16_s, step_pci3_17_s, step_pci3_18_s = 0, 0, 0, 0, 0, 0, 0, 0, 0
                            step_pci_4_s, step_pci_5_s, step_pci_6_s, step_pci_7_s = 0, 0, 0, 0
                            step_co_s = 0

                            step_e = 0
                            step_pp_e, step_ds_e, step_do_e, step_vs_e, step_vo_e = 999999, 999999, 999999, 999999, 999999
                            step_pci3_1_e, step_pci3_2_e, step_pci3_3_e, step_pci3_4_e, step_pci3_5_e, step_pci3_6_e, step_pci3_7_e, step_pci3_8_e, step_pci3_9_e = 999999, 999999, 999999, 999999, 999999, 999999, 999999, 999999, 999999
                            step_pci3_10_e, step_pci3_11_e, step_pci3_12_e, step_pci3_13_e, step_pci3_14_e, step_pci3_15_e, step_pci3_16_e, step_pci3_17_e, step_pci3_18_e = 999999, 999999, 999999, 999999, 999999, 999999, 999999, 999999, 999999
                            step_pci_4_e, step_pci_5_e, step_pci_6_e, step_pci_7_e = 999999, 999999, 999999, 999999
                            step_co_e = 999999

                            for line in lines:

                                d_no += 1
                                line = line.replace('\n','')
                                input_data = line.split('=')
                                
                                if input_data[0] == 'BIN':
                                    BIN = input_data[1]
                                elif input_data[0] == 'START_DATE':
                                    START_DATE = input_data[1]     
                                elif input_data[0] == 'START_TIME':
                                    chip_start_time = START_DATE + " " + input_data[1]
                                elif input_data[0] == 'END_DATE':
                                    END_DATE = input_data[1]     
                                elif input_data[0] == 'END_TIME':
                                    chip_end_time = END_DATE + " " + input_data[1]
                                elif input_data[0] == 'JUDGEMENT':
                                    judgement = input_data[1]                       
                                    
                                if "==Step" in line:

                                    STEP = line.replace("=","")
                                    step_s = d_no
                                    step_e = 0
                                    
                                elif (step_e==0) and ("==END" in line) : 

                                    step_s = 0
                                    step_e = d_no-1

                                elif step_s > 0 and step_e == 0: 

                                    if line[0] == "[":
                                        
                                        Defect_code = line[1:-1]                                     
                                        
                                    elif line[:4] == "(S1:":
                                        
                                        temp = line.split("Val=")
                                        value = temp[1]
                                        temp = temp[0].split("Val=")[0][1:-2].split(",")
                                        if len(temp) == 4:
                                            S1 = temp[0][3:]
                                            S2 = temp[1][3:]
                                            G1 = temp[2][3:]
                                            G2 = temp[3][3:]
                                            for S in range(int(S1),int(S2)+1):
                                                for G in range(int(G1),int(G2)+1):
                                                    if recipe_id[:4] in ["V160","V161"]:
                                                        if S%3 == 0:
                                                            LED_Type = "B"
                                                        elif S%3 == 1:
                                                            LED_Type = "R"
                                                        elif S%3 == 2:
                                                            LED_Type = "G"                                                            
                                                    else:                                            
                                                        LED_Type = color[S%3]
                                                    defect_info.append([STEP,Defect_code,LED_Type,S,G,value])   

                                # 抓 function test - POWER_PIN
                                if "[POWER_PIN]" in line:

                                    step_pp_s = d_no
                                    step_pp_e = 0
                                    
                                elif (step_pp_e==0) and ("TOTAL_CNT" in line) : 

                                    step_pp_s = 0
                                    step_pp_e = d_no-1

                                elif step_pp_s > 0 and step_pp_e == 0: 
                                    
                                    line = line.split("=")[1]
                                    key = line.split(",")[0]
                                    value = line.split(",")[1].split("(")[0].replace(" ","")
                                    if "(OK)" in line:
                                        judge = "OK"
                                    if "(NG)" in line:
                                        judge = "NG"                                        
                                    dict_pp[key] = [value,judge]

                                # 抓 function test - DRIVER_SHORT
                                if "[DRIVER_SHORT]" in line:

                                    step_ds_s = d_no
                                    step_ds_e = 0
                                    
                                elif (step_ds_e==0) and ("TOTAL_CNT" in line) : 

                                    step_ds_s = 0
                                    step_ds_e = d_no-1

                                elif (step_ds_s > 0) and (step_ds_e == 0) and ("_A" in line): 
                                    
                                    key = line.split("<<")[0]
                                    value = line.split(" = ")[1].split(" ")[0]
                                    judge = line.split(" = ")[1].split(" ")[1][1:3]
                                    dict_ds[key] = [value,judge]   

                                # 抓 function test - DRIVER_OPEN
                                if "[DRIVER_OPEN]" in line:

                                    step_do_s = d_no
                                    step_do_e = 0
                                    
                                elif (step_do_e==0) and ("TOTAL_CNT" in line) : 

                                    step_do_s = 0
                                    step_do_e = d_no-1

                                elif (step_do_s > 0) and (step_do_e == 0) and ("_V" in line): 
                                    
                                    key = line.split("<<")[0]
                                    value = line.split(" = ")[1].split(" ")[0]
                                    judge = line.split(" = ")[1].split(" ")[1][1:3]
                                    dict_do[key] = [value,judge]   

                                # 抓 function test - VIDEO_SHORT
                                if "[VIDEO_SHORT]" in line:

                                    step_vs_s = d_no
                                    step_vs_e = 0
                                    
                                elif (step_vs_e==0) and ("TOTAL_CNT" in line) : 

                                    step_vs_s = 0
                                    step_vs_e = d_no-1

                                elif (step_vs_s > 0) and (step_vs_e == 0) and ("_A" in line): 
                                    
                                    key = line.split("_A")[0]
                                    value = line.split(" = ")[1].split("(")[0]
                                    if "(OK)" in line:
                                        judge = "OK"
                                    if "(NG)" in line:
                                        judge = "NG"   
                                        
                                    dict_vs[key] = [value,judge] 

                                # 抓 function test - VIDEO_OPEN
                                if "[VIDEO_OPEN]" in line:

                                    step_vo_s = d_no
                                    step_vo_e = 0
                                    
                                elif (step_vo_e==0) and ("TOTAL_CNT" in line) : 

                                    step_vo_s = 0
                                    step_vo_e = d_no-1

                                elif (step_vo_s > 0) and (step_vo_e == 0) and ("_V " in line): 
                                    
                                    key = line.split("_V ")[0]
                                    value = line.split(" = ")[1].split("(")[0]
                                    if "(OK)" in line:
                                        judge = "OK"
                                    if "(NG)" in line:
                                        judge = "NG"  
                                    dict_vo[key] = [value,judge]            

                                # 抓 function test - POWER_CONSUME_INSPECTION_3_LOOP1
                                if "[POWER_CONSUME_INSPECTION_3_LOOP1]" in line:

                                    step_pci3_1_s = d_no
                                    step_pci3_1_e = 0
                                    
                                elif (step_pci3_1_e==0) and ("TOTAL_CNT" in line) : 

                                    step_pci3_1_s = 0
                                    step_pci3_1_e = d_no-1

                                elif (step_pci3_1_s > 0) and (step_pci3_1_e == 0) and ("_A " in line): 

                                    key = line.split("(")[0]
                                    value = line.split(" = ")[1].split("(")[0]
                                    judge = line.split(" = ")[1].split("(")[1][:2]
                                    dict_pci3_1[key] = [value,judge]    

                                # 抓 function test - POWER_CONSUME_INSPECTION_3_LOOP2
                                if "[POWER_CONSUME_INSPECTION_3_LOOP2]" in line:

                                    step_pci3_2_s = d_no
                                    step_pci3_2_e = 0
                                    
                                elif (step_pci3_2_e==0) and ("TOTAL_CNT" in line) : 

                                    step_pci3_2_s = 0
                                    step_pci3_2_e = d_no-1

                                elif (step_pci3_2_s > 0) and (step_pci3_2_e == 0) and ("_A " in line): 

                                    key = line.split("(")[0]
                                    value = line.split(" = ")[1].split("(")[0]
                                    judge = line.split(" = ")[1].split("(")[1][:2]
                                    dict_pci3_2[key] = [value,judge]  
                                    
                                # 抓 function test - POWER_CONSUME_INSPECTION_3_LOOP3
                                if "[POWER_CONSUME_INSPECTION_3_LOOP3]" in line:

                                    step_pci3_3_s = d_no
                                    step_pci3_3_e = 0
                                    
                                elif (step_pci3_3_e==0) and ("TOTAL_CNT" in line) : 

                                    step_pci3_3_s = 0
                                    step_pci3_3_e = d_no-1

                                elif (step_pci3_3_s > 0) and (step_pci3_3_e == 0) and ("_A " in line): 

                                    key = line.split("(")[0]
                                    value = line.split(" = ")[1].split("(")[0]
                                    judge = line.split(" = ")[1].split("(")[1][:2]
                                    dict_pci3_3[key] = [value,judge]    

                                # 抓 function test - POWER_CONSUME_INSPECTION_3_LOOP4
                                if "[POWER_CONSUME_INSPECTION_3_LOOP4]" in line:

                                    step_pci3_4_s = d_no
                                    step_pci3_4_e = 0
                                    
                                elif (step_pci3_4_e==0) and ("TOTAL_CNT" in line) : 

                                    step_pci3_4_s = 0
                                    step_pci3_4_e = d_no-1

                                elif (step_pci3_4_s > 0) and (step_pci3_4_e == 0) and ("_A " in line): 

                                    key = line.split("(")[0]
                                    value = line.split(" = ")[1].split("(")[0]
                                    judge = line.split(" = ")[1].split("(")[1][:2]
                                    dict_pci3_4[key] = [value,judge]    

                                # 抓 function test - POWER_CONSUME_INSPECTION_3_LOOP5
                                if "[POWER_CONSUME_INSPECTION_3_LOOP5]" in line:

                                    step_pci3_5_s = d_no
                                    step_pci3_5_e = 0
                                    
                                elif (step_pci3_5_e==0) and ("TOTAL_CNT" in line) : 

                                    step_pci3_5_s = 0
                                    step_pci3_5_e = d_no-1

                                elif (step_pci3_5_s > 0) and (step_pci3_5_e == 0) and ("_A " in line): 

                                    key = line.split("(")[0]
                                    value = line.split(" = ")[1].split("(")[0]
                                    judge = line.split(" = ")[1].split("(")[1][:2]
                                    dict_pci3_5[key] = [value,judge]  
                                    
                                # 抓 function test - POWER_CONSUME_INSPECTION_3_LOOP6
                                if "[POWER_CONSUME_INSPECTION_3_LOOP6]" in line:

                                    step_pci3_6_s = d_no
                                    step_pci3_6_e = 0
                                    
                                elif (step_pci3_6_e==0) and ("TOTAL_CNT" in line) : 

                                    step_pci3_6_s = 0
                                    step_pci3_6_e = d_no-1

                                elif (step_pci3_6_s > 0) and (step_pci3_6_e == 0) and ("_A " in line): 

                                    key = line.split("(")[0]
                                    value = line.split(" = ")[1].split("(")[0]
                                    judge = line.split(" = ")[1].split("(")[1][:2]
                                    dict_pci3_6[key] = [value,judge]       

                                # 抓 function test - POWER_CONSUME_INSPECTION_3_LOOP7
                                if "[POWER_CONSUME_INSPECTION_3_LOOP7]" in line:

                                    step_pci3_7_s = d_no
                                    step_pci3_7_e = 0
                                    
                                elif (step_pci3_7_e==0) and ("TOTAL_CNT" in line) : 

                                    step_pci3_7_s = 0
                                    step_pci3_7_e = d_no-1

                                elif (step_pci3_7_s > 0) and (step_pci3_7_e == 0) and ("_A " in line): 

                                    key = line.split("(")[0]
                                    value = line.split(" = ")[1].split("(")[0]
                                    judge = line.split(" = ")[1].split("(")[1][:2]
                                    dict_pci3_7[key] = [value,judge]    

                                # 抓 function test - POWER_CONSUME_INSPECTION_3_LOOP8
                                if "[POWER_CONSUME_INSPECTION_3_LOOP8]" in line:

                                    step_pci3_8_s = d_no
                                    step_pci3_8_e = 0
                                    
                                elif (step_pci3_8_e==0) and ("TOTAL_CNT" in line) : 

                                    step_pci3_8_s = 0
                                    step_pci3_8_e = d_no-1

                                elif (step_pci3_8_s > 0) and (step_pci3_8_e == 0) and ("_A " in line): 

                                    key = line.split("(")[0]
                                    value = line.split(" = ")[1].split("(")[0]
                                    judge = line.split(" = ")[1].split("(")[1][:2]
                                    dict_pci3_8[key] = [value,judge]  
                                    
                                # 抓 function test - POWER_CONSUME_INSPECTION_3_LOOP9
                                if "[POWER_CONSUME_INSPECTION_3_LOOP9]" in line:

                                    step_pci3_9_s = d_no
                                    step_pci3_9_e = 0
                                    
                                elif (step_pci3_9_e==0) and ("TOTAL_CNT" in line) : 

                                    step_pci3_9_s = 0
                                    step_pci3_9_e = d_no-1

                                elif (step_pci3_9_s > 0) and (step_pci3_9_e == 0) and ("_A " in line): 

                                    key = line.split("(")[0]
                                    value = line.split(" = ")[1].split("(")[0]
                                    judge = line.split(" = ")[1].split("(")[1][:2]
                                    dict_pci3_9[key] = [value,judge]    

                                # 抓 function test - POWER_CONSUME_INSPECTION_3_LOOP10
                                if "[POWER_CONSUME_INSPECTION_3_LOOP10]" in line:

                                    step_pci3_10_s = d_no
                                    step_pci3_10_e = 0
                                    
                                elif (step_pci3_10_e==0) and ("TOTAL_CNT" in line) : 

                                    step_pci3_10_s = 0
                                    step_pci3_10_e = d_no-1

                                elif (step_pci3_10_s > 0) and (step_pci3_10_e == 0) and ("_A " in line): 

                                    key = line.split("(")[0]
                                    value = line.split(" = ")[1].split("(")[0]
                                    judge = line.split(" = ")[1].split("(")[1][:2]
                                    dict_pci3_10[key] = [value,judge]    

                                # 抓 function test - POWER_CONSUME_INSPECTION_3_LOOP11
                                if "[POWER_CONSUME_INSPECTION_3_LOOP11]" in line:

                                    step_pci3_11_s = d_no
                                    step_pci3_11_e = 0
                                    
                                elif (step_pci3_11_e==0) and ("TOTAL_CNT" in line) : 

                                    step_pci3_11_s = 0
                                    step_pci3_11_e = d_no-1

                                elif (step_pci3_11_s > 0) and (step_pci3_11_e == 0) and ("_A " in line): 

                                    key = line.split("(")[0]
                                    value = line.split(" = ")[1].split("(")[0]
                                    judge = line.split(" = ")[1].split("(")[1][:2]
                                    dict_pci3_11[key] = [value,judge]  
                                    
                                # 抓 function test - POWER_CONSUME_INSPECTION_3_LOOP12
                                if "[POWER_CONSUME_INSPECTION_3_LOOP12]" in line:

                                    step_pci3_12_s = d_no
                                    step_pci3_12_e = 0
                                    
                                elif (step_pci3_12_e==0) and ("TOTAL_CNT" in line) : 

                                    step_pci3_12_s = 0
                                    step_pci3_12_e = d_no-1

                                elif (step_pci3_12_s > 0) and (step_pci3_12_e == 0) and ("_A " in line): 

                                    key = line.split("(")[0]
                                    value = line.split(" = ")[1].split("(")[0]
                                    judge = line.split(" = ")[1].split("(")[1][:2]
                                    dict_pci3_12[key] = [value,judge]   

                                # 抓 function test - POWER_CONSUME_INSPECTION_3_LOOP13
                                if "[POWER_CONSUME_INSPECTION_3_LOOP13]" in line:

                                    step_pci3_13_s = d_no
                                    step_pci3_13_e = 0
                                    
                                elif (step_pci3_13_e==0) and ("TOTAL_CNT" in line) : 

                                    step_pci3_13_s = 0
                                    step_pci3_13_e = d_no-1

                                elif (step_pci3_13_s > 0) and (step_pci3_13_e == 0) and ("_A " in line): 

                                    key = line.split("(")[0]
                                    value = line.split(" = ")[1].split("(")[0]
                                    judge = line.split(" = ")[1].split("(")[1][:2]
                                    dict_pci3_13[key] = [value,judge]    

                                # 抓 function test - POWER_CONSUME_INSPECTION_3_LOOP14
                                if "[POWER_CONSUME_INSPECTION_3_LOOP14]" in line:

                                    step_pci3_14_s = d_no
                                    step_pci3_14_e = 0
                                    
                                elif (step_pci3_14_e==0) and ("TOTAL_CNT" in line) : 

                                    step_pci3_14_s = 0
                                    step_pci3_14_e = d_no-1

                                elif (step_pci3_14_s > 0) and (step_pci3_14_e == 0) and ("_A " in line): 

                                    key = line.split("(")[0]
                                    value = line.split(" = ")[1].split("(")[0]
                                    judge = line.split(" = ")[1].split("(")[1][:2]
                                    dict_pci3_14[key] = [value,judge]  
                                    
                                # 抓 function test - POWER_CONSUME_INSPECTION_3_LOOP15
                                if "[POWER_CONSUME_INSPECTION_3_LOOP15]" in line:

                                    step_pci3_15_s = d_no
                                    step_pci3_15_e = 0
                                    
                                elif (step_pci3_15_e==0) and ("TOTAL_CNT" in line) : 

                                    step_pci3_15_s = 0
                                    step_pci3_15_e = d_no-1

                                elif (step_pci3_15_s > 0) and (step_pci3_15_e == 0) and ("_A " in line): 

                                    key = line.split("(")[0]
                                    value = line.split(" = ")[1].split("(")[0]
                                    judge = line.split(" = ")[1].split("(")[1][:2]
                                    dict_pci3_15[key] = [value,judge]    

                                # 抓 function test - POWER_CONSUME_INSPECTION_3_LOOP16
                                if "[POWER_CONSUME_INSPECTION_3_LOOP16]" in line:

                                    step_pci3_16_s = d_no
                                    step_pci3_16_e = 0
                                    
                                elif (step_pci3_16_e==0) and ("TOTAL_CNT" in line) : 

                                    step_pci3_16_s = 0
                                    step_pci3_16_e = d_no-1

                                elif (step_pci3_16_s > 0) and (step_pci3_16_e == 0) and ("_A " in line): 

                                    key = line.split("(")[0]
                                    value = line.split(" = ")[1].split("(")[0]
                                    judge = line.split(" = ")[1].split("(")[1][:2]
                                    dict_pci3_16[key] = [value,judge]    

                                # 抓 function test - POWER_CONSUME_INSPECTION_3_LOOP17
                                if "[POWER_CONSUME_INSPECTION_3_LOOP17]" in line:

                                    step_pci3_17_s = d_no
                                    step_pci3_17_e = 0
                                    
                                elif (step_pci3_17_e==0) and ("TOTAL_CNT" in line) : 

                                    step_pci3_17_s = 0
                                    step_pci3_17_e = d_no-1

                                elif (step_pci3_17_s > 0) and (step_pci3_17_e == 0) and ("_A " in line): 

                                    key = line.split("(")[0]
                                    value = line.split(" = ")[1].split("(")[0]
                                    judge = line.split(" = ")[1].split("(")[1][:2]
                                    dict_pci3_17[key] = [value,judge]  
                                    
                                # 抓 function test - POWER_CONSUME_INSPECTION_3_LOOP18
                                if "[POWER_CONSUME_INSPECTION_3_LOOP18]" in line:

                                    step_pci3_18_s = d_no
                                    step_pci3_18_e = 0
                                    
                                elif (step_pci3_18_e==0) and ("TOTAL_CNT" in line) : 

                                    step_pci3_18_s = 0
                                    step_pci3_18_e = d_no-1

                                elif (step_pci3_18_s > 0) and (step_pci3_18_e == 0) and ("_A " in line): 

                                    key = line.split("(")[0]
                                    value = line.split(" = ")[1].split("(")[0]
                                    judge = line.split(" = ")[1].split("(")[1][:2]
                                    dict_pci3_18[key] = [value,judge]                                       

                                # 抓 function test - POWER_CONSUME_INSPECTION_4
                                if "[POWER_CONSUME_INSPECTION_4]" in line:

                                    step_pci_4_s = d_no
                                    step_pci_4_e = 0
                                    
                                elif (step_pci_4_e==0) and ("TOTAL_CNT" in line) : 

                                    step_pci_4_s = 0
                                    step_pci_4_e = d_no-1

                                elif (step_pci_4_s > 0) and (step_pci_4_e == 0) and ("_A " in line): 
                                    
                                    key = line.split("_A ")[0]
                                    value = line.split(" = ")[1].split("(")[0]
                                    if "(OK)" in line:
                                        judge = "OK"
                                    if "(NG)" in line:
                                        judge = "NG"  
                                    dict_pci_4[key] = [value,judge] 

                                # 抓 function test - POWER_CONSUME_INSPECTION_5
                                if "[POWER_CONSUME_INSPECTION_5]" in line:

                                    step_pci_5_s = d_no
                                    step_pci_5_e = 0
                                    
                                elif (step_pci_5_e==0) and ("TOTAL_CNT" in line) : 

                                    step_pci_5_s = 0
                                    step_pci_5_e = d_no-1

                                elif (step_pci_5_s > 0) and (step_pci_5_e == 0) and ("_A " in line): 
                                    
                                    key = line.split("_A ")[0]
                                    value = line.split(" = ")[1].split("(")[0]
                                    if "(OK)" in line:
                                        judge = "OK"
                                    if "(NG)" in line:
                                        judge = "NG"  
                                    dict_pci_5[key] = [value,judge] 

                                # 抓 function test - POWER_CONSUME_INSPECTION_6
                                if "[POWER_CONSUME_INSPECTION_6]" in line:

                                    step_pci_6_s = d_no
                                    step_pci_6_e = 0
                                    
                                elif (step_pci_6_e==0) and ("TOTAL_CNT" in line) : 

                                    step_pci_6_s = 0
                                    step_pci_6_e = d_no-1

                                elif (step_pci_6_s > 0) and (step_pci_6_e == 0) and ("_A " in line): 
                                    
                                    key = line.split("_A ")[0]
                                    value = line.split(" = ")[1].split("(")[0]
                                    if "(OK)" in line:
                                        judge = "OK"
                                    if "(NG)" in line:
                                        judge = "NG"  
                                    dict_pci_6[key] = [value,judge]                 

                                # 抓 function test - POWER_CONSUME_INSPECTION_7_Step1
                                if "[POWER_CONSUME_INSPECTION_7_Step1]" in line:

                                    step_pci_7_s = d_no
                                    step_pci_7_e = 0
                                    
                                elif (step_pci_7_e==0) and ("TOTAL_CNT" in line) : 

                                    step_pci_7_s = 0
                                    step_pci_7_e = d_no-1

                                elif (step_pci_7_s > 0) and (step_pci_7_e == 0): 
                                    
                                    key = line.split("<<")[0]
                                    value1 = line.split(" = ")[1].split(" ")[0]
                                    judge1 = line.split(" = ")[1].split(" ")[1][1:3]
                                    value2 = line.split(" = ")[1].split(",")[2].split(" ")[0]
                                    judge2 = line.split(" = ")[1].split(",")[2].split(" ")[1][1:3]
                                    dict_pci_7o[key] = [value1,judge1]
                                    dict_pci_7s[key] = [value2,judge2]      

                                # 抓 function test - CARRYOUT
                                if "[CARRYOUT]" in line:

                                    step_co_s = d_no
                                    step_co_e = 0
                                    
                                elif (step_co_e==0) and ("TOTAL_CNT" in line) : 

                                    step_co_s = 0
                                    step_co_e = d_no-1

                                elif (step_co_s > 0) and (step_co_e == 0): 
                                    
                                    key = line.split(" = ")[0]
                                    value = line.split(" = ")[1].split(" ")[0]
                                    judge = line.split(" = ")[1].split(" ")[1][1:3]
                                    dict_co[key] = [value,judge]

                            df_defect = pd.DataFrame(data=defect_info, columns=['Step','Defect_code','LED_Type','Source','Gate','Value'])
                            df_defect = fs_defectinfo.put(Binary(pickle.dumps(df_defect, protocol=5))) 
                                                                
                            # assign whole chip information
                            table_schema = {'lm_time': lm_time,
                                            'chip_start_time': chip_start_time,
                                            'chip_end_time': chip_end_time,
                                            'eqp_id': eqp_id,
                                            'op_id': op_id,
                                            'recipe_id': recipe_id,
                                            'chip_id': chip_id,
                                            'chip_pos': chip_pos,
                                            'ins_cnt': ins_cnt,
                                            'BIN': BIN,
                                            'judgement': judgement,
                                            'df_defect': df_defect,
                                            'power_pin': dict_pp,
                                            'driver_short': dict_ds, 
                                            'driver_open': dict_do, 
                                            'video_short': dict_vs, 
                                            'video_open': dict_vo,
                                            'power_consume_inspection_3_loop1': dict_pci3_1,
                                            'power_consume_inspection_3_loop2': dict_pci3_2,
                                            'power_consume_inspection_3_loop3': dict_pci3_3,
                                            'power_consume_inspection_3_loop4': dict_pci3_4,
                                            'power_consume_inspection_3_loop5': dict_pci3_5,
                                            'power_consume_inspection_3_loop6': dict_pci3_6,
                                            'power_consume_inspection_3_loop7': dict_pci3_7,
                                            'power_consume_inspection_3_loop8': dict_pci3_8,
                                            'power_consume_inspection_3_loop9': dict_pci3_9,
                                            'power_consume_inspection_3_loop10': dict_pci3_10,
                                            'power_consume_inspection_3_loop11': dict_pci3_11,
                                            'power_consume_inspection_3_loop12': dict_pci3_12,
                                            'power_consume_inspection_3_loop13': dict_pci3_13,
                                            'power_consume_inspection_3_loop14': dict_pci3_14,
                                            'power_consume_inspection_3_loop15': dict_pci3_15,
                                            'power_consume_inspection_3_loop16': dict_pci3_16,
                                            'power_consume_inspection_3_loop17': dict_pci3_17,
                                            'power_consume_inspection_3_loop18': dict_pci3_18,   
                                            'power_consume_inspection_4': dict_pci_4,
                                            'power_consume_inspection_5': dict_pci_5,
                                            'power_consume_inspection_6': dict_pci_6,
                                            'power_consume_inspection_7_opentest': dict_pci_7o,
                                            'power_consume_inspection_7_shorttest': dict_pci_7s,
                                            'carryout': dict_co,                                            
                                            'file_path': file_path
                                            }
                                
                            try:
                                collection_defectinfo.insert_one(table_schema)
                                del df_defect
                                gc.collect()                               
                            except:
                                # db 內本來就有資料
                                pass
                                
                        # Chargemap 2d 解檔
                        elif "Step" in file_name:
                            
                            chip_pos = filelog[9].split("_")[0]
                            step = filelog[9].split("\\")[-1].split("_")[1] + "_" + filelog[9].split("\\")[-1].split("_")[2].split(".")[0]
                            charge_type = filelog[9].split(".")[-1]
                            
                            # 處理路徑檢視
                            print(file_path)

                            charge_2d_r, charge_2d_g, charge_2d_b = process_charge_file(f"../../mnt/disk2/AT_DATA/l4a/{file_path}")
                            charge_2d_r = fs_charge2d.put(Binary(pickle.dumps(charge_2d_r, protocol=5)))
                            charge_2d_g = fs_charge2d.put(Binary(pickle.dumps(charge_2d_g, protocol=5)))
                            charge_2d_b = fs_charge2d.put(Binary(pickle.dumps(charge_2d_b, protocol=5)))
                            
                            table_schema = {'lm_time': lm_time,
                                            'eqp_id': eqp_id,
                                            'op_id': op_id,
                                            'recipe_id': recipe_id,
                                            'chip_id': chip_id,
                                            'chip_pos': chip_pos,
                                            'ins_cnt': ins_cnt,
                                            'step': step,
                                            'charge_type': charge_type,
                                            '2d_r_object_id': charge_2d_r,                                        
                                            '2d_g_object_id': charge_2d_g,
                                            '2d_b_object_id': charge_2d_b,
                                            'file_path': file_path
                                            }
                            
                            try:
                                collection_charge2d.insert_one(table_schema)
                                del charge_2d_r
                                del charge_2d_g
                                del charge_2d_b
                                gc.collect()                               
                            except:
                                # db 內本來就有資料
                                pass
                        else:
                            
                            continue

    print("The current date and time is", datetime.now().strftime("%d/%m/%Y, %H:%M:%S"))
    print("==========Done==========")
    client.close()


task = PythonOperator(
    task_id='L4A_ETL',
    python_callable=etl,
    dag=dag,
    execution_timeout=timedelta(minutes=20),
)

task
