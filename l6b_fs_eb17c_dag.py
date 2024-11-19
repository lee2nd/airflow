from airflow import DAG
from airflow.operators.python import PythonOperator
from pymongo import MongoClient 
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import os
from bson.binary import Binary
import pickle
import gridfs
import gc
import trigger_setting


data_path = "/home/ivan/Program/AT_DATA/l6b_fs_eb17c"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3, # -1 在失敗時將不斷重試直到成功為止
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'L6B_FS_EB17C_ETL',
    default_args=default_args,
    description='L6B_FS_EB17C_ETL',
    schedule='*/10 * * * *', # 每 10 分鐘跑一次
)


# 取得 CONFIG 資訊
client = MongoClient('mongodb://ivan:ivan@10.88.26.183:27017')
db = client["AT_config"]
collection = db["config"]
cursor = collection.find()
df_config = pd.DataFrame.from_records(cursor).drop(columns=["_id"])


# 資料庫連結
db = client["AT"]
collection_charge2d = db["L6B_FS_charge2d"]
collection_defectinfo = db["L6B_FS_defectinfo"]
fs_charge2d = gridfs.GridFS(db, collection="L6B_FS_charge2d")
fs_defectinfo = gridfs.GridFS(db, collection="L6B_FS_defectinfo")

# 創建 index 避免資料重複塞進資料庫
collection_charge2d.create_index([("lm_time", 1),
                                    ("eqp_id", 1),
                                    ("op_seq", 1),
                                    ("recipe_id", 1),
                                    ("lot_id", 1),
                                    ("sheet_id", 1),
                                    ("chip_id", 1),
                                    ("chip_pos", 1),
                                    ("ins_cnt", 1)], 
                                unique=True)   
collection_defectinfo.create_index([("lm_time", 1),
                                    ("eqp_id", 1),
                                    ("op_seq", 1),
                                    ("recipe_id", 1),
                                    ("lot_id", 1),
                                    ("sheet_id", 1),
                                    ("chip_id", 1),
                                    ("chip_pos", 1),
                                    ("ins_cnt", 1),
                                    ("BIN", 1)], 
                                unique=True)  


def process_charge_file(file_path, lot_id):
    
    filetype = file_path.split('.')[-1]     
    lot_id_f2 = lot_id[:2]

    W = df_config[df_config["model"]==lot_id_f2]["W"].values[0]
    H = df_config[df_config["model"]==lot_id_f2]["H"].values[0]      
    
    if filetype == 'f1p':
        C=0.00244171
    elif filetype == 'f10p':
        C=0.0244171
    elif filetype == 'f100p':
        C=0.244171
    elif filetype == 'f1000p':
        C=2.44171
    
    in_f100p_a = np.fromfile(file_path,np.uint8)
    in_f100p_o = in_f100p_a[0::2]
    in_f100p_e = in_f100p_a[1::2]
    out_f100p_a = 0.256*in_f100p_o + 0.001*in_f100p_e
    out_f100p_a = out_f100p_a[4:(W*H)+4]*C*1000 

    charge_2d_r = np.reshape(out_f100p_a[0::3],(H,W//3))
    charge_2d_g = np.reshape(out_f100p_a[1::3],(H,W//3))
    charge_2d_b = np.reshape(out_f100p_a[2::3],(H,W//3))

    return charge_2d_r, charge_2d_g, charge_2d_b



def etl():
        
    for file in os.listdir(f"{data_path}/file_log"):
        
        file_log_path = os.path.join(f"{data_path}/file_log", file)
        filename = file_log_path.split("/")[-1]
        
        try:
            filedate = datetime.strptime(filename[7:15], "%Y%m%d")
        except:
            continue
        
        cond_1_1 = (filename[:6] == "EB-17C")
        cond_1_2 = (len(filename) == 19)
        cond_1_3 = (filedate >= (datetime.now()-timedelta(days=trigger_setting.day)).replace(hour=0, minute=0, second=0, microsecond=0))
        
        if all([cond_1_1,cond_1_2,cond_1_3]):
            
            f = open(file_log_path, 'r')
            fileloglines = f.readlines()

            for i in range(1,len(fileloglines)):
                
                filelog = fileloglines[i].split(",")

                # 運行時間檢視
                current_time = datetime.now()
                log_time = datetime.strptime(filelog[0], "%Y/%m/%d %H:%M:%S.%f")
                time_difference = current_time - log_time
                
                if time_difference.total_seconds()/60 <= trigger_setting.time_difference:
                    
                    print(log_time)
                    
                    # table schema
                    lm_time = datetime.strptime(filelog[0], "%Y/%m/%d %H:%M:%S.%f").strftime("%Y/%m/%d %H:%M:%S")
                    eqp_id = filelog[1]
                    op_seq = filelog[2]
                    recipe_id = filelog[3]
                    lot_id = filelog[4]
                    if lot_id[:2] not in ["EE","EG","EK","EJ","EM","EL"]: continue
                    sheet_id = filelog[5]
                    if filelog[7][0] != 'E' or len(filelog[7]) < 3: continue
                    chip_id = filelog[7]
                    if "_" in chip_id: chip_id = chip_id.split("_")[0]
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

                    if len(lot_id) == 10:
                        
                        # ADR 解檔成 Defect Information
                        if "Adr" in file_name:
                            
                            chip_pos = filelog[9].replace(".Adr","")
                            # 處理路徑檢視
                            print(file_path)
                            
                            try:
                                f = open(f"{data_path}/{file_path}", 'r')
                            except:
                                continue
                            lines = f.readlines()

                            defect_info = []
                            color = {0:df_config[df_config["model"]==lot_id[:2]]["RGB order"].values[0][2],
                                    1:df_config[df_config["model"]==lot_id[:2]]["RGB order"].values[0][0],
                                    2:df_config[df_config["model"]==lot_id[:2]]["RGB order"].values[0][1]}
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
                                
                                if line == "": continue
                                
                                if input_data[0] == 'BIN':
                                    BIN = input_data[1]
                                # chip time
                                elif input_data[0] == 'START_DATE':
                                    START_DATE = input_data[1]     
                                elif input_data[0] == 'START_TIME':
                                    chip_start_time = START_DATE + " " + input_data[1]
                                elif input_data[0] == 'END_DATE':
                                    END_DATE = input_data[1]     
                                elif input_data[0] == 'END_TIME':
                                    chip_end_time = END_DATE + " " + input_data[1]
                                # sheet time
                                elif input_data[0] == 'GLASS_START_DATE':
                                    GLASS_START_DATE = input_data[1]     
                                elif input_data[0] == 'GLASS_START_TIME':
                                    sheet_start_time = GLASS_START_DATE + " " + input_data[1]
                                # lot time
                                elif input_data[0] == 'LOT_START_DATE':
                                    LOT_START_DATE = input_data[1]     
                                elif input_data[0] == 'LOT_START_TIME':
                                    lot_start_time = LOT_START_DATE + " " + input_data[1]                        
                                    
                                # 抓 defect information
                                if "==Step" in line:
                                        
                                    STEP = line.replace("=","")
                                    step_s = d_no
                                    step_e = 0
                                    
                                elif ((step_e==0) and ("==END" in line)) or ("[ESCAPE_LINE_SOURCE]" in line): 

                                    step_s = 0
                                    step_e = d_no-1

                                elif step_s > 0 and step_e == 0: 

                                    if line[0] == "[":
                                        
                                        Defect_code = line[1:-1]

                                    elif line[:13] == "UPPER_LIMIT =":
                                            UPPER_LIMIT_R = line.split(" = ")[1]
                                    elif line[:13] == "UPPER_LIMIT_G":
                                            UPPER_LIMIT_G = line.split(" = ")[1]
                                    elif line[:13] == "UPPER_LIMIT_B":
                                            UPPER_LIMIT_B = line.split(" = ")[1]
                                    elif line[:13] == "LOWER_LIMIT =":
                                            LOWER_LIMIT_R = line.split(" = ")[1]
                                    elif line[:13] == "LOWER_LIMIT_G":                                           
                                            LOWER_LIMIT_G = line.split(" = ")[1]
                                    elif line[:13] == "LOWER_LIMIT_B":                                            
                                            LOWER_LIMIT_B = line.split(" = ")[1]                                            
                                        
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
                                                    LED_Type = color[S%3]
                                                    defect_info.append([STEP,Defect_code,LED_Type,S,G,value,UPPER_LIMIT_R,UPPER_LIMIT_G,UPPER_LIMIT_B,LOWER_LIMIT_R,LOWER_LIMIT_G,LOWER_LIMIT_B])   

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

                            df_defect = pd.DataFrame(data=defect_info, columns=['Step','Defect_code','LED_Type','Source','Gate','Value','Upper_limit_r','Upper_limit_g','Upper_limit_b','Lower_limit_r','Lower_limit_g','Lower_limit_b'])
                            # df_defect = df_defect[df_defect['Step'].isin(["Step1_Step2","Step7_Step8","Step9_Step10","Step11_Step12","Step19_Step20","Step27_Step28"])]
                            # df_defect = df_defect[df_defect['Defect_code'].isin(["BAD_PIXEL","SHORT_PIXEL"])]
                            
                            # 待做資料轉換
                            df_defect = fs_defectinfo.put(Binary(pickle.dumps(df_defect, protocol=5)))
                            
                            # assign whole chip information
                            table_schema = {'lm_time': lm_time,
                                            'sheet_start_time': sheet_start_time,
                                            'lot_start_time': lot_start_time,
                                            'chip_start_time': chip_start_time,
                                            'chip_end_time': chip_end_time,
                                            'eqp_id': eqp_id,
                                            'op_seq': op_seq,
                                            'recipe_id': recipe_id,
                                            'lot_id': lot_id,
                                            'sheet_id': sheet_id,
                                            'chip_id': chip_id,
                                            'chip_pos': chip_pos,
                                            'ins_cnt': ins_cnt,
                                            'BIN': BIN,
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
                                del table_schema
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
                            
                            try:
                                charge_2d_r, charge_2d_g, charge_2d_b = process_charge_file(f"{data_path}/{file_path}", lot_id)
                                charge_2d_r = fs_charge2d.put(Binary(pickle.dumps(charge_2d_r, protocol=5)))
                                charge_2d_g = fs_charge2d.put(Binary(pickle.dumps(charge_2d_g, protocol=5)))
                                charge_2d_b = fs_charge2d.put(Binary(pickle.dumps(charge_2d_b, protocol=5)))
                                
                                table_schema = {'lm_time': lm_time,
                                                'eqp_id': eqp_id,
                                                'op_seq': op_seq,
                                                'recipe_id': recipe_id,
                                                'lot_id': lot_id,
                                                'sheet_id': sheet_id,
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

                                collection_charge2d.insert_one(table_schema)
                                del charge_2d_r
                                del charge_2d_g
                                del charge_2d_b
                                del table_schema
                                gc.collect()                              
                            except:
                                # db 內本來就有資料
                                pass
                            
                        else:
                            # 非 adr, chargemap
                            continue
    
    print("The current date and time is", datetime.now().strftime("%d/%m/%Y, %H:%M:%S"))
    client.close()
    print("==========Done==========")


task = PythonOperator(
    task_id='L6B_FS_EB17C_ETL',
    python_callable=etl,
    dag=dag,
    execution_timeout=timedelta(minutes=10),
)


task
