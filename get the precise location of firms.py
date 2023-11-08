#%% 给出企业名称，得到经纬度以及具体镇
import requests

##该函数把企业转换为经纬度坐标
def convert(locationString):
    #contact me to get the key, you must be one of my wechat friends
    currentkey = ""
    output = 'json'
    batch = 'true'
    base2 = 'https://restapi.amap.com/v3/geocode/geo?'
    currentUrl2 = base2 + "output=" + output + "&batch=" + batch + "&address=" + locationString + "&key=" + currentkey
    # location
    try:
        response = requests.get(currentUrl2, timeout=5)  # 设置超时时间为5秒
        response.raise_for_status()  # 检查请求是否成功，如果请求失败会抛出异常
        result = response.json()

        location = result['geocodes'][0]['location']

        return location
    except:
        return 404

#%%
##该函数使用经纬度转换得到镇级坐标
def convert_lon_lat(coordString0, coordString1):
    #contact me to get the key, you must be one of my wechat friends
    currentkey = ""
    output = 'json'
    batch = 'true'
    base = 'https://restapi.amap.com/v3/geocode/regeo?'
    currentUrl = base + "output=" + output + "&batch=" + batch + "&location=" + coordString0 + "," + coordString1 + "&key=" + currentkey

    try:
        response = requests.get(currentUrl, timeout=5)  # 设置超时时间为5秒
        response.raise_for_status()  # 检查请求是否成功，如果请求失败会抛出异常
        result = response.json()
        formatted_address = result['regeocodes'][0]['formatted_address']
        province = result['regeocodes'][0]['addressComponent']['province']
        city = result['regeocodes'][0]['addressComponent']['city']
        citycode = result['regeocodes'][0]['addressComponent']['citycode']
        district = result['regeocodes'][0]['addressComponent']['district']
        adcode = result['regeocodes'][0]['addressComponent']['adcode']  #'township','towncode'
        township = result['regeocodes'][0]['addressComponent']['township']
        towncode = result['regeocodes'][0]['addressComponent']['towncode']
        print(coordString0 + "," + coordString1)
        return formatted_address, province, city, citycode, district, adcode,towncode,township
    except requests.exceptions.Timeout:  #错误控制模块
        print("请求超时，请检查网络连接或尝试增加超时时间。")
        return None, None, None, None, None, None
    except requests.exceptions.RequestException as e: #错误控制模块
        print("请求异常：", e)
        return None, None, None, None, None, None
#%%
import time
import random
import pandas as pd
import time
df = pd.read_csv('C:\\Users\\mycro.SHEERAN_CORE\\Desktop\\文件夹汇总\\cyq数据\\cyq数据.csv')
original_data = []
original_data1 = []
i=0
counter = 0
save_interval = 10
num_rows = len(df)

while i < num_rows:
    row = df.iloc[i]
    counter += 1
    try:
# while i < num_rows:
#     row = df.iloc[i]
#     value_column1 = row['lon']
#     value_column2 = row['lat']
#     try:
#         data = convert(str(row['lon']), str(row['lat']))
#         print(data)
#         original_data.append(data)
#         sleep_time = random.uniform(0.04, 0.2)  # 避免请求过快
#         time.sleep(sleep_time)
#         i += 1
#         print(f"正在进行第 {i} 个循环 共计数据量为{num_rows}")
#         print(f"翻译累计长度 {len(original_data)}")
#         # print(row)
#     except:
#         time.sleep(1)
        data = convert(row['reg_location'])
        # print(data)
        i += 1
        if  data!=404:
            coordString0 = data[0:10]
            coordString1 = data[11:21]
            # print(data)
            original_data.append(data)
            other_data=convert_lon_lat(coordString0, coordString1)
            print(other_data)
            original_data1.append(other_data)
            sleep_time = random.uniform(0.04, 0.2)  #避免请求过快
            time.sleep(sleep_time)
            print(f"正在进行第 {i} 个循环 共计数据量为{num_rows}")
            print(f"翻译累计长度 {len(original_data)}")
        else:
            if data == 404:
               original_data.append('')
               original_data1.append('')
    except: ##false control
        time.sleep(1)



#%%
print(counter)
extracted_data = df
original_data = pd.DataFrame(original_data)
original_data1 = pd.DataFrame(original_data1)
# Concatenate the two DataFrames vertically
merged_data = pd.concat([original_data, extracted_data], axis=1)
merged_data2 = pd.concat([original_data1, merged_data], axis=1)

extracted_data = df
original_data = pd.DataFrame(original_data)
original_data1 = pd.DataFrame(original_data1)
# Concatenate the two DataFrames vertically
merged_data = pd.concat([original_data, extracted_data], axis=1)
merged_data2 = pd.concat([original_data1, merged_data], axis=1)
output_filename = 'C:\\Users\\location_decode.csv'
column_names = ['formatted_address', 'province', 'city', 'citycode', 'district', 'adcode', 'towncode',
                        'street', 'lon_lat', 'name']
merged_data2.columns = column_names
# Create a DataFrame from the data and specify column names
df = pd.DataFrame(merged_data2, columns=column_names)
df.to_csv(output_filename, index=False)
