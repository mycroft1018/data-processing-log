#%%
import requests

def convert(coordString0, coordString1):
    currentkey = "734cd90e4a57acc97cb8e988afc51c8d"
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
df = pd.read_csv(r'C:\Users\mycro.SHEERAN_CORE\Desktop\cyq数据\cyq数据.csv')
original_data = []
i=0
# try:
#     # 解析','分隔的经纬度
#     addressString = tmpList[i]['formatted_address']
#     # 放入结果序列
#     resultList.append(addressString)
# except:
#     # 如果发生错误则存入None
#     resultList.append(None)
# return resultList
# for index, row in df.iterrows():
#     value_column1 = row['lon']
#     value_column2 = row['lat']
#     try:
#         data=convert(str(row['lon']), str(row['lat']))
#         print(data)
#         original_data.append(data)
#         time.sleep(0.10)
#         i+=1
#         print(i)
#         print(row)
#     except:
#         time.sleep(4)
#         value_column1 = row['lon']
#         value_column2 = row['lat']
#         original_data.append(None)
num_rows = len(df)
while i < num_rows:
    row = df.iloc[i]
    value_column1 = row['lon']
    value_column2 = row['lat']
    try:
        data = convert(str(row['lon']), str(row['lat']))
        print(data)
        original_data.append(data)
        sleep_time = random.uniform(0.04, 0.2)  #避免请求过快
        time.sleep(sleep_time)
        i += 1
        print(f"正在进行第 {i} 个循环 共计数据量为{num_rows}")
        print(f"翻译累计长度 {len(original_data)}")
        # print(row)
    except:
        time.sleep(1)
extracted_data=df
original_data = pd.DataFrame(original_data)
# Concatenate the two DataFrames vertically
merged_data = pd.concat([original_data, extracted_data], axis=1)
#%%
output_filename = 'C:\Users\mycro.SHEERAN_CORE\Desktop\cyq数据\cyq数据_location_decode.csv'
column_names = ['formatted_address', 'province', 'city', 'citycode', 'district', 'adcode', 'lon', 'lat','township','towncode']
merged_data.columns = column_names
# Create a DataFrame from the data and specify column names
df = pd.DataFrame(merged_data, columns=column_names)
df.to_csv(output_filename, index=False)

# def get_lon_lat(i):
#     url = 'https://restapi.amap.com/v3/geocode/geo?parameters'
#     parameters = {
#         'key':'',   							##输入自己的key
#         'address':'%s' % i
#         }
#     page_resource = requests.get(url,params=parameters)
#     text = page_resource.text       ##获得数据是json格式
#     data = json.loads(text)         ##把数据变成字典格式
#     lon_lat = data["geocodes"][0]['location']
#     return lon_lat






#%%
base = 'https://restapi.amap.com/v3/geocode/regeo?'
coordString0="116.6659"
coordString1="38.142471"
output = 'json'
batch = 'true'
currentkey = "3844e3f582067a25a6cd059bb32abbd0"
currentUrl = base + "output=" + output + "&batch=" + batch + "&location=" + coordString0 + "," + coordString1 + "&key=" + currentkey
#%%
currentkey = "3844e3f582067a25a6cd059bb32abbd0"
base2 = 'https://restapi.amap.com/v3/geocode/geo?'
locationString="长沙市岳麓区"
currentUrl2 = base2 + "output=" + output + "&batch=" + batch + "&address=" + locationString + "&key=" + currentkey

# api_url = f'{URL_geocode}city=邯郸市&address={address}&key={KEY}&output=json&callback=showLocation'
# https://restapi.amap.com/v3/geocode/geo?output=json&batch=true&address=岳麓区阳光100&key=3844e3f582067a25a6cd059bb32abbd0&callback=showLocation
# https://restapi.amap.com/v3/geocode/geo?key=27a90950bd4233a3589503ba03d58f25&address=上海市嘉定区墨玉南路888号|上海市嘉定区嘉定区墨玉南路1号&batch=true&output=json








#%% 给出企业名称，得到经纬度以及具体镇
import requests

##该函数把企业转换为经纬度坐标
def convert(locationString):
    currentkey = "3844e3f582067a25a6cd059bb32abbd0"
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
    currentkey = "3844e3f582067a25a6cd059bb32abbd0"
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
output_filename = 'C:\\Users\\mycro.SHEERAN_CORE\\Desktop\\文件夹汇总\\cyq数据\\cyq数据_location_decode.csv'
column_names = ['formatted_address', 'province', 'city', 'citycode', 'district', 'adcode', 'towncode',
                        'street', 'lon_lat', 'name']
merged_data2.columns = column_names
# Create a DataFrame from the data and specify column names
df = pd.DataFrame(merged_data2, columns=column_names)
df.to_csv(output_filename, index=False)