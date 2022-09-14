import pandas as pd
import numpy as np
import pymysql


# print("now, the system is analyzing data ......")

# pd_reader = pd.read_csv("./raw-61data.csv")

file = 'static/csv/raw_data'
# file = 'tem01'

pd_reader = pd.read_csv(file+'.csv')


# print(pd_reader)

# # df = pd_reader.drop(['时间'], axis=1)
df = pd_reader.copy()
# print("df:",df)
print("整理后的源数据:",df)



# 调整1，去除体动大于100的数据
df = df.drop(df[(df['体动'] > 100)].index)
# print("removed data(体动>100):",df)




# temp data cleaning
# df = df.drop(df[(df['心率'] == 0) | (df['低压'] == 0) | (df['高压'] == 0)].index)
# data clean, replace 0 by  1
df.replace(to_replace = 0, value = 1, inplace=True)
df = df.drop(columns=['X', 'Y', 'Z', '前面积','后面积','体动'], errors='ignore')
df = df.drop(df[(df['心率'] == 1) | (df['低压'] == 1) | (df['高压'] == 1) | (df['RR'] == 1)].index)

df = df.drop(df[(df['低压'] < 40) | (df['高压'] < 80)].index)
# witoutCol = '低压'
# df = df.drop(columns=[witoutCol])
df.reset_index(drop=True, inplace=True)
# print(df)


# ## 一 计算健康标尺

# In[29]:


# Geometric Mean of the column in dataframe
import scipy
from scipy import stats
print(len(df.columns)) 
##scipy.stats.gmean(df.iloc[:,1:7],axis=0)
# 结构时中
GEOMEAN1 = scipy.stats.gmean(df.iloc[:,1:len(df.columns)],axis=1)
print("max ----------",GEOMEAN1)
import math
# M个数据组结构时中中最大值与最小值的定量差异（最大值与最小值比值以0.618为底取对数，在取绝对值）
QDMM1 = abs(math.log(max(GEOMEAN1)/min(GEOMEAN1),0.618))


# print(QDMM1)


# In[ ]:


def calculateQDMMByFixedYinYang(df_yang, df_yin):
	QDMM_MAX = QDMM1
	colname_max = ''
	index_max = 0
	# print("yin", df_yin)
	if len(df_yin) > 0:
		df_temp = pd.concat([df_yang, df_yin], axis=1, join='inner')		
		for (index, colname) in enumerate(df_yin):
			print('df_yang: ', index, colname)
			if index !=0:
				df_yin[colname] = 1/df_yin[colname]
				GEOMEAN_temp = scipy.stats.gmean(df_temp.iloc[:,1:len(df_temp.columns)],axis=1)
				# print(GEOMEAN_temp)
				# M个数据组结构时中中最大值与最小值的定量差异（最大值与最小值比值以0.618为底取对数，在取绝对值）
				QDMM_temp = abs(math.log(max(GEOMEAN_temp)/min(GEOMEAN_temp),0.618))
				# print(QDMM_temp, index, colname)
				if QDMM_temp > QDMM_MAX:
					QDMM_MAX = QDMM_temp
					colname_max = colname
					index_max = index	
					df_drop = df_temp[colname]


# In[ ]:


def calculateQDMM(df_yang, df_yin):
	QDMM_MAX = QDMM1
	colname_max = ''
	index_max = 0
	# print("yin", df_yin)
	if len(df_yin) > 0:
		df_temp = pd.concat([df_yang, df_yin], axis=1, join='inner')		
		for (index, colname) in enumerate(df_yang):
			print('df_yang: ', index, colname)
			if index !=0:
				df_temp[colname] = 1/df_temp[colname]
				GEOMEAN_temp = scipy.stats.gmean(df_temp.iloc[:,1:len(df_temp.columns)],axis=1)
				# print(GEOMEAN_temp)
				# M个数据组结构时中中最大值与最小值的定量差异（最大值与最小值比值以0.618为底取对数，在取绝对值）
				QDMM_temp = abs(math.log(max(GEOMEAN_temp)/min(GEOMEAN_temp),0.618))
				# print(QDMM_temp, index, colname)
				if QDMM_temp > QDMM_MAX:
					QDMM_MAX = QDMM_temp
					colname_max = colname
					index_max = index	
					df_drop = df_temp[colname]
	else:
		df_temp = df_yang.copy()
		for (index, colname) in enumerate(df_temp):
			print('no df_yin: ', index, colname)
			if index !=0:
				df_temp[colname] = 1/df_temp[colname]
				GEOMEAN_temp = scipy.stats.gmean(df_temp.iloc[:,1:len(df_temp.columns)],axis=1)
				# print(GEOMEAN_temp)
				# M个数据组结构时中中最大值与最小值的定量差异（最大值与最小值比值以0.618为底取对数，在取绝对值）
				QDMM_temp = abs(math.log(max(GEOMEAN_temp)/min(GEOMEAN_temp),0.618))
				# print(QDMM_temp, index, colname)
				if QDMM_temp > QDMM_MAX:
					QDMM_MAX = QDMM_temp
					colname_max = colname
					index_max = index	
					df_drop = df_temp[colname]
	# print(QDMM_temp)
	if (index_max != 0):
		print(QDMM_MAX, colname_max, index_max)
		df_yang = df_yang.drop([colname_max], axis=1)
		# print(df_yang)
		if len(df_yin) > 0:
			df_yin  = pd.concat([df_yin, df_drop], axis=1, join='inner')
		else:
			df_yin = df_drop
		# print(df_yin)
		# calculateQDMM(df_yang, df_yin)
	# else:
	# 	print(QDMM_MAX, colname_max, index_max)
	# 	df_temp = pd.concat([df_yang, df_yin], axis=1, join='inner')
	# 	print(df_yang)
	return df_yang, df_yin, QDMM_temp, index_max	


# ## 二 排除异常数据组

# In[ ]:


def dataClean(df_yang, df_yin):
	df_temp = pd.concat([df_yang, df_yin], axis=1, join='inner')
	print(df_temp)
	# df_temp = df.copy()
	for (index, colname) in enumerate(df_temp):
		if index !=0:
			# 1.在GCYY数据流中计算每个参数的功能时中
			GEOMEAN_func_temp = scipy.stats.gmean(df_temp.iloc[:,index],axis=0)
			# print(GEOMEAN_func_temp)
			# 2.每个参数纵向除以自己的功能时中
			df_temp[colname] = df_temp[colname]/GEOMEAN_func_temp
		
	# df_temp = df_temp.append(df_temp.sum(axis=1, numeric_only=True), ignore_index=True)
	dataframe_sum = df_temp.sum(axis=1, numeric_only=True)
	# 3.每个数据组横向计算所有参数的和，称为数据和
	print(dataframe_sum)
	df_temp["sum"] = dataframe_sum
	
	# 4.计算每个参数横向除以自己的数据和，称为占比
	for (index, colname) in enumerate(df_temp):
		if index !=0 and colname != "sum":
			df_temp[colname] = df_temp[colname]/df_temp["sum"]
	# print(df)
	# 计算每个数据组横向所有占比的结构时中
	GEOMEAN2 = scipy.stats.gmean(df_temp.iloc[:,1:len(df_temp.columns)-2],axis=1)
	# print(GEOMEAN2)
	# 计算所有结构时中的几何均值GM
	GEOMEAN2_GM = scipy.stats.gmean(GEOMEAN2)
	# print(GEOMEAN2_GM)
	# print(GEOMEAN2_GM)
	# 计算每个结构时中与GM的定量差异，计算相继两个结构时中的定量差异，删除其中定量差异大于0.1610的数据组
	import numpy as np
	arr = np.array(GEOMEAN2)
	# todo: #1 check
	qdmmArray = abs(np.log(arr/GEOMEAN2_GM)/np.log(0.618))
	# print(qdmmArray)
	qdmmDiffArray = np.diff(qdmmArray)
	# print(qdmmDiffArray)
	indexrResult = np.where(abs(qdmmDiffArray) > 0.1610)
	# indexrResult = np.where(abs(np.log(arr/GEOMEAN2_GM)/np.log(0.618)) > 0.1610)
	# print(len(indexrResult[0]))
	# tt = []
	# for i in range(arr.shape[0]-1):		
	# 	# QDMM_temp = abs(math.log(arr[i+1]/arr[i],0.618))
	# 	QDMM_temp = abs(np.log(arr[i+1]/arr[i])/np.log(0.618))
	# 	if (QDMM_temp > 0.1610):
	# 		tt.append(i)
	# 		tt.append(i+1)
			# np.append(indexrResult[0], i)
			# np.append(indexrResult[0], i+1)
	# print("tt", len(tt))
	# print(len(indexrResult[0]))
	df_temp.drop(indexrResult[0], axis=0, inplace=True)
	df_yang.drop(indexrResult[0], axis=0, inplace=True)
	df_yin.drop(indexrResult[0], axis=0, inplace=True)
	# df.drop([5,6], axis=0, inplace=True)
	return (df_yang, df_yin)


# In[ ]:


# QDMM_MAX = QDMM1
# df_yang, df_yin, QDMM_temp, index_max = calculateQDMM(df, pd.DataFrame([]))
# print(df_yang)
# print(df_yin)
# while index_max != 0:		
# 	df_yang, df_yin, QDMM_temp, index_max = calculateQDMM(df_yang, df_yin)

# special case for specified yin and yang
for (index, colname) in enumerate(df):
	print('df: ', index, colname)
	if index !=0 and colname != 'RR':
		df[colname] = 1/df[colname]
df_yang = df.drop(['心率','低压','高压','时间'], axis=1, errors='ignore')
df_yin = df.drop(['RR','时间'], axis=1)

print(df_yang)
print(df_yin)

df_yang, df_yin = dataClean(df_yang, df_yin)


# # 三 计算健康标尺中的非常显著性差异点

# In[ ]:


# 再重复以上两个大步骤，直到结构不再变化。
# TODO: temp remove
# df_yang, df_yin, QDMM_temp, index_max = calculateQDMM(df_yang, df_yin)
# print(df_yang)
# print(df_yin)
# 在排除异常数据组之后，将GCYY数据流中的GCYY排序，计算所有GCYY的几何均值GM，
df_all = pd.concat([df_yang, df_yin], axis=1, join='inner')
print(len(df_all.columns)) 
GCYY = scipy.stats.gmean(df_all.iloc[:,1:len(df_all.columns)],axis=1)
print(GCYY)
GM = scipy.stats.gmean(GCYY)
# print(GM)
# 计算每个GCYY与GM的定量差异，计算相继两个GCYY的定量差异，
# 标出其中定量差异大于0.805的数据组，其中GCYY较低的标绿色，GCYY较高的标橙色
import numpy as np
arr_GCYY = np.array(GCYY)


indexrGreen = np.where((abs(np.log(arr_GCYY/GM)/np.log(0.618)) > 0.805) & (arr_GCYY < GM))
indexrOrange = np.where((abs(np.log(arr_GCYY/GM)/np.log(0.618)) > 0.805) & (arr_GCYY > GM))
print(np.where((abs(np.log(arr_GCYY/GM)/np.log(0.618)) > 0.805)))
print(len(indexrGreen[0]))
print(len(indexrOrange[0]))


# In[ ]:

# write to csv
# print(df)
# import os  
# os.makedirs('subfolder', exist_ok=True)  
df_all['GCYY'] = GCYY
# df_all['time'] = pd.to_datetime(df['时间'], format='%Y-%m-%d %H:%M:%S')
df_all['time'] = pd.to_datetime(df['时间'], format='%Y-%m-%d %H:%M:%S')
df_all = df_all.drop(df_all[(df_all['GCYY'] == 1)].index)
# df_csv = pd.concat([df_all, df_yin], axis=1, join='inner')
output = 'static/csv/' + "data_analysis" + '_report' + '.csv'
print(output)
df_csv = df_all.copy()
print(df_csv)
for (index, colname) in enumerate(df_csv):
	print('df_csv: ', index, colname)
	if ((colname == '心率') | (colname == '低压') | (colname == '高压')):
		df_csv[colname] = 1/df_csv[colname]
print(df_csv)

# 调整2， 计算每一分钟gcyy的平均值，只评估安静状态的gcyy
df_csv['time'] = df_csv['time'].dt.floor('T')
# print(df_csv)
grouped_df = df_csv.groupby(['time'])
mean_df = grouped_df.mean()
mean_df = mean_df.reset_index()

print(mean_df)

mean_df.to_csv(output, index=False)

print("恭喜🎉🎉🎉, 建模处理完毕！")
