# Azure

Data ingention from Azure blob storage
storage_account_name = 'StorageAccountName'
storage_account_access_key = 'Access_Key'
spark.conf.set('fs.azure.account.key.' + storage_account_name + '.blob.core.windows.net', storage_account_access_key)
blob_container = 'container_name'
filePath = "wasbs://" + blob_container + "@" + storage_account_name + ".blob.core.windows.net/filename"
df = spark.read.format("csv").load(filePath, inferSchema = True, header = True)
df.write.parquet("dim_cities")
blob_container = 'container_name'
filePath = "wasbs://" + blob_container + "@" + storage_account_name + ".blob.core.windows.net/filename"
df = spark.read.format("csv").load(filePath, inferSchema = True, header = True)
df.show()
+-------------+---------------+-----+------+-------+
|Respondent_ID|           Name|  Age|Gender|City_ID|
+-------------+---------------+-----+------+-------+
|       120031| Aniruddh Issac|15-18|Female|  CT117|
|       120032|    Trisha Rout|19-30|  Male|  CT118|
|       120033|   Yuvraj  Virk|15-18|  Male|  CT116|
|       120034|   Pranay Chand|31-45|Female|  CT113|
|       120035| Mohanlal Joshi|19-30|Female|  CT120|
|       120036|  Zeeshan Ratta|19-30|Female|  CT118|
|       120037|     Oorja Anne|19-30|  Male|  CT112|
|       120038|    Rhea Khanna|19-30|  Male|  CT116|
|       120039|     Zara Joshi|46-65|  Male|  CT116|
|       120040|    Sana Dhawan|19-30|Female|  CT116|
|       120041|  Misha Warrior|19-30|Female|  CT113|
|       120042|  Kiara Chander|19-30|Female|  CT112|
|       120043|   Kimaya Borde|19-30|  Male|  CT113|
|       120044|Tushar Malhotra|19-30|  Male|  CT112|
|       120045|      Azad Gera|  65+|  Male|  CT113|
|       120046|     Aarav Bath|19-30|  Male|  CT114|
|       120047|      Ojas Char|19-30|  Male|  CT112|
|       120048|    Advik Baria|19-30|  Male|  CT116|
|       120049|    Dhruv Loyal|19-30|Female|  CT113|
|       120050|Dharmajan Doshi|31-45|Female|  CT114|
+-------------+---------------+-----+------+-------+
only showing top 20 rows

df.write.parquet("dim_repondents")
blob_container = 'container_name'
filePath = "wasbs://" + blob_container + "@" + storage_account_name + ".blob.core.windows.net/filename"
df = spark.read.format("csv").load(filePath, inferSchema = True, header = True)
df.display()
Table
 
Response_ID
Respondent_ID
Consume_frequency
Consume_time
Consume_reason
Heard_before
Brand_perception
General_perception
Tried_before
Taste_experience
1
2
3
4
5
6
7
8
9
10
11
12
13
14
15
16
17
103001
120031
2-3 times a week
To stay awake during work/study
Increased energy and focus
Yes
Neutral
Not sure
No
5
103002
120032
2-3 times a month
Throughout the day
To boost performance
No
Neutral
Not sure
No
5
103003
120033
Rarely
Before exercise
Increased energy and focus
No
Neutral
Not sure
No
2
103004
120034
2-3 times a week
To stay awake during work/study
To boost performance
No
Positive
Dangerous
Yes
5
103005
120035
Daily
To stay awake during work/study
Increased energy and focus
Yes
Neutral
Effective
Yes
5
103006
120036
Rarely
For mental alertness
To combat fatigue
Yes
Negative
Not sure
No
5
103007
120037
2-3 times a month
To stay awake during work/study
Increased energy and focus
No
Positive
Not sure
No
4
103008
120038
Rarely
Before exercise
To combat fatigue
No
Neutral
Healthy
Yes
4
103009
120039
Once a week
To stay awake during work/study
To enhance sports performance
No
Neutral
Effective
Yes
3
103010
120040
Once a week
For mental alertness
To combat fatigue
Yes
Neutral
Healthy
No
4
103011
120041
2-3 times a week
Before exercise
To combat fatigue
No
Negative
Healthy
Yes
5
103012
120042
Rarely
To stay awake during work/study
To combat fatigue
Yes
Neutral
Dangerous
No
2
103013
120043
2-3 times a week
Before exercise
To enhance sports performance
No
Neutral
Healthy
Yes
4
103014
120044
2-3 times a week
For mental alertness
To combat fatigue
No
Neutral
Healthy
No
5
103015
120045
2-3 times a week
To stay awake during work/study
Increased energy and focus
No
Positive
Healthy
Yes
5
103016
120046
Daily
Before exercise
Increased energy and focus
No
Neutral
Effective
Yes
3
103017
120047
Rarely
Before exercise
To boost performance
Yes
Neutral
Effective
No
4
6,713 rows
|
Truncated data
df.write.parquet("fact_survey_responses")
