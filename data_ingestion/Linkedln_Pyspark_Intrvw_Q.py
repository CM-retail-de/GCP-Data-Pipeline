'''
Problem Statement
We have a dataset of LinkedIn users, where each record contains details about their work history — employer, job position, and the start and end dates of each job.

We want to find out how many users had Microsoft as their employer, and immediately after that, they started working at Google, with no other employers between these two positions.
'''
print("Initiating program")

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F

#Initialize spark sessions
spark = SparkSession.builder.appName("linedin_users").getOrCreate()

linedln_data = [
        (1, 'Microsoft', 'developer', '2020-04-13', '2021-11-01'),
    (1, 'Google', 'developer', '2021-11-01', None),
    (2, 'Google', 'manager', '2021-01-01', '2021-01-11'),
    (2, 'Microsoft', 'manager', '2021-01-11', None),
    (3, 'Microsoft', 'analyst', '2019-03-15', '2020-07-24'),
    (3, 'Amazon', 'analyst', '2020-08-01', '2020-11-01'),
    (3, 'Google', 'senior analyst', '2020-11-01', '2021-03-04'),
    (4, 'Google', 'junior developer', '2018-06-01', '2021-11-01'),
    (4, 'Google', 'senior developer', '2021-11-01', None),
    (5, 'Microsoft', 'manager', '2017-09-26', None),
    (6, 'Google', 'CEO', '2015-10-02', None)
]

linkedin_columns = ['user_id', 'employer', 'position', 'start_date', 'end_date']

linkedln_df = spark.createDataFrame(linedln_data,linkedin_columns)
#linkedln_df.orderBy('user_id','start_date').show()

window_spec = Window.partitionBy('user_id').orderBy('start_date')
linkedln_with_nxt_employer = linkedln_df.withColumn('next_column',F.lead('start_date').over(window_spec))
#linkedln_with_nxt_employer.show()

result = linkedln_with_nxt_employer.filter((linkedln_with_nxt_employer.employer=="Microsoft") & (linkedln_with_nxt_employer.next_column=="Google"))

#result.show()

# Step 4: Select and display user_id
user_ids = result.select('user_id').distinct()

# Show the result
user_ids.show()

# Count the number of distinct user_ids
user_count = user_ids.count()
print(user_count)
