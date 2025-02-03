from pyspark.sql import SparkSession
from pyspark.sql import functions as F

#Initialize Spark sessions
spark = SparkSession.builder.appName("Pyspark feature with US Medical data").getOrCreate()

data = [
        ("12345", "John Doe", 35, "Male", "NY", "2023-01-15", "Flu", 100.2, 150),
    ("67890", "Jane Smith", 42, "Female", "CA", "2023-02-20", "Cold", 99.5, 120),
    ("13579", "David Lee", 60, "Male", "TX", "2023-03-10", "Diabetes", None, 180),
    ("24680", "Sarah Jones", 28, "Female", "FL", "2023-04-05", "Flu", 101.0, 140),
    ("98765", "Michael Brown", 55, "Male", "NY", "2023-05-12", "Hypertension", None, 160),
    ("11223", "Emily Davis", 31, "Female", "CA", "2023-06-25", "Cold", 98.9, 110),
    ("44556", "Kevin Wilson", 48, "Male", "TX", "2023-07-01", "Diabetes", None, 190),
    ("77889", "Ashley Garcia", 24, "Female", "FL", "2023-08-18", "Flu", 100.8, 130)
]

columns = ["PatientID", "Name", "Age", "Gender", "State", "VisitDate", "Diagnosis", "Temperature", "BloodPressure"]

df = spark.createDataFrame(data,columns)

# Scenario: Select patient name, ID, and create a new column for full name
df_fullname = df.select("PatientID","Name").withColumn("FullName",F.upper(F.col("Name")))
#df_fullname.show()

# Scenario: Select patient information and add a column indicating if temperature is above normal (98.6)
df_tmpcheck = df.select("PatientID","Name","Age","Temperature").withColumn("Fever", F.when(F.col("Temperature")>98.6, "Yes").otherwise("No"))
#df_tmpcheck.show()

# 2. filter (or where)
# Scenario: Filter patients diagnosed with "Flu"
df_flu_patients = df.filter(F.col("Diagnosis")=="Flu")
#df_flu_patients.show()

# Scenario: Filter patients older than 40 and from "CA"
df_old_patient = df.filter((F.col("age")>40) & (F.col("state")=="CA"))
#df_old_patient.show()

# 3. groupBy and agg
# Scenario: Calculate the average age of patients by state
def_agg_by_age = df.groupBy("state").agg(F.avg("age").alias("AverageAge"))
#def_agg_by_age.show()

# Scenario: Count the number of patients with each diagnosis
df_diagnosys_count = df.groupBy("Diagnosis").agg(F.count("*").alias("Diagnosis_count"))
#df_diagnosys_count.show()

# 4. orderBy (or sort)
# Scenario: Order patients by age in descending order
df_order_patients = df.orderBy(F.col("age").desc())
#df_order_patients.show()

# 5. lit and concat_ws
# Scenario: Add a constant column "Hospital" with value "General Hospital"
df_with_hospital = df.withColumn("Hospitsl",F.lit("General Hospital"))
df_with_hospital.show()

# Scenario: Create a formatted address column by concatenating State and a literal ", USA"
df_with_address = df.withColumn("Address",F.concat_ws(", ", F.col("state"), F.lit("USA")))
df_with_address.show()

 # Stop SparkSession
spark.stop()