# pip install snowflake-connector-python

import snowflake.connector



# Set up connection parameters
# Use your Snowflake credentials

conn = snowflake.connector.connect(
    user='himanshu.gusain@aven.com',
    password='fymwu1-fesPub-dojwez',
    account='ii42072.us-east-2.aws',
    warehouse='FIVETRAN_WAREHOUSE',
    database='FIVETRAN_DATABASE',
    schema='POSTGRES_RDS_WAL_PUBLIC',

)

# Create a cursor object
cursor = conn.cursor()
