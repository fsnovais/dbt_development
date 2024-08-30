import time

def model(dbt, session):

    dbt.config(materialized="table")

    customers_df = dbt.ref("stg_customers")

    if target_name == "dev":
        customers_df = customers_df.limit(500)

    return customers_df