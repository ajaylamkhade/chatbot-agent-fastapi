import logging
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from google.cloud import bigquery

# Configure logging
logging.basicConfig(level=logging.INFO)
app = FastAPI()


# Define the input schema for the request
class BalanceRequest(BaseModel):
    customer_name: str
    account_number: int


# Initialize BigQuery client
client = bigquery.Client()

# Define your BigQuery table ID (replace with actual table details)
PROJECT_ID = 'my-first-llm-project-2024'
DATASET_ID = 'customer_account'
TABLE_ID = 'cus_acc'
TABLE = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"


# Define the FastAPI route to check account balance
@app.post("/check-balance/")
async def check_balance(request: BalanceRequest):
    customer_name = request.customer_name
    account_number = request.account_number
    logging.info(f"Name: {customer_name}, Account Number: {account_number}")

    # Initialize the BigQuery client with a specific project ID
    client = bigquery.Client(project='my-first-llm-project-2024')
    # Construct the query
    query = """
        SELECT account_balance
        FROM `my-first-llm-project-2024.customer_account.cus_acc`
        WHERE customer_name = @customer_name
        AND account_number = @account_number
    """



    # Create query job with parameterized input
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("customer_name", "STRING", request.customer_name),
            bigquery.ScalarQueryParameter("account_number", "INT64", request.account_number),
        ]
    )

    # Execute the query
    try:
        query_job = client.query(query, job_config=job_config)
        results = query_job.result()  # Wait for job to complete

        # Fetch the balance from the result
        for row in results:
            return {
               "account_balance": row.account_balance

            }

        raise HTTPException(status_code=404, detail="Account not found")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error querying BigQuery: {str(e)}")

# To run the FastAPI application:
# uvicorn filename:app --reload
