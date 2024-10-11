import logging
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from google.cloud import bigquery

# Configure logging
logging.basicConfig(level=logging.INFO)
app = FastAPI()

# Pydantic model to define the request format expected from Dialogflow
class FulfillmentRequest(BaseModel):
    queryResult: dict



# Initialize BigQuery client
client = bigquery.Client()

# Define your BigQuery table ID (replace with actual table details)
PROJECT_ID = 'my-first-llm-project-2024'
DATASET_ID = 'customer_account'
TABLE_ID = 'cus_acc'
TABLE = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"


# Define the FastAPI route to check account balance
@app.post("/check-balance/")
async def check_balance(request: FulfillmentRequest):
    # Extract customer_name and account_number from the request
    parameters = request.queryResult.get("parameters")
    customer_name = parameters.get("customer_name")
    account_number = parameters.get("account_number")

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
            bigquery.ScalarQueryParameter("customer_name", "STRING", customer_name),
            bigquery.ScalarQueryParameter("account_number", "INT64", account_number),
        ]
    )

    # Execute the query
    try:
        query_job = client.query(query, job_config=job_config)
        result = query_job.result()  # Wait for job to complete

        # Check if the result is not empty
        if result.total_rows > 0:
            for row in result:
                balance = row["account_balance"]
                fulfillment_text = f"Your account balance is ${balance}."
        else:
            fulfillment_text = "I could not find your account. Please check the details again."

    except Exception as e:
        fulfillment_text = f"An error occurred while fetching the account balance: {str(e)}"

        # Respond back with the fulfillment message to Dialogflow
    return {
        "fulfillmentText": fulfillment_text
    }
# To run the FastAPI application:
# uvicorn filename:app --reload
