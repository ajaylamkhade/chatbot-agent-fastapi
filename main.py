from fastapi import FastAPI, Request
from google.cloud import bigquery
import logging

from starlette.responses import FileResponse
from starlette.staticfiles import StaticFiles

# Configure logging
logging.basicConfig(level=logging.INFO)
app = FastAPI()

# Initialize BigQuery client
client = bigquery.Client()

# Define your BigQuery table ID (replace with actual table details)
#PROJECT_ID = 'my-first-llm-project-2024'
#DATASET_ID = 'customer_account'
#TABLE_ID = 'cus_acc'
#TABLE = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

# Define your dataset and table names
project_id='my-first-llm-project-2024'
dataset_name = "customer_account"
payments_table = "payment"
accounts_table = "cus_acc"


# Mount static files directory (for serving HTML)
app.mount("/static", StaticFiles(directory="static"), name="static")
# Serve static files from the "images" folder
#app.mount("/static/images", StaticFiles(directory="static/images"), name="images")

# Route to serve HTML page
@app.get("/")
async def serve_html():
    return FileResponse("static/index.html")


@app.post("/webhook")
async def handle_webhook(request: Request):
    logging.info(f"/webhook called")
    try:
        # Parse the request body from Dialogflow
        req_body = await request.json()

        # Get the intent name
        intent_name = req_body.get('queryResult', {}).get('intent', {}).get('displayName')

        # Extract parameters
        parameters = req_body.get('queryResult', {}).get('parameters', {})
        # Log the intent name
        logging.info(f"Received intent: {intent_name}")

        if intent_name == "Capture_Account_Details":
            # Get account_id from parameters
            customer_name = parameters.get("customer_name")
            account_number = parameters.get("account_number")
            if account_number:
                # SQL Query to fetch account balance from BigQuery
                query = f"""
                SELECT account_balance
                FROM `{project_id}.{dataset_name}.{accounts_table}`
                WHERE customer_name = @customer_name
                AND account_number = @account_number
                LIMIT 1
                """
                # Create query job with parameterized input
                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("customer_name", "STRING", customer_name),
                        bigquery.ScalarQueryParameter("account_number", "INT64", account_number),
                    ]
                )
                
                # Run the query
                query_job = client.query(query, job_config=job_config)
                results = query_job.result()

                # Extract balance from results
                balance = None
                for row in results:
                    balance = row.account_balance

                if balance is not None:
                    # Return balance response
                    return {
                        "fulfillmentText": f"Your account balance for account number {account_number} is: {balance}. Is there anything else I can help you with?"
                    }
                else:
                    # Handle case where account_id is not found
                    return {
                        "fulfillmentText": f"I couldn't find any account with account number {account_number}."
                    }

            else:
                # Handle missing account_number case
                return {
                    "fulfillmentText": "Please provide a valid account number."
                }

        elif intent_name == "Gather_Payment_Details_Intent":
            # Get payment_id from parameters
            payment_id = parameters.get("payment_id")
           ## logging.info(f"payment_id: {payment_id}")
            if payment_id:
                # SQL Query to fetch payment status from BigQuery
                query = f"""
                SELECT status
                FROM `{project_id}.{dataset_name}.{payments_table}`
                WHERE payment_id = @payment_id
                LIMIT 1
                """
                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("payment_id", "INT64", payment_id)
                    ]
                )

                # Run the query
                query_job = client.query(query, job_config=job_config)
                results = query_job.result()

                # Extract payment status from results
                status = None
                for row in results:
                    status = row.status

                if status:
                    # Return payment status response
                    return {
                        "fulfillmentText": f"Your payment with ID {payment_id} has the status: {status}. Is there anything else I can help you with ?"
                    }
                else:
                    # Handle case where payment_id is not found
                    return {
                        "fulfillmentText": f"I couldn't find any payment with ID {payment_id}."
                    }

            else:
                # Handle missing payment_id case
                return {
                    "fulfillmentText": "Please provide a valid payment ID."
                }

        else:
            # Default response if intent is not recognized
            return {
                "fulfillmentText": "Sorry, I couldn't understand your request."
            }

    except Exception as e:
        logging.info(e.__cause__)
        # Handle any errors that occur
        return {
            "fulfillmentText": "Sorry, something went wrong while processing your request."
        }
