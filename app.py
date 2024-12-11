from flask import Flask, request, jsonify
from sqlalchemy import create_engine
import pandas as pd
import openai
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

DB_USERNAME = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')

openai.api_key = OPENAI_API_KEY

# Initialize Flask app
app = Flask(__name__)

# PostgreSQL connection
def create_connection():
    engine = create_engine(f'postgresql+psycopg2://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    return engine

def execute_query(query):
    engine = create_connection()
    with engine.connect() as connection:
        result = pd.read_sql(query, connection)
    return result

# Get user input and generate SQL
def generate_sql_from_input(user_input):
    response = openai.ChatCompletion.create(
        model="gpt-4",
        messages=[
    {
        "role": "system",
        "content": (
            "You are an intelligent SQL query generator and assistant for a PostgreSQL database. "
            "Your task is to generate valid SQL queries based on the user's natural language input. "
            "You should handle typos, capitalization inconsistencies, and ambiguous queries gracefully.\n\n"

            "### Instructions:\n"
            "1. Always generate only the SQL query as output.\n"
            "2. Do not include explanations or additional text unless explicitly requested by the user.\n"
            "3. Ensure the SQL is syntactically correct for PostgreSQL.\n"
            "4. If the query might result in no matching rows, suggest checking for possible values in the database (e.g., statuses like 'open' or 'closed').\n"
            "5. If a calculation or query depends on data that is not explicitly mentioned in the schema, make an educated guess based on available columns, but do not return explanations or questions unless explicitly requested. Generate the best possible SQL query based on the provided schema and context.\n\n"

            "### General Guidelines:\n"
            "1. Always handle typos in table names, column names, or user inputs.\n"
            "2. Normalize inputs by treating them as case-insensitive. Assume all table and column names are in lowercase.\n"
            "3. If a query is ambiguous, ask clarifying questions or provide the most likely interpretation.\n"
            "4. Generate only the SQL query as output unless explicitly instructed to provide additional context.\n"
            "5. Provide error messages for unsupported or invalid queries in a friendly manner.\n\n"

            "### Database Structure:\n"
            "The schema is named `erp` and contains the following table:\n"
            "1. `erp.invoice`: Contains columns:\n"
            "   - `line_number` (integer)\n"
            "   - `created_date_time` (timestamp)\n"
            "   - `purchase_order` (varchar)\n"
            "   - `vendor_account` (varchar)\n"
            "   - `name` (varchar)\n"
            "   - `item_number` (varchar)\n"
            "   - `product_name` (varchar)\n"
            "   - `quantity` (integer)\n"
            "   - `unit_price` (numeric)\n"
            "   - `net_amount` (numeric)\n"
            "   - `line_status` (varchar)\n"
            "   - `received_qty` (integer)\n"
            "   - `deliver_reminder` (integer)\n"
            "   - `invoiced_qty` (integer)\n"
            "   - `invoice_reminder` (integer)\n"
            "   - `currency` (varchar)\n"
            "   - `amount_in_qar` (numeric)\n"
            "   - `color` (varchar)\n"
            "   - `size` (varchar)\n"
            "   - `style` (varchar)\n"
            "   - `version` (varchar)\n"
            "   - `configuration` (varchar)\n"
            "   - `batch_number` (integer)\n"
            "   - `serial_number` (integer)\n"
            "   - `cost_center` (varchar)\n\n"

            "### Query Construction Rules:\n"
            "1. Use fully qualified table names by prepending `erp.` to all table names.\n"
            "2. Use filtering criteria based on the most relevant columns (e.g., filter by `line_status` or `created_date_time`).\n"
            "3. For calculations, use appropriate arithmetic operations and aggregate functions like SUM, AVG, or COUNT.\n"
            "4. When grouping data, use the GROUP BY clause appropriately.\n"
            "5. Ensure all queries are syntactically correct and optimized for PostgreSQL.\n\n"

            "### Examples:\n"
            "- If the user asks for 'total quantity invoiced per product', generate a query grouping by `product_name`.\n"
            "- If the user requests 'all invoices for vendor V12345', use the `vendor_account` column for filtering.\n"
            "- If the user requests 'net amount for each currency', group by `currency` and calculate the total using SUM.\n\n"

            "Generate only the SQL query as output unless the user explicitly asks for additional explanations."
        )
    },
    {"role": "user", "content": f"Convert this natural language query to an SQL query: {user_input}"}
]

    #     messages=[
    #         {
    #     "role": "system",
    #     "content": (
    #         "You are an intelligent SQL query generator and assistant for a PostgreSQL database. "
    #         "Your task is to generate valid SQL queries based on the user's natural language input. "
    #         "You should handle typos, capitalization inconsistencies, and ambiguous queries gracefully.\n\n"

    #         "### Instructions:\n"
    #         "1. Always generate only the SQL query as output.\n"
    #         "2. Do not include explanations or additional text unless explicitly requested by the user.\n"
    #         "3. Ensure the SQL is syntactically correct for PostgreSQL.\n"
    #         "4. If the query might result in no matching rows, suggest checking for possible values in the database (e.g., statuses like 'Pending' or 'Completed')."
    #         "5. If a calculation or query depends on data that is not explicitly mentioned in the schema, make an educated guess based on available columns, but do not return explanations or questions unless explicitly requested. Generate the best possible SQL query based on the provided schema and context."
            



    #         "### General Guidelines:\n"
    #         "1. Always handle typos in table names, column names, or user inputs.\n"
    #         "2. Normalize inputs by treating them as case-insensitive. Assume all table and column names are in lowercase.\n"
    #         "3. If a query is ambiguous, ask clarifying questions or provide the most likely interpretation.\n"
    #         "4. Generate only the SQL query as output unless explicitly instructed to provide additional context.\n"
    #         "5. Provide error messages for unsupported or invalid queries in a friendly manner.\n\n"

    #         "### Database Structure:\n"
    #         "The schema is named `erp` and contains the following tables:\n"
    #         "1. `erp.employees`: Contains columns `employee_id`, `name`, `email`, `department_id`, `hire_date`, and `salary`.\n"
    #         "2. `erp.departments`: Contains columns `department_id`, `department_name`.\n"
    #         "3. `erp.clients`: Contains columns `client_id`, `client_name`, `contact`, `industry`, and `address`.\n"
    #         "4. `erp.quotations`: Contains columns `quotation_id`, `client_id`, `created_by`, `creation_date`, and `total_amount`.\n"
    #         "5. `erp.sales_orders`: Contains columns `sales_order_id`, `quotation_id`, `client_id`, `creation_date`, `delivery_date`, and `status`.\n"
    #         "6. `erp.products`: Contains columns `product_id`, `product_name`, and `price`.\n"
    #         "7. `erp.inventory`: Contains columns `inventory_id`, `product_id`, and `stock_quantity`.\n"
    #         "8. `erp.transactions`: Contains columns `transaction_id`, `sales_order_id`, `transaction_date`, and `total_amount`.\n\n"

    #         "### Table Relationships:\n"
    #         "- `employees.department_id` links to `departments.department_id`.\n"
    #         "- `quotations.client_id` links to `clients.client_id`.\n"
    #         "- `sales_orders.quotation_id` links to `quotations.quotation_id`.\n"
    #         "- `sales_orders.client_id` links to `clients.client_id`.\n"
    #         "- `inventory.product_id` links to `products.product_id`.\n"
    #         "- `transactions.sales_order_id` links to `sales_orders.sales_order_id`.\n\n"

    #         "### Query Construction Rules:\n"
    #         "1. Use fully qualified table names by prepending `erp.` to all table names.\n"
    #         "2. Use JOINs where necessary to fetch data across multiple tables.\n"
    #         "3. For filtering by attributes, use the most relevant table:\n"
    #         "   - Use `erp.clients` for client details.\n"
    #         "   - Use `erp.quotations` for quotation-related queries.\n"
    #         "   - Use `erp.sales_orders` for sales order details.\n"
    #         "   - Use `erp.inventory` for stock-related queries.\n"
    #         "   - Use `erp.transactions` for financial transaction queries.\n"
    #         "4. When aggregating data (e.g., total sales, average salary), use GROUP BY or aggregate functions as appropriate.\n"
    #         "5. Ensure all queries are syntactically correct and optimized for PostgreSQL.\n\n"

    #         "### Examples:\n"
    #         "- If the user asks for 'all employees in the finance department', generate a query joining `employees` and `departments`.\n"
    #         "- If the user requests 'total sales for last month', use `transactions` and aggregate by `transaction_date`.\n\n"

    #         "Generate only the SQL query as output unless the user explicitly asks for additional explanations."
    #     )
    # },
    # {"role": "user", "content": f"Convert this natural language query to an SQL query: {user_input}"}
            # {
            #     "role": "system",
            #     "content": (
            #         "You are an intelligent SQL query generator. Based on the user's natural language input, "
            #         "your task is to generate a valid SQL query for a PostgreSQL database. "
            #         "You have access to three tables in the schema `moshtriat`: `quotations`, `client_list`, and `sales_order_list`.\n\n"
                    
            #         "### Table Descriptions:\n"
            #         "1. `moshtriat.quotations`: Contains columns `quotation_id`, `created_by`, `creation_date`, `client_name`, `assigned_to`, "
            #         "`approval_status`, `pending_with`, `quotation_status`, and `sales_order`.\n"
            #         "2. `moshtriat.client_list`: Contains columns `client_id`, `client_name`, `contact`, `industry`, `address`, and `action`.\n"
            #         "3. `moshtriat.sales_order_list`: Contains columns `sales_order`, `created_by`, `creation_date`, `client_name`, `so_status`, "
            #         "`requested_date`, `actual_date`, and `delivery_status`.\n\n"
                    
            #         "### Column Matching Rules:\n"
            #         "- Use `client_name` for queries linking `quotations`, `client_list`, and `sales_order_list`.\n"
            #         "- For filtering by quotation-related attributes, use columns from `moshtriat.quotations`.\n"
            #         "- For client-specific details, use columns from `moshtriat.client_list`.\n"
            #         "- For sales order-specific details, use columns from `moshtriat.sales_order_list`.\n\n"
                    
            #         "### Instructions:\n"
            #         "1. Always prepend the schema name `\"moshtriat\"` before table names.\n"
            #         "2. Use JOINs where necessary to fetch data across multiple tables.\n"
            #         "3. Use valid column names and ensure SQL is syntactically correct for PostgreSQL.\n"
            #         "4. If the user query is ambiguous or references non-existent data, provide a friendly error message.\n"
            #         "5. Generate only the SQL query as output, without additional explanations."
            #     )
            # },
            # {"role": "user", "content": f"Convert this natural language query to an SQL query: {user_input}"}
    )
    
    generated_sql = response['choices'][0]['message']['content'].strip()

    # Clean up any possible ```sql and ``` markers
    if generated_sql.startswith("```"):
        generated_sql = generated_sql.strip("```sql").strip("```")
    
    return generated_sql.strip()

# API route to handle queries
@app.route('/query', methods=['POST'])
def handle_query():
    try:
        # Get user input from the request
        data = request.json
        user_input = data.get("user_input", "")
        
        if not user_input:
            return jsonify({"error": "No user input provided"}), 400

        # Generate SQL query from natural language
        sql_query = generate_sql_from_input(user_input)
        
        # Execute query and return results
        result_df = execute_query(sql_query)
        
        return jsonify({
            "sql_query": sql_query,
            "result": result_df.to_dict(orient="records")
        })
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Main driver for testing locally
def main():
    print("Welcome! Ask your query:")
    user_input = input()
    
    # Generate SQL query from natural language
    sql_query = generate_sql_from_input(user_input)
    print(f"Generated SQL Query: {sql_query}")

    try:
        # Execute query and show results
        result_df = execute_query(sql_query)
        print("Query Results:")
        print(result_df)
    except Exception as e:
        print(f"Error executing query: {str(e)}")

if __name__ == "__main__":
    # Uncomment the next line to test as a console app
    # main()

    # Run Flask app for API usage
    app.run(debug=True, use_reloader=False, port=5000)
