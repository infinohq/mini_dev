import requests
import argparse
import json
import websocket
import time
import os

IAI_BASE = "localhost:8000"
CONNECTOR_BASE = "localhost:7000"

def create_connection():
    connection = {
        "name": "snowflake",
        "account": os.environ.get("SNOWFLAKE_ACCOUNT"),
        "user": os.environ.get("SNOWFLAKE_USERNAME"),
        "password": os.environ.get("SNOWFLAKE_PASSWORD"),
        "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE"),
        "database": os.environ.get("SNOWFLAKE_DATABASE"),
        "schema": os.environ.get("SNOWFLAKE_SCHEMA"),
        "role": os.environ.get("SNOWFLAKE_ROLE")
    }
    url = f"http://{CONNECTOR_BASE}/_connectors/snowflake/connect"

    if connection.get("name") != "snowflake":
        raise ValueError("Connection name must be snowflake. We only support snowflake for now.")

    print("Connecting to Snowflake... The Snowflake DB must have birdbench data already ingested.")
    payload = json.dumps({
      "config": connection
    })
    headers = {
      'content-type': 'application/json',
      'x-infino-account-id': '000000000000'
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    connection_id = response.json()["connection_id"]
    print("Created connection to Snowflake with id: ", connection_id)

    return connection_id

def create_thread(connection_id):
    print("Creating thread for connection: ", connection_id)
    url = f"http://{IAI_BASE}/_conversation/threads"

    payload = json.dumps({
        "index_name": "CUSTOMERS", # Any index name should work here
        "connection_id": connection_id,
        "name": f"Snow Conn{connection_id}"
    })
    headers = {
        'content-type': 'application/json',
        'x-infino-account-id': '000000000000',
        'x-infino-username': 'admin'
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    thread_id = response.json()["id"]
    print("Created thread with id: ", thread_id)
    return thread_id

def setup_fino():
    print("Setting up Fino...")
    connection_id = create_connection()
    thread_id = create_thread(connection_id)
    return thread_id

def generate_sql(thread_id, query, summary):
    print("Generating SQL queries for thread: ", thread_id)
    url = f"ws://{IAI_BASE}/_conversation/ws"
    headers = {
        'content-type': 'application/json',
        'x-infino-account-id': '000000000000',
        'x-infino-username': 'admin',
        'x-infino-thread-id': thread_id,
        'x-infino-client-id': 'birdbench'
    }

    ws = websocket.WebSocket()
    ws.connect(url, header=headers)
    ws.send(json.dumps(
        {
            "content": {
                "user_query": query,
                "type": "user",
                "summary": summary,
                "data": {},
                "vegaspec": {},
                "querydsl": {},
                "followup_queries": [],
                "sender_agent": "user",
                "user_context": {
                    "viz_querydsl": {},
                    "syntax": "sql",
                    "model": "auto"
                }
            }
        }
    ))

    while True:
        time.sleep(1)
        try:
            response = ws.recv()
        except websocket.WebSocketConnectionClosedException:
            print("WebSocket connection closed. Reconnecting...")
            ws.connect(url, header=headers)
            continue
        except Exception as e:
            print(e)
            break
        json_response = json.loads(response)
        content = json_response.get("content")
        if content == None:
            continue

        if content.get("type") == "result":
            print("Received result: ")
            print(content)
            generated_sql = content.get("sql")
            ws.close()
            print("Generated SQL: ", generated_sql)
            return generated_sql

def parse_questions(datasets, metadata):
    question_list = []
    for i, data in enumerate(datasets):
        database_group_id = data["db_id"]
        metadata_item = next((item for item in metadata if item["db_id"] == database_group_id), {})
        tables = metadata_item.get("table_names_original", [])
        summary = data.get("evidence", "")
        question = data.get("question", "")
        if len(tables) > 0:
            table_names = ", ".join(tables)
            summary += f"\n Use the following tables {table_names.upper()}."

        question = {
            "query": question,
            "summary": summary,
        }
        question_list.append(question)

    return question_list

def generate_sql_file(sql_lst, output_path=None):
    """
    Function to save the SQL results to a file.
    """
    # We dont need to sort at the moment because we're running queries sequentially
    # sql_lst.sort(key=lambda x: x[1])
    result = {}
    for i, sql in enumerate(sql_lst):
        result[i] = sql

    if output_path:
        directory_path = os.path.dirname(output_path)
        if not os.path.exists(directory_path):
            os.makedirs(directory_path)
        json.dump(result, open(output_path, "w"), indent=4)

    return result


if __name__ == "__main__":
    args_parser = argparse.ArgumentParser()
    args_parser.add_argument("--eval_path", type=str, default="")
    args_parser.add_argument("--sql_dialect", type=str, default="SQLite")
    args_parser.add_argument("--metadata_path", type=str)
    args_parser.add_argument("--data_output_path", type=str)
    args = args_parser.parse_args()

    print("Parsing evaluation questions at ", args.eval_path)
    eval_data = json.load(open(args.eval_path, "r"))
    metadata = json.load(open(args.metadata_path, "r"))
    questions = parse_questions(datasets=eval_data, metadata=metadata)
    print(questions)

    # Set up Fino for query generation
    print("Setting up Fino for query generation")
    thread_id = setup_fino()


    print("Generating SQL queries using Fino...")
    responses = []
    for question in questions:
        print(f"Generating SQL query for question: {question}")
        # Generate SQL queries
        sql = generate_sql(thread_id, question["query"], question["summary"])
        responses.append(sql)

    output_path = (
        args.data_output_path
        + "predict_"
        + "_"
        + args.sql_dialect
        + ".json"
    )
    print(responses)
    generate_sql_file(sql_lst=responses, output_path=output_path)
    print("Successfully generated SQL queries. Saved at ", output_path)
