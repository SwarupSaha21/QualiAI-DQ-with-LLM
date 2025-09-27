from langgraph.graph import StateGraph, END,START
from langchain_openai import ChatOpenAI
import anthropic
from openai import OpenAI
import duckdb
from dotenv import load_dotenv
from IPython.display import Image
import os
from langchain_community.document_loaders import CSVLoader
import json

load_dotenv()
# shared state

class AgentState(dict):
    user_input: str
    sql_query: str
    error: str | None
    result: str | None
    col_names:list
    data_load_ack:str
    ruleset_str: str
    dataset_path: str
    ruleset_path:str

# loader  = CSVLoader(file_path='F:\langgraph_edenmarco\duckdb_dq\dq_ruleset.csv')

# Step1: ingest data to duckdb
def ingest_data(state:AgentState):
    # Path to your CSV file
    csv_path = "F:\DQ_WITH_LLM\input_docs\dummy_address.csv"

    # Path to your DuckDB database file (creates one if it doesn't exist)
    db_path = "my_database.duckdb"

    # Connect to DuckDB
    conn = duckdb.connect(db_path)

    # Read the CSV into DuckDB and store it as a table
    conn.execute(f"""
        CREATE OR REPLACE TABLE table100 AS
        SELECT * FROM read_csv_auto('{csv_path}', ALL_VARCHAR=TRUE)
    """)

    return {**state, "data_load_ack": "successfully imported data", "error": None}

#  fetch column names from table
def fetch_col_names(state:AgentState):
    """Fetch column names from the table created"""
    
    # Path to your DuckDB database file (creates one if it doesn't exist)
    db_path = "my_database.duckdb"

    # Connect to DuckDB
    conn = duckdb.connect(db_path)
    columns = conn.execute("PRAGMA table_info(table100)").fetchall()
    cols = [col[1] for col in columns]
    column_names =  str(cols)

    return {**state, "col_names": column_names, "error": None}


def parse_ruleset(state:AgentState):
    loader  = CSVLoader(file_path='F:\DQ_WITH_LLM\input_docs\dq_ruleset.csv')
    data = loader.load()
    str_pg=''
    for row in data:
        row_pg = row.page_content
        str_pg=str_pg + '\n' +row_pg

    lines = str_pg.strip().splitlines()
    data = {}
    key, value = None, None
        

    for line in lines:
        line = line.strip()
        if not line:  # skip empty lines
            continue
        if line.startswith("Field:"):
            key = line.split(":", 1)[1].strip()
        elif line.startswith("Rule:"):
            value = line.split(":", 1)[1].strip()
            if key and value:  # only add if both exist
                data[key] = value
                key, value = None, None  # reset for next block
    
    return {**state, "ruleset_str": json.dumps(data, indent=4)}
    

# Step 2: Generate SQL
def generate_sql(state: AgentState):
    llm = OpenAI()
    """Call LLM to generate SQL query based on prompt."""

    prompt = state['ruleset_str']

    prompt_col_names="The column names for the table are given as a list as below: "

    role_prompt = """
            You are an expert in writing SQL scripts in duckdb to handle complex transformations. as per the given prompt generate a sql command to fetch rows that do not match atleast one of the given criterias as well as the rows that match.
            Please write SQL syntax strictly compatible with duckdb and nothing else. Do not include functions like REGEXP as they are not compatible with duck db
            The keys of the dictionary are column name while the values are the rules the column names are given in the prompt as keys of a dictionary while the table name is table100.
            Every column in the table is of STRING data type so write the query keeping that in mind
            You should also create a new field called Reject_Reason and add the reason for rejection for each row if it is not satisfying any of the given requirements.
            In case of records that have more than 1 reject reason, concatenate them in one field. DO NOT create a new row for that
            For rows that satisfy all conditions, keep the Reject_Reason column value blank. Basically, the query should return all the rows whether they satisfy the conditions or not.
            The reject reason should be very specific so you should write separate cases for each check. for example, if the city field is null, the reject reason should be City cannot be empty whereas if the characters exceed for the field the message should be different like City cannot have more than say X characters.
            Your output should only be the SQL query and nothing else. No need to give any explanations of any step, keep the output as short as possible
    """

    response = llm.chat.completions.create(
        model="gpt-4o",  # Or any available model
        messages=[
            {"role": "system", "content": role_prompt},
            {"role": "user", "content": prompt},
            {"role": "user", "content": state['user_input']},
            {"role": "user", "content": prompt_col_names + state['col_names']}
        ],
        temperature=0.2
    )

    sql_query = response.choices[0].message.content.strip()

    return {**state, "sql_query": sql_query, "error": None}

def execute_sql(state: AgentState):
    """Execute the generated sql in duck db"""
    raw_sql = state['sql_query']

    if raw_sql.startswith("```") and raw_sql.endswith("```"):
        clean_sql_query =  "\n".join(raw_sql.split("\n")[1:-1]).strip()

    # Path to your DuckDB database file (creates one if it doesn't exist)
    try:
        db_path = "my_database.duckdb"

        # Connect to DuckDB
        conn = duckdb.connect(db_path)
        res = conn.execute(clean_sql_query).fetchdf()

        res.to_csv("F:\DQ_WITH_LLM\output\output_new.csv", index=False)


        return {**state, "result": "CSV file successfully written to path", "error": None}
    
    except Exception as e:
        return {**state, "error": str(e), "result": None}
    

def remediate_sql(state: AgentState):
    evaluator_llm = anthropic.Anthropic(
    api_key=os.getenv("ANTHROPIC_API_KEY")
    )
    error = state["error"]
    bad_query = state["sql_query"]
    role_prompt = """
        You are an expert in writing SQL queries and have a keen eye for details.
        Your job is to debug SQL queries looking for all types of errors.
        You are job also involves optimizing the queries,if required.
        Always generate code that is compatible with duckdb and NOTHING ELSE.
        Please ensure that your output is just the code and nothing else, no need to explain any step, just plain code and keep everything concise as possible.
    """
    prompt = f"""
    The following SQL query caused an error in DuckDB:
    Query: {bad_query}
    Error: {error}
    Please fix the SQL query.
    """
    response_claude = evaluator_llm.messages.create(
    model="claude-sonnet-4-20250514",  # or claude-3-haiku-20240307, claude-3-opus-20240229
    max_tokens=1000,  # Required parameter for Anthropic
    temperature=0.2,
    system=role_prompt,  # System message is a separate parameter
    messages=[
        {"role": "user", "content": prompt}
        ]
    )

    # Access the response content
    fixed_sql = response_claude.content[0].text

    if fixed_sql.startswith("```") and fixed_sql.endswith("```"):
        clean_fixed_sql_query =  "\n".join(fixed_sql.split("\n")[1:-1]).strip()
    return {**state, "sql_query": clean_fixed_sql_query, "error": None}


def route_node(state: AgentState):
    if state['error'] == None :
        return 'approved'
    else:
        return 'fix needed'
    
workflow = StateGraph(AgentState)


workflow.add_node('ingest_data',ingest_data)
workflow.add_node('fetch_col_names',fetch_col_names)
workflow.add_node('parse_ruleset',parse_ruleset)
workflow.add_node('generate_sql',generate_sql)
workflow.add_node('execute_sql',execute_sql)
workflow.add_node('remediate_sql',remediate_sql)

workflow.add_edge(START,"ingest_data")
workflow.add_edge("ingest_data","fetch_col_names")
workflow.add_edge("fetch_col_names","parse_ruleset")
workflow.add_edge("parse_ruleset","generate_sql")
workflow.add_edge("generate_sql","execute_sql")

workflow.add_conditional_edges("execute_sql",route_node,{"approved":END,"fix needed":"remediate_sql"})

workflow.add_edge("remediate_sql","execute_sql")


app = workflow.compile()

result = app.invoke({'user_input': '*any additional information*'})
print(result['sql_query'])

# Image(app.get_graph().draw_mermaid_png())

