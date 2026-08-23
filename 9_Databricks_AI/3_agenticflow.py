# Databricks notebook source
# MAGIC %md
# MAGIC ![](Autonomous Ecom Agent Workflow.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Autonomous Customer Support Agentic Resolution Engine
# MAGIC
# MAGIC **1. Major AI Concepts Used:**
# MAGIC Agentic AI (Single Agent): Autonomous control over specific tools to achieve the goal. It Reasons & Act to solve the user's problem before generating the final response.
# MAGIC
# MAGIC Tool Calling (Function Calling): The LLM is provided with a JSON schema defined  functions (get_order_details, check_inventory, etc.).
# MAGIC
# MAGIC ReAct Loop (Reason + Act) Think -> Decide -> Act -> Observe -> Finish: The agent Reasons about the current state, decides to Act (call a tool), Observes the output of that tool, and then repeats the process until it has enough information to provide a final answer.
# MAGIC
# MAGIC Medallion Architecture:
# MAGIC Bronze: Raw ingestion of data (ecommerce_raw_reviews).
# MAGIC Gold: Highly refined, actionable data resulting from the AI processing (ecommerce_agent_response).
# MAGIC
# MAGIC 2. LLM Model: Meta Llama 3.3
# MAGIC
# MAGIC Model Size: 70 Billion Parameters (70B) for complex reasoning and reliable tool calling capabilities.
# MAGIC
# MAGIC Variant: Instruct (Instruction tuned to follow directions and act as an assistant).
# MAGIC
# MAGIC Hosting Platform: Databricks Model Serving (via Foundation Model APIs). The model is hosted securely within the Databricks environment, accessed via an OpenAI-compatible client.
# MAGIC
# MAGIC 3. Types of Prompting Techniques Used
# MAGIC The system_prompt
# MAGIC
# MAGIC Role Prompting (Persona): `"You are an autonomous customer support agent."*
# MAGIC
# MAGIC Dynamic prompt Injection (Grounding): "...The customer is writing about Order ID: {order_id}."
# MAGIC
# MAGIC Instruction Prompting for Tool Use: "Use tools to investigate issues before responding. Never guess order details."
# MAGIC
# MAGIC Guardrail / Negative Prompting: "Do not give cash refunds."
# MAGIC
# MAGIC Conditional Logic Prompting: "If an item is out of stock, issue store credit automatically and give the customer the code."

# COMMAND ----------

# ==============================================================================
# 1. SETUP THE DATABASES
# ==============================================================================
print("1. Creating and loading the Delta Tables...")
# (Tables setup remains the same as your previous working version)

# Insert sample customer reviews with their associated order IDs
spark.sql("CREATE OR REPLACE TABLE lakehousecat.default.ecommerce_raw_reviews (review_id STRING, order_id STRING, customer_review STRING)")
spark.sql("""INSERT INTO lakehousecat.default.ecommerce_raw_reviews VALUES 
  ('REV-001', 'ORD-111', 'The shoes are beautiful, but they ripped after two days of wearing them! I want my money back.'),
  ('REV-002', 'ORD-222', 'Shipping took 3 weeks. Absolutely unacceptable.'),
  ('REV-003', 'ORD-333', 'Perfect fit! Will definitely be buying from you guys again.'),
  ('REV-004', 'ORD-555', 'I received the wrong color. I ordered black but got blue. How do I fix this?')""")

spark.sql("""INSERT INTO lakehousecat.default.ecommerce_raw_reviews VALUES 
  ('REV-001', 'ORD-112', 'My Name is Irfan')""")

# Create the orders table to act as our internal e-commerce database
spark.sql("CREATE OR REPLACE TABLE lakehousecat.default.ecommerce_orders (order_id STRING, customer_id STRING, item STRING, ordered_color STRING, shipped_color STRING, price DOUBLE)")
spark.sql("""INSERT INTO lakehousecat.default.ecommerce_orders VALUES 
  ('ORD-111', 'CUST-10', 'Dress Shoes', 'Brown', 'Brown', 90.0),
  ('ORD-222', 'CUST-45', 'Sneakers', 'White', 'White', 60.0),
  ('ORD-333', 'CUST-88', 'Boots', 'Black', 'Black', 150.0),
  ('ORD-555', 'CUST-99', 'Running Shoes', 'Black', 'Blue', 120.0)""")
display(spark.read.table("lakehousecat.default.ecommerce_raw_reviews"))
display(spark.read.table("lakehousecat.default.ecommerce_orders"))

# COMMAND ----------

import json
import os
from openai import OpenAI
from pyspark.sql import Row
# ==============================================================================
# 2. DEFINE THE AGENT'S TOOLS (Defining Python Functions & Mapping Python Functions with the Agents Arm using Aent JSON)
# ==============================================================================
print("2. Giving AI - the Hands and tools ...")
# Define a tool to fetch order details directly from the table
def get_order_details_py_func1(order_id, **kwargs):
    print(f"[Tool1] Executing Database Lookup for Order: {order_id}")
    res = spark.sql(f"SELECT * FROM lakehousecat.default.ecommerce_orders WHERE order_id = '{order_id}'").first()
    return json.dumps(res.asDict()) if res else json.dumps({"error": "Order not found"})
# Define a tool to check warehouse inventory
def check_inventory(item_name, color, **kwargs):
    #Actually this function can be extended to check for the item or color is available in the table or not.
    print(f"[Tool2] Checking Warehouse for {item_name} in {color}")
    return json.dumps({"item": item_name, "color": color, "in_stock": False})
# Define a tool to simulate issuing store credit to a customer
def issue_store_credit(customer_id, amount, **kwargs):
    print(f"[Tool3] Issuing ${amount} store credit to Customer: {customer_id}")
    return json.dumps({"status": "Success", "credit_code": "CREDIT-XYZ-123", "amount": amount})
def dummy_function():
    pass

# Create the JSON schema that strictly defines these tools for the LLM to understand
#type : Specifies the type of structure ("function" or "AgenticTool").
#name: A unique, descriptive name for the tool (e.g., "get_weather" or "ScientificTextSummarizer").
#description: A clear, human-readable description of what the tool does. The LLM uses this description to decide when to use the tool.
#input_schema (or parameters): A JSON Schema object that strictly defines the expected input arguments for the tool.
#type : Must be "object".
#properties: An object where each key is an argument name, and its value defines the data type (e.g., "string", "number") and a description.
#required: A list of strings specifying which properties are mandatory for the tool to function correctly.
agent_tools_json = [
    {
        "type": "function",
        "function": {
            "name": "get_order_details_tool1",
            "description": "Look up an order by its ID.",
            "parameters": {
                "type": "object",
                "properties": {
                    "order_id": {"type": "string"},
                    "justification": {"type": "string", "description": "Explain exactly why you need to call this tool."}
                },
                "required": ["order_id", "justification"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "check_inventory",
            "description": "Check if an item/color is in stock. MUST call this before issuing credit for wrong/damaged items.",
            "parameters": {
                "type": "object",
                "properties": {
                    "item_name": {"type": "string"},
                    "color": {"type": "string"},
                    "justification": {"type": "string", "description": "Explain exactly why you need to call this tool."}
                },
                "required": ["item_name", "color", "justification"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "issue_store_credit",
            "description": "Issue store credit if replacement is out of stock.",
            "parameters": {
                "type": "object",
                "properties": {
                    "customer_id": {"type": "string"},
                    "amount": {"type": "number"},
                    "justification": {"type": "string", "description": "Explain exactly why you need to call this tool."}
                },
                "required": ["customer_id", "amount", "justification"]
            }
        }
    } ]

# Map the string names from the JSON schema to the actual Python functions (Mapping Tools(Py function code) with the Arms(AgenticTool/Function))
available_functions_mapping = {"get_order_details_tool1": get_order_details_py_func1, "check_inventory": check_inventory, "issue_store_credit": issue_store_credit}

# ==============================================================================
# 3. CONFIGURE THE LLM CLIENT & AGENT LOGIC (Brain attached with the Arms(Tools) (Tools already attached with Functions))
# ==============================================================================
#Client Chrome Browser : OpenAI Client
#Password to reach the Server: Databricks Token/Api Key
#Proxy/Network Route: https://3631047205072094.ai-gateway.cloud.databricks.com/mlflow/v1/ (AI API Gateway)
#Server: databricks-meta-llama-3-3-70b-instruct

def run_ecommerce_agent(customer_review, order_id, api_token):
    client = OpenAI(api_key=api_token, base_url="https://3631047205072094.ai-gateway.cloud.databricks.com/mlflow/v1/")
    
    #Prompt Engineering: Strict instructions to force the detailed reasoning behavior
    system_prompt = f"""You are an autonomous customer support agent for Order ID: {order_id}.
    1. ALWAYS look up the order details first.
    2. If an item is damaged or the wrong color, you MUST check inventory for a replacement.
    3. If and ONLY IF the inventory is out of stock, issue store credit.
    4. You must provide a clear, professional justification for every tool call."""
    

    system_user_agent_prompt = [{"role": "system", "content": system_prompt},
        {"role": "user", "content": customer_review}]
    '''system_user_prompt=f"""You are an autonomous customer support agent for Order ID: {order_id}.
    1. ALWAYS look up the order details first.
    2. If an item is damaged or the wrong color, you MUST check inventory for a replacement.
    3. If and ONLY IF the inventory is out of stock, issue store credit.
    4. You must provide a clear, professional justification for every tool call.
    User prompt: Ord-123 - The shoes are beautiful, but they ripped after two days of wearing them! I want my money back.
    Agent Prompt1: Order found {order information}
    Agent Prompt2: No such color present in our warehouse
    Agent Prompt3: let me crete store credit for the given amount
    """
    '''
    #ReAct Loop
    # Reasoning Loop: The agent is given up to 5 turns to solve the problem. This prevents "infinite loops".
    #Step A (Think): We send the prompt and the tools to the Databricks-hosted Llama 3.3 70B model.
    #Step B (Decide): The AI will decide to use the tool or without using tool just produce the result.

    #Actioning Loop: 
    #Step C (Act): If a tool was requested, Python code extracts the tool name and arguments, executes the corresponding function from available_functions, and gets the JSON result (e.g., the order details from the database).
    
    #Step D (Observe): The JSON result is appended to the master prompt, and the loop starts over. The AI reads the database result and "thinks" about what to do next.

    #Step E (Finish): If the AI has all the data it needs and resolves the issue, it outputs a standard text response. The loop breaks, and the final email draft is returned.
    
    for i in range(5):#To avoid infinate ReAct loop in a corner cases.
        #Re Part (Reasoning part - Think & Decide)
        #LLM Inference & Tool Choice
        #Step A (Think): Understand the prompt and think of using the tools
        #Step B (Decide): Tool Call
        response = client.chat.completions.create(model="databricks-meta-llama-3-3-70b-instruct", messages=system_user_agent_prompt, tools=agent_tools_json)        
        #Example for Reason - Aspirants are telling irfan (you are a trainer, when u r delivering something, i am not to hear) -> Irfan using his LLM(Brain) first Think (voice is not good, so i have to use my tools) then Tool Call/Select ( internet strength -> mic -> zoom tool)
        #print(response)
        #print("LLM Response",response)
        # message response
        msg = response.choices[0].message
        #print(msg)
        #print("system_user_agent_prompt",system_user_agent_prompt)
        ##Step E (Finish): No Tools Called - Agent reached early conclusion for exit without calling any tools
        if not msg.tool_calls:
            print(f"\n FINAL AGENT RESPONSE:\n{msg.content}")
            return msg.content
                
        #Step D (Observe): append the LLM's thought/decision to the messages history, needed for maintaining the conversation history (Now we will have system prompt, user prompt & agent prompt also appended in this system_user_agent_prompt variable)
        system_user_agent_prompt.append(msg.model_dump(exclude_none=True))
        
        #Step C (Act):
        for tool in msg.tool_calls:
            #Extraction of tool arguments
            args = json.loads(tool.function.arguments)
            #Justification (if nothing then default no justification)
            print(f"AGENT JUSTIFICATION just for our understanding of the agent's reasoning: {args.get('justification', 'No justification provided.')}")
            #looks up the function object in the dictionary defined earlier
            result = available_functions_mapping[tool.function.name](**args)#Real action part of calling respective tool/functions based on the agent justification (mapping of function description with the agent justification happen here)

            #Step D (Observe): append the iteration of messages
            #Tool call id - shows the tool request this result belongs to
            system_user_agent_prompt.append({"role": "tool", "tool_call_id": tool.id, "name": tool.function.name, "content": result})

# ==============================================================================
# 4. EXECUTE THE PIPELINE
# ==============================================================================
print("\n2. Reading raw data and starting Agentic Pipeline...")

reviews_list = spark.read.table("lakehousecat.default.ecommerce_raw_reviews").collect()
processed_records = []

DATABRICKS_TOKEN = dbutils.secrets.get(scope="izgenaiscope", key="databricks_token")

for row in reviews_list:
    print("\n" + "="*60)
    print(f"NEW TICKET: {row['customer_review']}")
    print(f"ATTACHED ORDER ID: {row['order_id']}")
    print("-" * 60)
    
    reply = run_ecommerce_agent(row['customer_review'], row['order_id'], DATABRICKS_TOKEN)
    processed_records.append({"review_id": row['review_id'], "order_id": row['order_id'], "agent_response": reply})

df_gold = spark.createDataFrame([Row(**r) for r in processed_records])
df_gold.write.mode("overwrite").saveAsTable("lakehousecat.default.agents_responces")
display(df_gold)

# COMMAND ----------

# MAGIC %md
# MAGIC ![](agent flow.jpg)

# COMMAND ----------

# MAGIC %md
# MAGIC **1. The Data Foundation (Bronze Tables)**
# MAGIC Before an AI can do anything, it needs data. This section acts as your Data Warehouse setup.
# MAGIC
# MAGIC ecommerce_raw_reviews: This is your "Bronze" input table. It contains the incoming customer complaints, the specific review_id, and critically, the order_id.
# MAGIC
# MAGIC ecommerce_orders: This acts as your company's internal database. It holds the ground truth about what a customer ordered, what they paid, and what was actually shipped.
# MAGIC
# MAGIC What the code does: It uses spark.sql() to create these tables in Databricks and inserts a few mock rows so the Agent has real data to practice on.
# MAGIC
# MAGIC **2. The Tools (Giving the AI "Hands")**
# MAGIC An LLM by itself is just a text generator. To make it an "Agent," you have to give it tools to interact with the outside world.
# MAGIC
# MAGIC **Universal JSON schema template for defining a tool.**
# MAGIC {
# MAGIC     "type": "function",
# MAGIC     "function": {
# MAGIC         "name": "your_function_name_here",
# MAGIC         "description": "A highly detailed explanation of WHAT this tool does, and WHEN the AI should decide to use it.",
# MAGIC         "parameters": {
# MAGIC             "type": "object",
# MAGIC             "properties": {
# MAGIC                 "first_parameter_name": {
# MAGIC                     "type": "string", 
# MAGIC                     "description": "Explain what this specific variable is (e.g., 'The customer ID')."
# MAGIC                 },
# MAGIC                 "second_parameter_name": {
# MAGIC                     "type": "number", 
# MAGIC                     "description": "Explain this variable (e.g., 'The refund amount in dollars')."
# MAGIC                 },
# MAGIC                 "third_parameter_name": {
# MAGIC                     "type": "boolean",
# MAGIC                     "description": "Explain this variable (e.g., 'True if expedited shipping, False otherwise')."
# MAGIC                 }
# MAGIC             },
# MAGIC             "required": [
# MAGIC                 "first_parameter_name", 
# MAGIC                 "second_parameter_name"
# MAGIC             ]
# MAGIC         }
# MAGIC     }
# MAGIC }
# MAGIC
# MAGIC The Python Functions: Defined three capabilities:
# MAGIC
# MAGIC get_order_details: This is the most powerful tool. It takes an order_id, runs a real PySpark SQL query against your ecommerce_orders table, and returns the data.
# MAGIC
# MAGIC check_inventory: Simulates checking a warehouse system.
# MAGIC
# MAGIC issue_store_credit: Simulates hitting a billing API to generate a refund code.
# MAGIC
# MAGIC The agent_tools Schema: The LLM cannot read Python code. This JSON list acts as a "user manual" for the AI. It tells the AI the names of the tools, what they do, and exactly what parameters (like order_id) it must provide to use them.
# MAGIC
# MAGIC available_functions: A simple Python dictionary that maps the string name the AI generates to the actual Python function that needs to be executed.
# MAGIC
# MAGIC **3. The Brain (The ReAct Agent Loop)**
# MAGIC This is the core engine of the system. The run_ecommerce_agent function handles the reasoning and execution.
# MAGIC
# MAGIC Dynamic Grounding: It takes the order_id from the data row and injects it directly into the system_prompt. This tells the AI exactly which order to investigate immediately.
# MAGIC
# MAGIC The for step in range(5) Loop: This is the ReAct (Reason + Act) loop. We limit it to 5 steps so the AI doesn't get confused and run forever.
# MAGIC
# MAGIC Step A (Think): We send the prompt and the tools to the Databricks-hosted Llama 3.3 70B model.
# MAGIC
# MAGIC Step B (Decide): The AI responds. If it needs data, it doesn't return text; it returns a tool_call requesting to run a specific function.
# MAGIC
# MAGIC Step C (Act): If a tool was requested, your Python code extracts the tool name and arguments, executes the corresponding function from available_functions, and gets the JSON result (e.g., the order details from the database).
# MAGIC
# MAGIC Step D (Observe): The JSON result is appended to the messages history, and the loop starts over. The AI reads the database result and "thinks" about what to do next.
# MAGIC
# MAGIC Step E (Finish): If the AI has all the data it needs and resolves the issue, it outputs a standard text response. The loop breaks, and the final email draft is returned.
# MAGIC
# MAGIC 4. The Orchestrator (Executing the Pipeline)
# MAGIC This final section glues the Data Engineering and the AI together.
# MAGIC
# MAGIC Fetch the Backlog: It reads the ecommerce_raw_reviews table into memory using .collect().
# MAGIC
# MAGIC The Processing Loop: It iterates through every single review one by one. For each row, it extracts the customer_text and order_id, and hands them to the Agent (run_ecommerce_agent).
# MAGIC
# MAGIC Saving the Results (Gold Table): As the Agent finishes each ticket, the results are stored in a list. Once all tickets are processed, that list is converted back into a PySpark DataFrame and saved as ecommerce_agent_response (your polished, "Gold" data).
