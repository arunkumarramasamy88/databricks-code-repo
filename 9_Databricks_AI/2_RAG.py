# Databricks notebook source
# MAGIC %md
# MAGIC ![](rag pipeline1.png)

# COMMAND ----------

# MAGIC %md
# MAGIC **Large Language Models (LLMs)** do not know about our private company data, it will either hallucinate or say it doesn't know. RAG solves this like an open-book test for the AI.
# MAGIC
# MAGIC Steps of RAG:
# MAGIC - Reads our private PDFs.
# MAGIC - It stores it in a searchable "Vector Database".
# MAGIC - When a user asks a question, it searches the database for the exact paragraphs related to the question.
# MAGIC - LLM "Answer the user's question using only this text."

# COMMAND ----------

# DBTITLE 1,Cell 1
# MAGIC %pip install langchain-text-splitters langchain_community pypdf databricks-vectorsearch
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

#Langchain is used for - Data Connection (RAG), Chaining Actions, Agentic Behavior & Memory management
from langchain_community.document_loaders import PyPDFLoader
from langchain_text_splitters import RecursiveCharacterTextSplitter
from pyspark.sql.functions import monotonically_increasing_id
from databricks.vector_search.client import VectorSearchClient
import mlflow.deployments
import time

# --- CONFIGURATION ---
CATALOG = "catalog1_we47"
SCHEMA = "default"
VOLUME = "policy_pdfs"
TABLE_NAME = f"{CATALOG}.{SCHEMA}.policy_chunks"
ENDPOINT_NAME = "hr_policy_vs_endpoint"
INDEX_NAME = f"{CATALOG}.{SCHEMA}.policy_index7"

#Step 1: Configuration & Data Ingestion (Chunking)
# 1. INGESTION
#Document Loading: LangChain extracts the raw text directly from the PDF.
#pdf_path = "/Volumes/catalog1_we47/default/docs/HR Policy Manual 2025.pdf"
pdf_path="/Volumes/catalog1_we47/default/docs/lic policy document.pdf"
loader = PyPDFLoader(pdf_path)#Instantiating the class as an object with path as argument
docs = loader.load()#Inside the object we are calling a function.

#Chunking: To fit the LLM's size limits (context window), the text is split into 1000-character chunks. A 100-character overlap ensures context isn't lost when sentences are cut.
text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=100)
#hello team, good morning, we have a policy of starting session at 7 am ist
#chunk_size=20 & overlap 10 - (hello team, good mor),(, good morning, we have a),(ave a policy of),(policy of starting session),(session at 7 am ist),(7 am ist))
chunks = text_splitter.split_documents(docs)

#The chunks are saved into a Databricks Delta Table.
data = [{"content": c.page_content, "source": c.metadata['source']} for c in chunks]
#{"content":"some chunk of content from the pdf","source":"deltafile location"}
df = spark.createDataFrame(data).withColumn("id", monotonically_increasing_id())
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(TABLE_NAME)

#enableChangeDataFeed: This allows the Vector Database to automatically update with only the incremental (updated/inserted/deleted data alone, without boiling the entire index content).
spark.sql(f"ALTER TABLE {TABLE_NAME} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
display(spark.sql("select * from catalog1_we47.default.policy_chunks"))

#Regular Data engineering work is completed...
#Elevated - AI - Data engineering work is start...
# 2. VECTOR SEARCH SETUP
vsc = VectorSearchClient()

# Check if endpoint exists, if not create it else use the existing endpoint
#endpoint - Database of vector tables/indexes, through which we can access vector indexes.
existing_endpoints = [e['name'] for e in vsc.list_endpoints().get('endpoints', [])]
if ENDPOINT_NAME not in existing_endpoints:
    print(f"Creating endpoint {ENDPOINT_NAME}...")
    vsc.create_endpoint(name=ENDPOINT_NAME, endpoint_type="STANDARD")

# Create Index to store Embeddings (Converts text into numerical vectors) for future semanting search operations (REETIVAL)
#Vector Index: A specialized database for vectors

print("Creating/Syncing Vector Index...")
vsc.create_delta_sync_index(
    endpoint_name=ENDPOINT_NAME,
    source_table_name=TABLE_NAME,
    index_name=INDEX_NAME,
    pipeline_type='TRIGGERED',
    primary_key="id",
    embedding_source_column="content",
    embedding_model_endpoint_name="databricks-bge-large-en")

print(INDEX_NAME)


# COMMAND ----------

#Semantic Search (RAG without LLM)
#We are achieving only R or RAG (Retrival part of the Retrival Augmented Generation)
#We are not using LLM here, only using Vector Search (Vector Index)

from databricks.vector_search.client import VectorSearchClient
import mlflow.deployments
import time

CATALOG = "catalog1_we47"
SCHEMA = "default"
INDEX_NAME = f"{CATALOG}.{SCHEMA}.policy_index7"
ENDPOINT_NAME = "hr_policy_vs_endpoint"

# 2. Re-initialize the Vector Search Client (to make this cell run individually)
vsc = VectorSearchClient()

def hr_retrival_only(question):
    # 1. Initialize Vector Search Client (No MLflow/LLM needed here)
    vsc = VectorSearchClient()
    index = vsc.get_index(endpoint_name=ENDPOINT_NAME, index_name=INDEX_NAME)
    
    # 2. Semantic Search: Find the most relevant fragments
    results = index.similarity_search(
        query_text=question, 
        columns=["content", "source"], 
        num_results=3 #Only returns top3 similarity scored results             
    )
    
    # 3. Return the raw data
    return results.get('result', {}).get('data_array', [])

# Actively wait for the index to be ready before executing the RAG function
print("Waiting for index to synchronize... this may take several minutes for a new index.")
index = vsc.get_index(endpoint_name=ENDPOINT_NAME, index_name=INDEX_NAME)
while True:
    status = index.describe().get('status', {})
    is_ready = status.get('ready', False)
    detailed_state = status.get('detailed_state', 'UNKNOWN')
    
    # Exit loop if ready
    if is_ready and detailed_state.startswith("ONLINE"):
        print("\nSystem ready! Index is ONLINE....")
        break
        
    # Exit loop if it failed
    if "FAILED" in detailed_state or "ERROR" in detailed_state:
        print(f"\nSync Failed! State: {detailed_state}")
        print(f"Error Message: {status.get('message', 'No details provided by Databricks.')}")
        break
        
    # Give detailed visibility into the current process
    print(f"Still syncing... Current phase: {detailed_state}")
    time.sleep(10)

search_results = hr_retrival_only("what is the installment tenure in days?")
for i, res in enumerate(search_results):
    print(f"Result {i+1} (Source: {res[1]}):\n{res[0]}\n")

# COMMAND ----------

#Do Retrival + Augument & Generation also, so user can understand the final result of what he needed...

from databricks.vector_search.client import VectorSearchClient
import mlflow.deployments
import time

CATALOG = "catalog1_we47"
SCHEMA = "default"
INDEX_NAME = f"{CATALOG}.{SCHEMA}.policy_index7"
ENDPOINT_NAME = "hr_policy_vs_endpoint"

# 2. Re-initialize the Vector Search Client
vsc = VectorSearchClient()

# 3. BOT LOGIC (Semantic Search)
def do_retrival_vectorindex_augument_llm_generate_nlg(question):
    # Initialize the MLflow deployment client
    client = mlflow.deployments.get_deploy_client("databricks")
    
    # Referring the index created above
    index = vsc.get_index(endpoint_name=ENDPOINT_NAME, index_name=INDEX_NAME)
    
    # Semantic Search & Data RETRIVAL
    #Search: Finds the top 3 text chunks most relevant to our question.
    results = index.similarity_search(query_text=question, columns=["content"], num_results=3)
    #Extract: Safely pulls the actual data array out of the search results' dictionary structure.
    docs = results.get('result', {}).get('data_array', [])
    #Format Context: Stitches those 3 separate text chunks into one single string
    index_retrived_context = "\n---\n".join([doc[0] for doc in docs]) if docs else "No context found."
    
    # llm & NLG
    #Prompt Construction: Combines retrieved context and the user's question into a single instruction to forces GenAI(LLM) to base its answer strictly on our documents.
    prompt = f"Answer using context:\nContext: {index_retrived_context}\nQuestion: {question}"
    #LLM Generation: Sends this tailored prompt LLM.
    response = client.predict(
        endpoint="databricks-meta-llama-3-3-70b-instruct",
        inputs={"messages": [{"role": "user", "content": prompt}]} )
    #Response Extraction: complex JSON data returned by the API to text answer.
    return response['choices'][0]['message']['content']

# Only run the test if the index successfully came online
if index.describe().get('status', {}).get('ready'):
    print(do_retrival_vectorindex_augument_llm_generate_nlg("what is the installment tenure in days?"))
