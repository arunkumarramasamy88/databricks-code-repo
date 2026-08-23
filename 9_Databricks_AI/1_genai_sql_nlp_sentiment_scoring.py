# Databricks notebook source
# MAGIC %md
# MAGIC ![](automated ticket pipeline.jpg)

# COMMAND ----------

# MAGIC %md
# MAGIC ####1. Automated Support Triage & Ticket Routing
# MAGIC Customer service teams are often overwhelmed with a massive backlog of tickets. Instead of having humans read every ticket to decide who handles it, your pipeline does it instantly.

# COMMAND ----------

# MAGIC %md
# MAGIC ####NLP Techniques (Syntax & Structure)
# MAGIC - Tokenization: Splits text into individual words and punctuation.
# MAGIC - POS Tagging: Assigns grammatical labels (nouns, verbs, adjectives).
# MAGIC - Vector Embeddings: Converts text meaning into mathematical numbers.
# MAGIC
# MAGIC ####NLU Techniques (Meaning & Context)
# MAGIC - Named Entity Recognition (NER): Extracts specific real-world subjects (products, locations).
# MAGIC - Sentiment Analysis: Identifies emotional tone (Positive, Negative, Neutral).
# MAGIC - Intent Detection: Determines the user's specific goal (returns, complaints).

# COMMAND ----------

# MAGIC %md
# MAGIC ####How to create Databricks Token
# MAGIC - In your Databricks workspace, click your username in the top bar and select Settings.
# MAGIC - Click Developer.
# MAGIC - Next to Access tokens, click Manage.
# MAGIC - Click Generate new token.

# COMMAND ----------

#databricks secrets create-scope izgenaiscope
#databricks secrets put-secret izgenaiscope databricks_token
DATABRICKS_TOKEN = dbutils.secrets.get(scope="izgenaiscope", key="databricks_token")

# COMMAND ----------

# MAGIC %md
# MAGIC 1. We are getting data into the Bronze layer of our Medallion Arch.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS lakehousecat;
# MAGIC CREATE SCHEMA IF NOT EXISTS lakehousecat.default;
# MAGIC drop table if exists lakehousecat.default.customer_reviews;
# MAGIC CREATE TABLE IF NOT EXISTS lakehousecat.default.customer_reviews (
# MAGIC   review_id STRING,
# MAGIC   city string,
# MAGIC   review_text STRING);
# MAGIC INSERT INTO lakehousecat.default.customer_reviews VALUES
# MAGIC ("REV-001",'NYC', "The shoe are beautiful, but they ripped after two days of wearing them! I want my money back."),
# MAGIC ("REV-002",'NY', "The Laptop Shipping took 3 weeks. Absolutely unacceptable."),
# MAGIC ("REV-003",'CA', "Perfect fit! Will definitely be buying from you guys again."),
# MAGIC ("REV-004",'CAL', "I received the wrong color. I ordered black but got blue. How do I fix this?");
# MAGIC
# MAGIC INSERT INTO lakehousecat.default.customer_reviews VALUES
# MAGIC ("REV-005",'NYC', "Do you know how much mark my son is going to get in this exam?");
# MAGIC
# MAGIC INSERT INTO lakehousecat.default.customer_reviews VALUES
# MAGIC ("REV-006",'NYC', "phone purchased a week ago for \$500 in NewYork");

# COMMAND ----------

df_reviews = spark.read.table("lakehousecat.default.customer_reviews")
display(df_reviews)

# COMMAND ----------

# MAGIC %md
# MAGIC ####NLP & NLU using Text Generation Model

# COMMAND ----------

# MAGIC %md
# MAGIC here ai_query is a function.. which takes model name and prompt as i/p and return the output in a string? Is it a spark function or databricks func?

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW lakehousecat.default.advanced_text_analysis AS
# MAGIC SELECT 
# MAGIC   review_id,
# MAGIC   review_text,
# MAGIC
# MAGIC   -- NLP CONCEPTS: Syntax, Structure, & Mechanics
# MAGIC --Prompt engineering techniques we used here - Major 2 types of prompts - System & User Prompt (Role Prompt, Instruction Prompt, Output Prompt.)
# MAGIC   -- 1. Tokenization: Breaking text into individual pieces
# MAGIC   ai_query(
# MAGIC     'databricks-meta-llama-3-1-8b-instruct',
# MAGIC     CONCAT('You are a tokenizer. Break the following text down into individual words and punctuation marks. Return ONLY a comma-separated list of tokens: ', review_text)
# MAGIC   ) AS nlp_tokens,
# MAGIC
# MAGIC   -- 2. POS (Part-of-Speech) Tagging: Identifying grammatical labels
# MAGIC   ai_query(
# MAGIC     'databricks-meta-llama-3-1-8b-instruct',
# MAGIC     CONCAT('You are a POS tagger. Identify the part of speech for the words in this text. Return ONLY a comma-separated list in "word(TAG)" format (e.g., fast(ADJECTIVE), run(VERB)): ', review_text)
# MAGIC   ) AS nlp_pos_tags,
# MAGIC
# MAGIC   -- 3. Vector Embeddings: Converting tex into mathematical (number) representations
# MAGIC   -- Note: Text generation models (like Llama) don't create embeddings. 
# MAGIC   ai_query(
# MAGIC     'databricks-bge-large-en', 
# MAGIC     review_text) AS nlp_vector_embedded_values,
# MAGIC
# MAGIC --The below rephrased words is not the part of NLP operation, just to show GenAI generates rather than use the existing code.
# MAGIC   ai_query(
# MAGIC     'databricks-meta-llama-3-1-8b-instruct',
# MAGIC     CONCAT('You are a rephraser. From the given text, can you rephrase the entire text in 2 other ways. Return rephrased sentence in double quotes terminated by comma: ', review_text)
# MAGIC   ) AS example_of_GenAI_rephrased_words,
# MAGIC
# MAGIC   -- NLU CONCEPTS: Meaning, Intent, Sentiment & Context
# MAGIC   --NLU 2 major roles - 1. Intent Identification , 2. Entity Recognition.
# MAGIC
# MAGIC   -- 4. NER (Named Entity Recognition): Extracting specific real-world subjects
# MAGIC   ai_query(
# MAGIC     'databricks-meta-llama-3-1-8b-instruct',
# MAGIC     CONCAT('You are an NER tool., Extract Named Entities such as Person, Organization, Duration, Time, Money, Location, or Product. Return ONLY a comma-separated list in "Entity (Type)" format. If none exist, return "None": ', review_text)
# MAGIC   ) AS nlu_ner_entities,
# MAGIC
# MAGIC   -- 5. Semantic Matching (Similarity Scoring) : Checking for a specific underlying goal (This example is to show the similarity scoring used behind)
# MAGIC   --by converting intent & given prompt into vectors
# MAGIC   --product is not useful,money back [1.2,4.1]= refund [1.2,4]
# MAGIC   ai_query(
# MAGIC     'databricks-meta-llama-3-3-70b-instruct',
# MAGIC     CONCAT('Does this review express an intent to return the product, ask for a refund, or complain about shipping duration? Return ONLY the word YES or NO: ', review_text)
# MAGIC   ) AS nlu_return_semantic_matching,
# MAGIC
# MAGIC   -- 6. Intent Classification/Generation: Identifying the broad purpose of the user
# MAGIC   ai_query(
# MAGIC     'databricks-meta-llama-3-1-8b-instruct',
# MAGIC     CONCAT('Identify the primary intent of this customer review. Choose ONLY ONE from the following categories: "Information", "Product Inquiry", "Complaint", "Praise", "Feature Request", "Customer Support", or "Other". Return ONLY the category name: ', review_text)
# MAGIC   ) AS nlu_primary_8b_intent,
# MAGIC
# MAGIC   -- 6. Intent Classification/Generation: Identifying the broad purpose of the user
# MAGIC   ai_query(
# MAGIC     'databricks-meta-llama-3-3-70b-instruct',
# MAGIC     CONCAT('Identify the primary intent of this customer review. Choose ONLY ONE from the following categories: "Information", "Product Inquiry", "Complaint", "Praise", "Feature Request", "Customer Support", or "Other". Return ONLY the category name: ', review_text)
# MAGIC   ) AS nlu_primary_70b_intent,
# MAGIC   
# MAGIC   -- 7. Intent Classification/Generation: Identifying the broad purpose of the user
# MAGIC   ai_query(
# MAGIC     'databricks-meta-llama-3-3-70b-instruct',
# MAGIC     CONCAT('Identify the primary intent & sentiment of this customer review. Choose ONLY ONE from the following department we have to route this intented and sentiment to customer request such as: "Logistics Department", "Support Department","Quality Control Department", "Escalation" or "Other". Return ONLY the category name: ', review_text)
# MAGIC   ) AS nlu_primary_70b_intent_routing,
# MAGIC
# MAGIC   -- 7. Sentiment Analysis: Understanding emotional tone
# MAGIC   ai_query(
# MAGIC     'databricks-meta-llama-3-1-8b-instruct',
# MAGIC     CONCAT('You are a sentiment analyzer. Classify the emotional tone of this review, dont hallucinate. Return sentiment score in a range of -1 to 1: ', review_text)
# MAGIC   ) AS sentiment_scoring
# MAGIC
# MAGIC FROM lakehousecat.default.customer_reviews;
# MAGIC
# MAGIC SELECT * FROM lakehousecat.default.advanced_text_analysis;

# COMMAND ----------

# MAGIC %sql
# MAGIC select uniqueid,ai_query(
# MAGIC     'databricks-meta-llama-3-1-8b-instruct',concat('you are a doctor, you have to suggest a treatment for the following condition',condition)) as treatment
# MAGIC      from catalog1_we47.default.drug_info

# COMMAND ----------

# MAGIC %md
# MAGIC ![](automated mail generator.jpg)

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. Automated Customer Review Response Generator (E-Commerce)
# MAGIC Instead of just classifying the sentiments and intents, We can use LLM (NLG) to actually write the email response to the customer.
# MAGIC
# MAGIC The Pipeline Concept: Read the negative review from a Delta table, pass it to the LLM with strict instructions, and output a drafted email into a new column.
# MAGIC
# MAGIC LLM & GenAI: LLM is a Large Lang Model using Meta Llama LLM, we are going to generate the mail output using NLG (GenAI+LLM) based on the input review text.
# MAGIC
# MAGIC NLG (Natural Language Generation): The model synthesizes a polite, grammatically perfect, and empathetic email (as per our prompt)
# MAGIC
# MAGIC Grounding: Pass the company's official return policy into the system prompt. The AI is grounded in this specific document so it doesn't make up rules.
# MAGIC
# MAGIC Guardrailing: Add a strict rule to the prompt: "Never promise a refund or discount. Only offer to connect them to the support team."
# MAGIC
# MAGIC Hallucination (Anti) Mitigation: Set temperature=0.1, so the AI doesn't invent fake product features or invent a fake customer service rep name.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lakehousecat.default.ecommerce_raw_reviews (
# MAGIC   review_id STRING,
# MAGIC   customer_review STRING,
# MAGIC   custname string
# MAGIC );
# MAGIC
# MAGIC INSERT INTO lakehousecat.default.ecommerce_raw_reviews (review_id, customer_review,custname)
# MAGIC VALUES 
# MAGIC   ('REV-001', 'The shoes are beautiful, but they ripped after two days of wearing them! I want my money back.','Irfan'),
# MAGIC   ('REV-002', 'Shipping took 3 weeks. Absolutely unacceptable.','Vaanmathy'),
# MAGIC   ('REV-003', 'Perfect fit! Will definitely be buying from you guys again.','Sarangabani'),
# MAGIC   ('REV-004', 'I received the wrong color. I ordered black but got blue. How do I fix this?','Vasu');
# MAGIC
# MAGIC SELECT * FROM lakehousecat.default.ecommerce_raw_reviews;

# COMMAND ----------

# MAGIC %sql
# MAGIC --What we learn here - Prompt Engineering, Context Engineering, Grounding, Guardrailing, Anti Hallucination, Human In Loop, Model Parameters, NLG
# MAGIC SELECT 
# MAGIC     review_id,
# MAGIC     customer_review,
# MAGIC     custname,
# MAGIC     ai_query(
# MAGIC         -- 1. The Model Endpoint
# MAGIC         'databricks-meta-llama-3-3-70b-instruct',
# MAGIC         
# MAGIC         -- 2. The Contextual Prompt (System prompt/context (Grounding/Guardrailing/Anti Hallucination) + The Data Column (User Prompt))
# MAGIC         CONCAT(
# MAGIC             'You are an empathetic, professional customer support Engineer for an E-Commerce company. ',
# MAGIC             'Read the customer review and write a direct email response to them addressing the customer. ',
# MAGIC             'Produce the output with subject, salutation, body, closing message, and signature with - Thanks & Regards, Inceptez Technologies',
# MAGIC             
# MAGIC             'GROUNDING CONTEXT: ',
# MAGIC             '- We only accept returns within 30 days of purchase. ',
# MAGIC             '- We do NOT give cash refunds. We only offer store credit or exact item replacements. ',
# MAGIC             
# MAGIC             'GUARDRAILS: ',
# MAGIC             '1. Never promise a refund through electronic money transfer under any circumstances. ',
# MAGIC             '2. Never offer a discount code or coupon. ',
# MAGIC             '3. Keep the response under 4 sentences when you do NLG. ',
# MAGIC             
# MAGIC             'ANTI HALLUCINATION: ',
# MAGIC             'If the user asks for something not covered in the policies above, do not invent a solution. ',
# MAGIC             'Simply state: "I am escalating this to our senior support team who will contact you within 24 hours." ',
# MAGIC             
# MAGIC             'Customer Review: ', customer_review, custname
# MAGIC         ),
# MAGIC         
# MAGIC         -- 3. The Model Parameters (Ensuring robotic/strict adherence to guardrails)
# MAGIC         modelParameters => named_struct('temperature', 0, 'max_tokens', 100)
# MAGIC         
# MAGIC     ) AS ai_generated_email
# MAGIC
# MAGIC FROM lakehousecat.default.ecommerce_raw_reviews;
