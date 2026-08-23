# Databricks notebook source
#Decorator is python feature, used for applying some additional functionalities on top of an existing function if it requires the decoration(additional functionalities)
#from 3m import detailing
def polish_car(f):
    def wrapper():        
        f()
        print(f"I am going to polish your car")
        print(f"polished the car")
    return wrapper

@polish_car#decorator
def car():    
    print("plain car from showroom")

car()

# COMMAND ----------

#Decorator is python feature, used for applying some additional functionalities on top of an existing function if it requires the decoration(additional functionalities)
#from pyspark import pipelines as dp
#@dp.view(name="final_view")
def table(f):
    def wrapper():
        f()
        print("creating table and load your df data and we add lot of features like lineage, dq, governance etc.,")
    return wrapper

@table
def df_program():
    print("Plain DF imperative spark program (Extraction + Transformation)")
    return "Plain DF imperative spark program (Extraction + Transformation)"

df_program()
