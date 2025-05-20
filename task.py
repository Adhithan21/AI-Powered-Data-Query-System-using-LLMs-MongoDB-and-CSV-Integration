import os
import pandas as pd
from pymongo import MongoClient
from langchain_ollama import OllamaLLM
from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain
from urllib.parse import quote_plus
import datetime

#  MongoDB Atlas connection
username = quote_plus("adhi")        
password = quote_plus("Adhithan")      

uri = f"mongodb+srv://{username}:{password}@adhi.hcwg3kf.mongodb.net/?retryWrites=true&w=majority&appName=Adhi"

try:
    client = MongoClient(uri)
    db = client["product_database"]
    collection = db["products"]
except Exception as e:
    print(f"❌ Failed to connect to MongoDB Atlas: {e}")
    exit()

# LLM using Ollama

llm = OllamaLLM(model="llama2")

# Function to load CSV into MongoDB

def load_csv_to_mongodb(csv_path):
    try:
        df = pd.read_csv(csv_path)
        df['LaunchDate'] = pd.to_datetime(df['LaunchDate'], format='%d-%m-%Y')
        df['Discount'] = df['Discount'].str.rstrip('%').astype('float') / 100.0
        collection.drop()  # clear old data
        collection.insert_many(df.to_dict('records'))
        print(f"✅ Loaded {len(df)} records into MongoDB.")
        return True
    except Exception as e:
        print(f"❌ Error loading CSV: {e}")
        return False

def generate_mongodb_query(user_input, columns):
    try:
        prompt_template = PromptTemplate(
            input_variables=["user_input", "columns"],
            template="""
You are a MongoDB query generator. Based on the user's input, generate a valid MongoDB query.
The available columns in the database are: {columns}

Rules:
1. Use double quotes for field names.
2. Use ISODate for dates.
3. Use $gt, $lt, $in for logic.
4. Output only the part inside db.collection.find(...)

User Input: {user_input}

MongoDB Query:
"""
        )
        chain = LLMChain(llm=llm, prompt=prompt_template)
        result = chain.run(user_input=user_input, columns=", ".join(columns)).strip()

        if result.startswith("db.collection.find("):
            start = result.find("{")
            end = result.rfind("}") + 1
            return result[start:end]
        elif result.startswith("{"):
            return result
        else:
            print("⚠️ Unexpected LLM output format.")
            return None
    except Exception as e:
        print(f"❌ Query generation error: {e}")
        return None

def execute_query(query_str):
    try:
        query_dict = eval(query_str)
        results = list(collection.find(query_dict))
        return results
    except Exception as e:
        print(f"❌ Error executing query: {e}")
        return []

def handle_results(results, action='save', filename=None):
    if not results:
        print("⚠️ No results found.")
        return
    df = pd.DataFrame(results)
    if '_id' in df.columns:
        df.drop('_id', axis=1, inplace=True)
    if action == 'display':
        print(df.to_string(index=False))
    elif action == 'save':
        if not filename:
            filename = f"result_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(filename, index=False)
        print(f"✅ Results saved to {filename}")
        return filename

def main():
    print("=== Automated Data Query and Retrieval System ===")

    csv_path = input("Enter path to CSV file (or press Enter to use default 'sample_data.csv'): ").strip()
    if not csv_path:
        csv_path = "sample_data.csv"

    if not os.path.exists(csv_path):
        print(f"❌ File not found: {csv_path}")
        return

    if load_csv_to_mongodb(csv_path):
        sample_doc = collection.find_one()
        if not sample_doc:
            print("❌ No data found in MongoDB.")
            return

        columns = [k for k in sample_doc.keys() if k != '_id']
        print("\nAvailable Columns:", ", ".join(columns))

        # Test cases
        test_cases = [
            "Find all products with a rating below 4.5 that have more than 200 reviews and are offered by the brand 'Nike' or 'Sony'",
            "Which products in the Electronics category have a rating of 4.5 or higher and are in stock?",
            "List products launched after January 1, 2022, in the Home & Kitchen or Sports categories with a discount of 10% or more, sorted by price in descending order"
        ]

        queries_generated = []

        for i, test_case in enumerate(test_cases, 1):
            print(f"\n🔍 Test Case {i}: {test_case}")
            query = generate_mongodb_query(test_case, columns)
            if query:
                print(f"📄 Query: {query}")
                queries_generated.append(f"Test Case {i}:\n{query}\n\n")
                results = execute_query(query)
                handle_results(results, 'save', f"test_case{i}.csv")

        with open("Queries_generated.txt", "w") as f:
            f.writelines(queries_generated)

        print("\n📁 All queries saved to Queries_generated.txt")

    print("\n✅ All Done!")

# Run the app
if __name__ == "__main__":
    main()
