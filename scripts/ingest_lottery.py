import psycopg
import os
from dotenv import load_dotenv

load_dotenv()

def run_ingest():
    # Render Database URL from your environment variables
    db_url = os.getenv("DATABASE_URL")
    
    try:
        # Note: Using 'psycopg' (v3) syntax instead of psycopg2
        with psycopg.connect(db_url) as conn:
            with conn.cursor() as cur:
                print("Successfully connected to the Render database!")
                # Joe's ingestion logic will go here
                
    except Exception as e:
        print(f"Error connecting to database: {e}")

if __name__ == "__main__":
    run_ingest()
