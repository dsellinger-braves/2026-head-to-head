import os
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

url = os.environ.get("SUPABASE_URL") or os.environ.get("VITE_SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY") or os.environ.get("VITE_SUPABASE_ANON_KEY")
if not url or not key:
    raise ValueError("Missing SUPABASE_URL or SUPABASE_KEY in environment or .env")

supabase: Client = create_client(url, key)

try:
    res = supabase.table('historical_data').select('*').limit(1).execute()
    if res.data:
        for k, v in res.data[0].items():
            print(f"{k}: {type(v).__name__}")
except Exception as e:
    pass
