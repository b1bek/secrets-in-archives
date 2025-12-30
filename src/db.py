import psycopg2
import json
from .config import Config

class DatabaseManager:
    def __init__(self, db_url=Config.DATABASE_URL):
        self.db_url = db_url
        self.conn = None

    def __enter__(self):
        self.conn = psycopg2.connect(self.db_url)
        self.init_db()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.close()

    def init_db(self):
        try:
            with self.conn.cursor() as c:
                # Create scan_results table
                c.execute('''CREATE TABLE IF NOT EXISTS scan_results
                             (id SERIAL PRIMARY KEY,
                              detector_name TEXT,
                              raw_secret TEXT,
                              raw_json TEXT,
                              created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')

                # Create unique index on raw_secret for deduplication
                c.execute('''CREATE UNIQUE INDEX IF NOT EXISTS idx_scan_results_raw_secret ON scan_results (raw_secret)''')

                # Create processed_files table
                c.execute('''CREATE TABLE IF NOT EXISTS processed_files
                             (file_hash TEXT PRIMARY KEY,
                              filename TEXT,
                              status TEXT DEFAULT 'completed',
                              processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
                
                self.conn.commit()
        except psycopg2.Error as e:
            self.conn.rollback()
            print(f"Database initialization error: {e}")

    def is_file_processed(self, file_hash):
        return self.get_file_status(file_hash) == 'completed'

    def get_file_status(self, file_hash):
        try:
            with self.conn.cursor() as c:
                c.execute("SELECT status FROM processed_files WHERE file_hash = %s", (file_hash,))
                row = c.fetchone()
                return row[0] if row else None
        except psycopg2.Error as e:
            self.conn.rollback()
            print(f"Error getting file status for {file_hash}: {e}")
            return None

    def update_file_status(self, file_hash, filename, status):
        try:
            with self.conn.cursor() as c:
                c.execute("""
                    INSERT INTO processed_files (file_hash, filename, status, processed_at) 
                    VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (file_hash) 
                    DO UPDATE SET status = EXCLUDED.status, processed_at = EXCLUDED.processed_at
                """, (file_hash, filename, status))
                self.conn.commit()
        except psycopg2.Error as e:
            self.conn.rollback()
            print(f"Error updating status for {filename}: {e}")

    def save_finding(self, filename, json_line):
        if not json_line:
            return

        try:
            data = json.loads(json_line)
            detector_name = data.get('DetectorName', 'Unknown')
            raw_secret = data.get('Raw', data.get('rawSecret', ''))
            
            with self.conn.cursor() as c:
                c.execute("""INSERT INTO scan_results (detector_name, raw_secret, raw_json) 
                             VALUES (%s, %s, %s)
                             ON CONFLICT (raw_secret) DO NOTHING""",
                          (detector_name, raw_secret, json_line))
                self.conn.commit()
                print(f"Saved finding for {filename}", flush=True)
                
        except json.JSONDecodeError:
            print(f"Failed to decode JSON: {json_line[:50]}...", flush=True)
        except psycopg2.Error as e:
            self.conn.rollback()
            print(f"Database transaction error: {e}", flush=True)

    def save_results(self, filename, scan_output):
        count = 0
        
        # Split lines first
        lines = [line for line in scan_output.strip().split('\n') if line]
        values_to_insert = []

        for line in lines:
            try:
                data = json.loads(line)
                detector_name = data.get('DetectorName', 'Unknown')
                raw_secret = data.get('Raw', data.get('rawSecret', ''))
                
                values_to_insert.append((detector_name, raw_secret, line))
            except json.JSONDecodeError:
                continue
        
        if not values_to_insert:
            print(f"No valid findings to save for {filename}")
            return

        try:
            with self.conn.cursor() as c:
                # Use executemany for batch insertion - much faster!
                c.executemany("""INSERT INTO scan_results (detector_name, raw_secret, raw_json) 
                                 VALUES (%s, %s, %s)
                                 ON CONFLICT (raw_secret) DO NOTHING""",
                              values_to_insert)
                
                count = len(values_to_insert)
                self.conn.commit()
                print(f"Saved {count} new findings for {filename}")
                
        except psycopg2.Error as e:
            self.conn.rollback()
            print(f"Database transaction error: {e}")
