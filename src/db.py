import psycopg2
import json
from .config import Config

class DatabaseManager:
    def __init__(self, db_url=Config.DATABASE_URL):
        self.db_url = db_url
        self.conn = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            try:
                self.conn.close()
            except:
                pass

    def connect(self):
        """Establish or re-establish database connection"""
        if self.conn and not self.conn.closed:
            return
        
        try:
            self.conn = psycopg2.connect(self.db_url)
            self.init_db()
        except psycopg2.Error as e:
            print(f"Failed to connect to database: {e}")
            raise

    def ensure_connection(self):
        """Check if connection is alive, reconnect if needed"""
        if self.conn is None or self.conn.closed:
            print("Database connection closed. Reconnecting...")
            self.connect()
            return

        # Optional: Active check
        try:
            with self.conn.cursor() as c:
                c.execute('SELECT 1')
        except psycopg2.Error:
            print("Database connection lost. Reconnecting...")
            self.connect()

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
            self._safe_rollback()
            print(f"Database initialization error: {e}")

    def is_file_processed(self, file_hash):
        return self.get_file_status(file_hash) == 'completed'

    def get_file_status(self, file_hash):
        try:
            self.ensure_connection()
            with self.conn.cursor() as c:
                c.execute("SELECT status FROM processed_files WHERE file_hash = %s", (file_hash,))
                row = c.fetchone()
                return row[0] if row else None
        except psycopg2.Error as e:
            self._safe_rollback()
            print(f"Error getting file status for {file_hash}: {e}")
            return None

    def update_file_status(self, file_hash, filename, status):
        try:
            self.ensure_connection()
            with self.conn.cursor() as c:
                c.execute("""
                    INSERT INTO processed_files (file_hash, filename, status, processed_at) 
                    VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (file_hash) 
                    DO UPDATE SET status = EXCLUDED.status, processed_at = EXCLUDED.processed_at
                """, (file_hash, filename, status))
                self.conn.commit()
        except psycopg2.Error as e:
            self._safe_rollback()
            print(f"Error updating status for {filename}: {e}")

    def save_finding(self, filename, json_line):
        if not json_line:
            return

        try:
            data = json.loads(json_line)
            detector_name = data.get('DetectorName', 'Unknown')
            raw_secret = data.get('Raw', data.get('rawSecret', ''))
            
            self.ensure_connection()
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
            self._safe_rollback()
            print(f"Database transaction error: {e}", flush=True)

    def _safe_rollback(self):
        """Attempt to rollback, ignoring connection errors"""
        if self.conn and not self.conn.closed:
            try:
                self.conn.rollback()
            except psycopg2.Error:
                pass
