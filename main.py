import os
import sys
from src.config import Config
from src.db import DatabaseManager
from src.r2 import R2Client
from src.archive import ArchiveExtractor
from src.scanner import Scanner
from src.utils import cleanup, ensure_dir

def process_file(r2_client, db_manager, file_key, file_hash, file_size):
    local_file = os.path.join(Config.TEMP_DIR, file_key)
    extract_dir = os.path.join(Config.TEMP_DIR, f"{file_key}_extracted")

    try:
        # Check current status
        current_status = db_manager.get_file_status(file_hash)
        
        # 1. Download
        if current_status in ['downloaded', 'extracted', 'completed']:
            if os.path.exists(local_file) and os.path.getsize(local_file) == file_size:
                print(f"File {local_file} already exists and verified, skipping download.")
            else:
                # If status says downloaded but file is missing/invalid, re-download
                print(f"File missing or invalid despite '{current_status}' status. Re-downloading.")
                db_manager.update_file_status(file_hash, file_key, 'started')
                r2_client.download_file(file_key, local_file)
                db_manager.update_file_status(file_hash, file_key, 'downloaded')
        else:
            # Check if file exists locally anyway (from partial run)
            if os.path.exists(local_file) and os.path.getsize(local_file) == file_size:
                print(f"File {local_file} already exists and verified (found locally), skipping download.")
                db_manager.update_file_status(file_hash, file_key, 'downloaded')
            else:
                db_manager.update_file_status(file_hash, file_key, 'started')
                r2_client.download_file(file_key, local_file)
                db_manager.update_file_status(file_hash, file_key, 'downloaded')

        # 2. Extract
        if current_status in ['extracted', 'completed'] and os.path.exists(extract_dir):
            print(f"Directory {extract_dir} already exists, skipping extraction.")
        else:
            if ArchiveExtractor.extract(local_file, extract_dir):
                db_manager.update_file_status(file_hash, file_key, 'extracted')
            else:
                print(f"Extraction failed for {local_file}")
                return

        # 3. Scan & Store
        for line in Scanner.run_trufflehog(extract_dir):
            db_manager.save_finding(file_key, line)
        
        # Mark as completed
        db_manager.update_file_status(file_hash, file_key, 'completed')
            
    except Exception as e:
        print(f"Error processing {file_key}: {e}")
    finally:
        # 5. Cleanup
        cleanup(local_file)
        cleanup(extract_dir)
        print(f"Cleaned up {file_key}")

def main():
    try:
        Config.validate()
    except ValueError as e:
        print(str(e))
        return

    ensure_dir(Config.TEMP_DIR)

    try:
        r2_client = R2Client()
        
        with DatabaseManager() as db_manager:
            files = r2_client.list_files()
            if not files:
                print("No files found in bucket.")
                return

            # Sort files to prioritize locally existing ones
            files.sort(key=lambda obj: os.path.exists(os.path.join(Config.TEMP_DIR, obj['Key'])), reverse=True)

            for obj in files:
                key = obj['Key']
                if not ArchiveExtractor.is_archive(key):
                    print(f"Skipping non-archive {key}")
                    continue

                # Get file hash (ETag) from R2 object
                file_hash = obj.get('ETag', '').strip('"')
                file_size = obj.get('Size', 0)
                
                if db_manager.is_file_processed(file_hash):
                    print(f"Skipping already processed file: {key} (hash: {file_hash})")
                    continue

                process_file(r2_client, db_manager, key, file_hash, file_size)

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        cleanup(Config.TEMP_DIR)

if __name__ == "__main__":
    main()
