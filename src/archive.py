import os
import zipfile
import tarfile
import rarfile

class ArchiveExtractor:
    @staticmethod
    def extract(archive_path, extract_to):
        print(f"Extracting {archive_path}...")
        if not os.path.exists(extract_to):
            os.makedirs(extract_to)
        
        try:
            if zipfile.is_zipfile(archive_path):
                with zipfile.ZipFile(archive_path, 'r') as zf:
                    zf.extractall(extract_to)
            elif tarfile.is_tarfile(archive_path):
                with tarfile.open(archive_path, 'r') as tf:
                    tf.extractall(extract_to)
            elif rarfile.is_rarfile(archive_path):
                with rarfile.RarFile(archive_path, 'r') as rf:
                    rf.extractall(extract_to)
            else:
                print(f"Unsupported or invalid archive format: {archive_path}")
                return False
            return True
        except Exception as e:
            print(f"Failed to extract {archive_path}: {e}")
            return False

    @staticmethod
    def is_archive(filename):
        # Only support formats we can handle
        return filename.lower().endswith(('.zip', '.rar', '.tar', '.gz', '.tgz'))
