import time
import os
import logging

def cleanup_old_zips(path, days=30):
    now = time.time()
    cutoff = now - (days * 86400)

    for root, dirs, files in os.walk(path):
        for file in files:
            if file.endswith('.zip'):
                file_path = os.path.join(root, file)
                if os.path.getmtime(file_path) < cutoff:
                    os.remove(file_path)
                    logging.info(f'Deleted old ZIP: {file_path}')
