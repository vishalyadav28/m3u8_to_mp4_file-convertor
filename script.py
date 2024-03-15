import os
import csv
import subprocess
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import re

def best_m3u8_with_quality(file_quality, base_url):
    m3u8_url = base_url + '/' + file_quality[max(file_quality.keys())]
    return m3u8_url

def file_operation(output_path, r):
    with open(output_path, 'wb') as f:
        total_size = int(r.headers.get('content-length', 0))
        with tqdm(total=total_size, unit='B', unit_scale=True, unit_divisor=1024) as pbar:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
                    pbar.update(len(chunk))
    return True

def download_m3u8(url, output_path):
    r = requests.get(url, stream=True)
    return file_operation(output_path, r)

def get_video_files(content):
    file_names = [content[i] for i in range(2, len(content), 2)]
    return file_names

def select_highest_bandwidth_variant(master_playlist_content):
    variants = {}
    all_files_name = get_video_files(master_playlist_content.split('\n'))
    count = 0
    for line in master_playlist_content.split('\n'):
        if line.startswith('#EXT-X-STREAM-INF'):
            match_bandwidth = re.search(r'BANDWIDTH=(\d+)', line)
            if match_bandwidth:
                bandwidth = int(match_bandwidth.group(1))
                file_name = all_files_name[count]
                count += 1
                variants[bandwidth] = file_name

    max_key = max(variants.keys())
    result = {max_key: variants[max_key]}
    return result

def convert_m3u8_to_mp4(input_path, output_path):
    subprocess.run(['ffmpeg', '-i', input_path, '-c', 'copy', output_path])

def is_valid_url(url):
    # Regular expression pattern for URL validation
    pattern = re.compile(r'^https?://(?:www\.)?[\w-]+(?:\.[\w-]+)+[\w.,@?^=%&:/~+#-]*$')
    return bool(pattern.match(url))

def process_url(row):
    url = row['real_live_url']

    print(row['title'])



    title = row['title'].replace(' ', '_')  # Replace white spaces with underscores
    filename = f"{row['id']}__{title}.mp4"
    output_path = os.path.join('video_files', filename)
    master_playlist_path = f"temp_{filename}.m3u8"
    base_url = url.rsplit('/', 1)[0]

    
    if not is_valid_url(url):
        return None, f"Invalid URL: {url}"
    
    try:
      download_m3u8(url, master_playlist_path)
      highest_bandwidth_variant = select_highest_bandwidth_variant(open(master_playlist_path).read())
      if highest_bandwidth_variant:
          m3u8_file_url = best_m3u8_with_quality(highest_bandwidth_variant, base_url)
          convert_m3u8_to_mp4(m3u8_file_url, output_path)
      os.remove(master_playlist_path)
      return filename, None
    except Exception as e:
        return None, f"Error processing URL {url}: {str(e)}"


def main():
    if not os.path.exists('video_files'):
        os.makedirs('video_files')

    input_csv_path = 'xyz.csv'
    output_success_path = 'downloaded_files.txt'
    output_skipped_path = 'skipped_files.txt'

    with open(input_csv_path, 'r') as csvfile, \
        open(output_success_path, 'w') as success_file, \
        open(output_skipped_path, 'w') as skipped_file:
        
        fieldnames = ['field1', 'field2', 'field3']
        reader = csv.DictReader(csvfile, fieldnames=fieldnames)
        next(reader)  # Skip header
        urls = list(reader)
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(process_url, row) for row in urls]
            for future in as_completed(futures):
                downloaded_filename, error_message = future.result()
                if downloaded_filename:
                    success_file.write(downloaded_filename + "success" + '\n')
                elif error_message:
                    skipped_file.write(error_message + '\n')

if __name__ == "__main__":
    main()
