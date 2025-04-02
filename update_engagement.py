
import requests
import time
import os
from urllib.parse import urlparse, parse_qs
from dotenv import load_dotenv
load_dotenv()
# --------------------
# SETTINGS (from environment)
# --------------------
AIRTABLE_API_KEY = os.environ['AIRTABLE_API_KEY']
AIRTABLE_BASE_ID = os.environ['AIRTABLE_BASE_ID']
AIRTABLE_TABLE_NAME = os.environ['AIRTABLE_TABLE_NAME']
YOUTUBE_API_KEY = os.environ['YOUTUBE_API_KEY']

# --------------------
# EXTRACT VIDEO ID
# --------------------
def extract_video_id(url):
    if not url:
        return None
    parsed_url = urlparse(url)

    # Handle youtu.be short links
    if 'youtu.be' in parsed_url.netloc:
        return parsed_url.path.strip("/")

    # Handle /live/ links
    if 'youtube.com' in parsed_url.netloc and parsed_url.path.startswith('/live/'):
        return parsed_url.path.split('/live/')[-1].split('/')[0]

    # Handle full youtube.com/watch links
    if 'youtube.com' in parsed_url.netloc:
        query = parse_qs(parsed_url.query)
        return query.get('v', [None])[0]

    return None

# --------------------
# GET ENGAGEMENT DATA
# --------------------
def get_youtube_stats(video_id):
    url = f"https://www.googleapis.com/youtube/v3/videos?part=statistics&id={video_id}&key={YOUTUBE_API_KEY}"
    response = requests.get(url)
    data = response.json()
    if "items" in data and data["items"]:
        stats = data["items"][0]["statistics"]
        return {
            "views": int(stats.get("viewCount", 0)),
            "likes": int(stats.get("likeCount", 0)),
            "comments": int(stats.get("commentCount", 0)),
        }
    return None

# --------------------
# GET AIRTABLE RECORDS (WITH PAGINATION)
# --------------------
def get_airtable_records():
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}"
    headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}"}
    all_records = []
    offset = None

    while True:
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset

        response = requests.get(url, headers=headers, params=params)
        data = response.json()
        all_records.extend(data.get("records", []))

        offset = data.get("offset")
        if not offset:
            break

    return all_records

# --------------------
# BATCH UPDATE AIRTABLE RECORDS
# --------------------
def batch_update_airtable(records_to_update):
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}"
    headers = {
        "Authorization": f"Bearer {AIRTABLE_API_KEY}",
        "Content-Type": "application/json"
    }

    for i in range(0, len(records_to_update), 10):  # Airtable allows 10 updates per batch
        batch = records_to_update[i:i+10]
        data = {"records": batch}
        response = requests.patch(url, headers=headers, json=data)
        if response.status_code != 200:
            print(f"Batch update failed: {response.status_code}, {response.text}")
        time.sleep(0.25)  # Stay within rate limit

# --------------------
# MAIN LOGIC
# --------------------
def main():
    records = get_airtable_records()
    records_to_update = []

    for record in records:
        fields = record.get('fields', {})
        url = fields.get('Asset Link')
        video_id = extract_video_id(url)

        if not video_id:
            print(f"Skipping record (no video ID): {url}")
            continue

        stats = get_youtube_stats(video_id)
        if stats:
            print(f"Fetched stats for {url}: {stats}")
            records_to_update.append({
                "id": record['id'],
                "fields": {
                    "Views": stats["views"],
                    "Likes": stats["likes"],
                    "Comments": stats["comments"]
                }
            })
        else:
            print(f"Failed to fetch stats for: {video_id}")

    if records_to_update:
        batch_update_airtable(records_to_update)

if __name__ == "__main__":
    main()
