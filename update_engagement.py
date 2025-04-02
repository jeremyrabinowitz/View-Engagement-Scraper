import os
import time
import requests
from urllib.parse import urlparse, parse_qs
from dotenv import load_dotenv

# 🔐 Load secrets from .env file (via Render Secret File)
load_dotenv()

AIRTABLE_API_KEY = os.environ['AIRTABLE_API_KEY']
AIRTABLE_BASE_ID = os.environ['AIRTABLE_BASE_ID']
AIRTABLE_TABLE_NAME = os.environ['AIRTABLE_TABLE_NAME']
YOUTUBE_API_KEY = os.environ['YOUTUBE_API_KEY']

# You can also move the view name to a secret if you'd like
AIRTABLE_VIEW_NAME = "View/Engagement Tracker - Recent"

# 🎯 Extract YouTube video ID from URL
def extract_video_id(url):
    if not url:
        return None
    parsed_url = urlparse(url)
    if 'youtu.be' in parsed_url.netloc:
        return parsed_url.path.strip("/")
    if 'youtube.com' in parsed_url.netloc and parsed_url.path.startswith('/live/'):
        return parsed_url.path.split('/live/')[-1].split('/')[0]
    if 'youtube.com' in parsed_url.netloc:
        query = parse_qs(parsed_url.query)
        return query.get('v', [None])[0]
    return None

# 📊 Fetch engagement stats from YouTube
def get_youtube_stats(video_id):
    url = f"https://www.googleapis.com/youtube/v3/videos?part=statistics&id={video_id}&key={YOUTUBE_API_KEY}"
    response = requests.get(url)
    if response.status_code != 200:
        return None
    data = response.json()
    if "items" in data and data["items"]:
        stats = data["items"][0]["statistics"]
        return {
            "views": int(stats.get("viewCount", 0)),
            "likes": int(stats.get("likeCount", 0)),
            "comments": int(stats.get("commentCount", 0)),
        }
    return None

# 🔁 Fetch ALL Airtable records from your target view (with pagination)
def get_airtable_records():
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}"
    headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}"}
    all_records = []
    offset = None

    while True:
        params = {
            "pageSize": 100,
            # 🔍 Limit to just the specified view
            "view": AIRTABLE_VIEW_NAME
        }
        if offset:
            params["offset"] = offset

        response = requests.get(url, headers=headers, params=params)
        data = response.json()
        all_records.extend(data.get("records", []))
        offset = data.get("offset")
        if not offset:
            break

    return all_records

# 🚀 Update Airtable in batches of 10 (with throttle)
def batch_update_airtable(records_to_update):
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}"
    headers = {
        "Authorization": f"Bearer {AIRTABLE_API_KEY}",
        "Content-Type": "application/json"
    }

    for i in range(0, len(records_to_update), 10):
        batch = {"records": records_to_update[i:i+10]}
        response = requests.patch(url, headers=headers, json=batch)
        if response.status_code != 200:
            print(f"Error updating records: {response.status_code}")
        time.sleep(0.25)  # throttle to stay under rate limit

# 🔁 Main script loop
def main():
    records = get_airtable_records()
    updates = []

    for record in records:
        fields = record.get("fields", {})
        url = fields.get("Asset Link")
        video_id = extract_video_id(url)

        if not video_id:
            print(f"Skipping invalid video URL: {url}")
            continue

        stats = get_youtube_stats(video_id)
        if not stats:
            print(f"Failed to fetch stats for video ID: {video_id}")
            continue

        updates.append({
            "id": record["id"],
            "fields": {
                "Views": stats["views"],
                "Likes": stats["likes"],
                "Comments": stats["comments"]
            }
        })

    if updates:
        batch_update_airtable(updates)
        print(f"✅ Updated {len(updates)} records.")
    else:
        print("No updates to send.")

if __name__ == "__main__":
    main()
