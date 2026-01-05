import sys
import os
import csv
import re
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from datetime import datetime, timezone

# --- CONFIGURATION ---
API_KEY = os.environ.get("YOUTUBE_API_KEY")

# 1. DEFINE YOUR SEARCH
SEARCH_QUERY = "Colosseum tours"

# 2. DEFINE THE YEARS TO FETCH
YEARS_TO_FETCH = [2025]

# 3. DEFINE THE CATEGORY IDs TO FETCH
CATEGORY_IDS_TO_FETCH = ["19"]

# 4. SET THE MAX VIDEOS *PER YEAR/CATEGORY BATCH*
TARGET_TOTAL_VIDEOS = 10

# 5. SET MINIMUM VIEWS THRESHOLD (None to disable)
MIN_VIEWS_THRESHOLD = 10  # Only include videos with at least this many views

# 6. SET THE *SINGLE* FILENAME FOR THE COMBINED REPORT
query_tag = SEARCH_QUERY.replace(' ', '_')
CSV_FILENAME = f"Youtube_{query_tag}_combined_categories_report_2.csv"
# ---------------------

YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"


def parse_duration(duration_iso):
    """Converts YouTube's ISO 8601 duration format into seconds."""
    if not duration_iso: return 0
    match = re.match(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?', duration_iso)
    if not match: return 0
    hours, minutes, seconds = match.groups()
    total_seconds = 0
    if hours: total_seconds += int(hours) * 3600
    if minutes: total_seconds += int(minutes) * 60
    if seconds: total_seconds += int(seconds)
    return total_seconds


def get_category_map(youtube):
    """Fetches the video category list and returns an {id: name} map."""
    print("Fetching video category names (this happens once)...")
    category_map = {}
    try:
        request = youtube.videoCategories().list(part="snippet", regionCode="US")
        response = request.execute()
        for item in response.get("items", []):
            category_map[item["id"]] = item["snippet"]["title"]
        print("Category map created.\n")
        return category_map
    except HttpError as e:
        print(f"Warning: Could not fetch category map. Will use IDs. Error: {e.content}")
        return {}


def get_channel_stats(youtube, channel_ids):
    """Fetches statistics for a batch of channel IDs."""
    if not channel_ids: return {}
    print(f"Fetching stats for {len(channel_ids)} unique channels...")
    channel_stats_map = {}
    unique_ids_list = list(channel_ids)

    for i in range(0, len(unique_ids_list), 50):
        batch_ids = unique_ids_list[i:i + 50]
        try:
            request = youtube.channels().list(part="statistics", id=",".join(batch_ids))
            response = request.execute()
            for item in response.get("items", []):
                channel_id = item["id"]
                stats = item.get("statistics", {})
                sub_count = stats.get("subscriberCount", "N/A")
                if stats.get("hiddenSubscriberCount", False): sub_count = "Hidden"
                channel_stats_map[channel_id] = {
                    "subscriberCount": sub_count,
                    "videoCount": stats.get("videoCount", 0)
                }
        except HttpError as e:
            print(f"An HTTP error {e.resp.status} occurred while fetching channel stats: {e.content}")

    print(f"Finished fetching channel stats.")
    return channel_stats_map


def get_top_videos(youtube, query, category_id, total_results_target, after_date, before_date, min_views=None):
    """Paginates to get up to the target number of video objects for a specific category."""
    all_videos = []
    next_page_token = None
    max_per_page = 50
    pages_to_fetch = (total_results_target + max_per_page - 1) // max_per_page

    date_filter_text = f"published after {after_date} and before {before_date}"
    category_text = f"in category ID {category_id}"

    print(f"\nStarting search for '{query}' {category_text}, {date_filter_text}.")
    print(f"Will fetch up to {total_results_target} videos.")
    if min_views:
        print(f"Minimum views threshold: {min_views:,}\n")
    else:
        print()

    try:
        for i in range(pages_to_fetch):
            print(f"Fetching page {i + 1} for {after_date[:4]}, category {category_id}...")

            search_params = {
                'part': 'snippet', 'q': query, 'type': 'video', 'order': 'viewCount',
                'maxResults': max_per_page, 'pageToken': next_page_token,
                'publishedAfter': f"{after_date}T00:00:00Z",
                'publishedBefore': f"{before_date}T00:00:00Z",
                'videoCategoryId': category_id
            }

            search_response = youtube.search().list(**search_params).execute()

            video_ids = [item["id"]["videoId"] for item in search_response.get("items", [])]
            if not video_ids:
                print(f"No more search results found for {after_date[:4]}, category {category_id}.")
                break

            video_response = youtube.videos().list(
                part="snippet,statistics,contentDetails",
                id=",".join(video_ids)
            ).execute()

            fetched_videos = video_response.get("items", [])

            # NEW: Early stopping if min_views is specified
            if min_views is not None:
                videos_above_threshold = []
                for video in fetched_videos:
                    view_count = int(video.get("statistics", {}).get("viewCount", 0))
                    if view_count >= min_views:
                        videos_above_threshold.append(video)
                    else:
                        # Since results are sorted by viewCount, once we hit below threshold, stop
                        print(
                            f"Reached video with {view_count:,} views (below {min_views:,} threshold). Stopping pagination.")
                        all_videos.extend(videos_above_threshold)
                        return all_videos

                all_videos.extend(videos_above_threshold)
            else:
                all_videos.extend(fetched_videos)

            next_page_token = search_response.get("nextPageToken")
            if not next_page_token:
                print(f"End of search results for {after_date[:4]}, category {category_id}.")
                break
        return all_videos
    except HttpError as e:
        print(f"An HTTP error {e.resp.status} occurred: {e.content}")
        return all_videos if all_videos else None  # Return partial results on error if any


# --- MAIN FUNCTION ---
def main():
    if not API_KEY:
        print("ERROR: YOUTUBE_API_KEY environment variable not set.")
        sys.exit(1)
    if not YEARS_TO_FETCH:
        print("ERROR: The 'YEARS_TO_FETCH' list is empty.")
        sys.exit(1)
    if not CATEGORY_IDS_TO_FETCH:
        print("ERROR: The 'CATEGORY_IDS_TO_FETCH' list is empty.")
        sys.exit(1)

    try:
        youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=API_KEY)
        category_map = get_category_map(youtube)
        today = datetime.now(timezone.utc)
        total_videos_exported = 0

        print(f"\nOpening {CSV_FILENAME} to write all results...")

        with open(CSV_FILENAME, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)

            # Write the header ONCE
            writer.writerow([
                "Search Year", "Search Category Name",
                "Title", "Channel", "Channel Subscribers", "Channel Total Videos",
                "Views", "Likes", "Comments", "Avg Views per Day",
                "Published Date", "Like-to-View Ratio (%)", "Comment-to-View Ratio (%)",
                "Duration (Seconds)", "Spoken Language", "Text Language", "Video Category",
                "Tags", "URL"
            ])

            # --- Sequential Loops ---
            for year in YEARS_TO_FETCH:
                for category_id in CATEGORY_IDS_TO_FETCH:
                    category_name = category_map.get(category_id, f"ID:{category_id}")
                    print(f"\n=================================================")
                    print(f"--- STARTING BATCH FOR YEAR: {year}, CATEGORY: {category_name} ---")
                    print(f"=================================================")

                    published_after_date = f"{year}-01-01"
                    published_before_date = f"{year + 1}-01-01"

                    videos = get_top_videos(
                        youtube,
                        SEARCH_QUERY,
                        category_id,
                        TARGET_TOTAL_VIDEOS,
                        published_after_date,
                        published_before_date,
                        min_views=MIN_VIEWS_THRESHOLD
                    )

                    if not videos:
                        print(f"No results found for {year}, Category {category_name}. Skipping...")
                        continue

                    unique_channel_ids = {video['snippet']['channelId'] for video in videos if
                                          'snippet' in video and 'channelId' in video['snippet']}
                    channel_stats_map = get_channel_stats(youtube, unique_channel_ids) if unique_channel_ids else {}

                    print(
                        f"\nProcessing and writing {len(videos)} videos for {year}/{category_name} to {CSV_FILENAME}...")

                    for video in videos:
                        if 'snippet' not in video or 'id' not in video:
                            print(f"Skipping malformed video data: {video.get('id', 'Unknown ID')}")
                            continue

                        snippet = video["snippet"]
                        stats = video.get("statistics", {})
                        details = video.get("contentDetails", {})

                        title = snippet.get("title", "N/A")
                        channel_id = snippet.get("channelId", "N/A")
                        channel = snippet.get("channelTitle", "N/A")
                        video_id = video.get("id", "N/A")
                        url = f"https://www.youtube.com/watch?v={video_id}" if video_id != "N/A" else "N/A"
                        published_at_str = snippet.get("publishedAt")
                        published_date = published_at_str.split('T')[0] if published_at_str else "N/A"

                        spoken_lang = snippet.get("defaultAudioLanguage", "N/A")
                        text_lang = snippet.get("defaultLanguage", "N/A")
                        current_category_id = snippet.get("categoryId", "N/A")
                        current_category_name = category_map.get(current_category_id, "N/A")
                        tags = "|".join(snippet.get("tags", []))
                        duration_iso = details.get("duration", "PT0S")
                        duration_sec = parse_duration(duration_iso)

                        views_str = stats.get("viewCount", 0)
                        likes_str = stats.get("likeCount", 0)
                        comments_str = stats.get("commentCount", 0)

                        try:
                            views = int(views_str)
                        except (ValueError, TypeError):
                            views = 0
                        try:
                            likes = int(likes_str)
                        except (ValueError, TypeError):
                            likes = 0
                        try:
                            comments = int(comments_str)
                        except (ValueError, TypeError):
                            comments = 0

                        avg_views_per_day = 0.0
                        if published_at_str:
                            if published_at_str.endswith('Z'):
                                published_at_str = published_at_str[:-1] + '+00:00'
                            try:
                                publish_datetime = datetime.fromisoformat(published_at_str)
                                days_since_published = (today - publish_datetime).days
                                if days_since_published > 0:
                                    avg_views_per_day = views / days_since_published
                                elif days_since_published <= 0:
                                    avg_views_per_day = views
                            except ValueError:
                                avg_views_per_day = None  # Indicate error

                        like_ratio = (likes / views) * 100 if views > 0 else 0
                        comment_ratio = (comments / views) * 100 if views > 0 else 0

                        channel_data = channel_stats_map.get(channel_id, {})
                        sub_count = channel_data.get("subscriberCount", "N/A")
                        channel_video_count = channel_data.get("videoCount", "N/A")

                        writer.writerow([
                            year, category_name,
                            title, channel, sub_count, channel_video_count,
                            views, likes, comments,
                            f"{avg_views_per_day:.2f}" if avg_views_per_day is not None else "N/A",
                            published_date, f"{like_ratio:.2f}%", f"{comment_ratio:.2f}%",
                            duration_sec, spoken_lang, text_lang, current_category_name,
                            tags, url
                        ])

                    total_videos_exported += len(videos)
                    print(f"--- FINISHED BATCH FOR YEAR: {year}, CATEGORY: {category_name} ---")

        print("\n=================================================")
        print(f"--- All batches complete. Combined report saved! ---")
        print(f"Total videos exported: {total_videos_exported}")
        print(f"Filename: {CSV_FILENAME}")
        print("=================================================")

    except HttpError as e:
        print(f"\nAn HTTP error {e.resp.status} occurred: {e.content}")
    except ValueError as ve:
        print(f"\nAn error occurred processing a date: {ve}")
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")


if __name__ == "__main__":
    main()