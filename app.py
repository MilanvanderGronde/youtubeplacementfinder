import streamlit as st
import pandas as pd
import re
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from datetime import datetime, timezone

# --- PAGE CONFIG ---
st.set_page_config(
    page_title="YouTube Placement Finder",
    page_icon="🎯",
    layout="wide"
)


# --- HELPER FUNCTIONS ---

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


@st.cache_data(show_spinner=False)
def get_category_map(_youtube_service):
    """Fetches category list. Cached so it only runs once."""
    category_map = {}
    try:
        request = _youtube_service.videoCategories().list(part="snippet", regionCode="US")
        response = request.execute()
        for item in response.get("items", []):
            category_map[item["id"]] = item["snippet"]["title"]
    except HttpError as e:
        st.error(f"Could not fetch categories: {e}")
    return category_map


def get_channel_stats(youtube, channel_ids):
    """Fetches statistics for a batch of channel IDs."""
    if not channel_ids: return {}
    channel_stats_map = {}
    unique_ids_list = list(channel_ids)

    progress_bar = st.progress(0, text="Fetching channel stats...")

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
        except HttpError:
            pass
        progress_bar.progress(min((i + 50) / len(unique_ids_list), 1.0))

    progress_bar.empty()
    return channel_stats_map


def search_videos(youtube, query, category_id, target_total, year, min_views):
    """Main search logic refactored to return a list of dicts."""
    all_videos = []
    next_page_token = None

    max_per_page = 50
    pages_to_fetch = (target_total + max_per_page - 1) // max_per_page

    try:
        for i in range(pages_to_fetch):
            search_params = {
                'part': 'snippet',
                'q': query,
                'type': 'video',
                'order': 'viewCount',
                'maxResults': max_per_page,
                'pageToken': next_page_token
            }

            if year:
                search_params['publishedAfter'] = f"{year}-01-01T00:00:00Z"
                search_params['publishedBefore'] = f"{int(year) + 1}-01-01T00:00:00Z"

            if category_id and category_id != "All":
                search_params['videoCategoryId'] = category_id

            search_response = youtube.search().list(**search_params).execute()
            video_ids = [item["id"]["videoId"] for item in search_response.get("items", [])]

            if not video_ids: break

            video_response = youtube.videos().list(
                part="snippet,statistics,contentDetails",
                id=",".join(video_ids)
            ).execute()

            fetched_videos = video_response.get("items", [])

            for video in fetched_videos:
                view_count = int(video.get("statistics", {}).get("viewCount", 0))
                if min_views is not None and view_count < min_views:
                    return all_videos
                all_videos.append(video)
                if len(all_videos) >= target_total:
                    return all_videos[:target_total]

            next_page_token = search_response.get("nextPageToken")
            if not next_page_token: break

    except HttpError as e:
        st.error(f"API Error: {e}")

    return all_videos


# --- MAIN UI ---
def main():
    st.title("🎯 YouTube Placement Finder")
    st.markdown("Generate targeted video lists for your marketing campaigns.")

    # 1. Sidebar: Configuration
    with st.sidebar:
        st.header("1. API Configuration")
        st.markdown("👉 [**How to get an API Key?**](https://developers.google.com/youtube/v3/getting-started) 🔑")

        default_key = ""
        if "YOUTUBE_API_KEY" in st.secrets:
            default_key = st.secrets["YOUTUBE_API_KEY"]

        api_key = st.text_input("Enter YouTube API Key", value=default_key, type="password")
        if default_key:
            st.caption("✅ Key loaded from local secrets")

        st.divider()
        st.header("2. Search Filters")
        query = st.text_input("Search Query", value="")

        year_input = st.text_input("Year (Leave blank for all time)", value="2025")
        year = int(year_input) if year_input.strip() and year_input.isdigit() else None
        min_views = st.number_input("Min Views Threshold", min_value=0, value=10)

        target_total = st.number_input("Total Videos to Fetch (for CSV)", min_value=1, max_value=500, value=10)

    # --- FLOATING BUTTON (STICKY RIGHT BOTTOM) ---
    buymeacoffee_url = "https://buymeacoffee.com/youtubeplacementfinder"
    st.markdown(
        f"""
        <style>
            .fixed-bottom-right {{
                position: fixed;
                bottom: 20px;
                right: 20px;
                z-index: 9999;
                background-color: white;
                text-align: right;
            }}
            .fixed-bottom-right img {{
                transition: transform 0.2s;
                border-radius: 8px;
                box-shadow: 0px 2px 10px rgba(0,0,0,0.2);
            }}
            .fixed-bottom-right img:hover {{
                transform: scale(1.05);
            }}
        </style>
        <div class="fixed-bottom-right">
            <a href="{buymeacoffee_url}" target="_blank">
                Support the developer, buy him a coffee </br>   
                <img src="https://cdn.buymeacoffee.com/buttons/v2/default-yellow.png" alt="Buy Me A Coffee" width="160">
            </a>
        </div>
        """,
        unsafe_allow_html=True
    )
    # ---------------------------------------------

    # 2. Main Area: Logic
    if not api_key:
        st.info("👈 Please enter your YouTube API Key in the sidebar to start.")
        return

    try:
        youtube = build("youtube", "v3", developerKey=api_key)

        cat_map = get_category_map(youtube)
        name_to_id = {v: k for k, v in cat_map.items()}
        name_to_id["All Categories"] = "All"
        cat_options = ["All Categories"] + sorted([k for k in name_to_id.keys() if k != "All Categories"])

        col1, col2 = st.columns([1, 1])
        with col1:
            selected_cat_name = st.selectbox("Select Category", options=cat_options, index=0)

        selected_cat_id = name_to_id[selected_cat_name]

        if st.button("🚀 Run Search", type="primary"):
            display_year = year if year else "All Time"
            with st.spinner(f"Searching for '{query}' ({display_year})..."):

                raw_videos = search_videos(youtube, query, selected_cat_id, target_total, year, min_views)

                if not raw_videos:
                    st.warning("No videos found matching criteria.")
                    return

                unique_channel_ids = {v['snippet']['channelId'] for v in raw_videos if 'snippet' in v}
                channel_stats = get_channel_stats(youtube, unique_channel_ids)

                processed_data = []
                today = datetime.now(timezone.utc)

                for video in raw_videos:
                    snippet = video["snippet"]
                    stats = video.get("statistics", {})
                    details = video.get("contentDetails", {})

                    views = int(stats.get("viewCount", 0))
                    likes = int(stats.get("likeCount", 0))
                    duration_sec = parse_duration(details.get("duration", "PT0S"))
                    published_at = snippet.get("publishedAt", "")

                    thumbnail_url = snippet.get("thumbnails", {}).get("high", {}).get("url", "")

                    avg_daily_views = 0
                    if published_at:
                        pub_dt = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
                        days_live = (today - pub_dt).days
                        avg_daily_views = views / days_live if days_live > 0 else views

                    cid = snippet.get("channelId")
                    c_stats = channel_stats.get(cid, {})

                    processed_data.append({
                        "Thumbnail": thumbnail_url,
                        "Title": snippet.get("title"),
                        "Views": views,
                        "Avg Daily Views": round(avg_daily_views, 2),
                        "Channel": snippet.get("channelTitle"),
                        "Subscribers": c_stats.get("subscriberCount", "N/A"),
                        "Published": published_at.split("T")[0],
                        "URL": f"https://www.youtube.com/watch?v={video['id']}",
                        "Likes": likes,
                        "Duration (s)": duration_sec
                    })

                df_full = pd.DataFrame(processed_data)
                df_display = df_full.head(10).copy()

                st.divider()
                st.success(f"✅ Search Complete! Fetched {len(df_full)} videos.")

                col_d1, col_d2 = st.columns([1, 4])
                with col_d1:
                    filename_year = year if year else "AllTime"
                    csv = df_full.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="📥 Download Full CSV",
                        data=csv,
                        file_name=f"youtube_{query.replace(' ', '_')}_{filename_year}.csv",
                        mime="text/csv"
                    )

                st.subheader(f"Top 10 Preview: {query}")
                st.dataframe(
                    df_display,
                    column_config={
                        "Thumbnail": st.column_config.ImageColumn("Preview", width="medium"),
                        "URL": st.column_config.LinkColumn("Video Link"),
                        "Views": st.column_config.NumberColumn(format="%d"),
                        "Avg Daily Views": st.column_config.NumberColumn(format="%.2f"),
                    },
                    use_container_width=True,
                    hide_index=True
                )

    except Exception as e:
        st.error(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
