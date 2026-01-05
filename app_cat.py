import streamlit as st
import pandas as pd
import re
import uuid
import io
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from datetime import datetime, timezone

# --- IMPORT MODULES ---
from constants import ALL_COUNTRY_CODES, LANGUAGE_CODES
from tracker import log_usage, get_logs, estimate_daily_usage

# --- PAGE CONFIG ---
st.set_page_config(
    page_title="ContextLab - Placement Finder",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- CSS LOADER ---
def load_css(file_name):
    with open(file_name) as f:
        st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

# --- HELPER FUNCTIONS ---
def parse_duration(duration_iso):
    if not duration_iso: return 0
    match = re.match(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?', duration_iso)
    if not match: return 0
    hours, minutes, seconds = match.groups()
    total_seconds = 0
    if hours: total_seconds += int(hours) * 3600
    if minutes: total_seconds += int(minutes) * 60
    if seconds: total_seconds += int(seconds)
    return total_seconds

def format_duration(seconds):
    if not seconds: return "0:00"
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    if h > 0: return f"{h}:{m:02d}:{s:02d}"
    return f"{m}:{s:02d}"

def format_time_ago(date_str):
    try:
        pub_date = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        diff = now - pub_date
        if diff.days == 0: return "Today"
        elif diff.days == 1: return "Yesterday"
        elif diff.days < 30: return f"{diff.days} days ago"
        elif diff.days < 365: months = diff.days // 30; return f"{months} mo ago"
        else: years = diff.days // 365; return f"{years} yr ago"
    except: return date_str

def format_big_number(num):
    if num >= 1_000_000: return f"{num / 1_000_000:.1f}M"
    if num >= 1_000: return f"{num / 1_000:.0f}k"
    return str(num)

def extract_video_id(url_or_id):
    """
    Extracts the 11-char Video ID from a YouTube URL or returns the ID if it's already clean.
    """
    if not isinstance(url_or_id, str):
        return None
    regex = r'(?:v=|\/|youtu\.be\/|embed\/)([0-9A-Za-z_-]{11})'
    match = re.search(regex, url_or_id)
    if match:
        return match.group(1)
    if re.match(r'^[0-9A-Za-z_-]{11}$', url_or_id):
        return url_or_id
    return None

# --- API FUNCTIONS ---

@st.cache_data(show_spinner=False)
def get_category_map(_youtube_service, region_code="US"):
    category_map = {}
    try:
        request = _youtube_service.videoCategories().list(part="snippet", regionCode=region_code)
        response = request.execute()
        for item in response.get("items", []):
            category_map[item["id"]] = item["snippet"]["title"]
    except HttpError: pass
    return category_map

@st.cache_data(show_spinner=False, ttl=3600)
def get_channel_stats(_youtube, channel_ids):
    """ 
    Returns (channel_data_map, quota_cost) 
    Fetches FULL available channel details including keywords, topics, and status.
    """
    if not channel_ids: return {}, 0
    channel_data_map = {}
    unique_ids_list = list(channel_ids)
    quota_cost = 0

    # Request parts for full details (auditDetails/contentOwnerDetails omitted as they require OAuth)
    parts = "snippet,statistics,contentDetails,topicDetails,status,brandingSettings"

    for i in range(0, len(unique_ids_list), 50):
        batch_ids = unique_ids_list[i:i + 50]
        try:
            request = _youtube.channels().list(part=parts, id=",".join(batch_ids))
            response = request.execute()
            quota_cost += 1 
            
            for item in response.get("items", []):
                cid = item["id"]
                snippet = item.get("snippet", {})
                stats = item.get("statistics", {})
                content = item.get("contentDetails", {})
                topics = item.get("topicDetails", {})
                status = item.get("status", {})
                branding = item.get("brandingSettings", {})
                
                # Parsing Branding / Keywords
                brand_channel = branding.get("channel", {})
                keywords = brand_channel.get("keywords", "")
                
                # Parsing Topics (cleaning up the Wikipedia URLs)
                topic_cats = topics.get("topicCategories", [])
                clean_topics = [t.split('/')[-1] for t in topic_cats] # Extract 'Music' from '.../wiki/Music'
                
                thumbs = snippet.get("thumbnails", {})
                thumb_url = thumbs.get("default", {}).get("url") or \
                            thumbs.get("medium", {}).get("url") or \
                            "https://cdn-icons-png.flaticon.com/512/847/847969.png"

                channel_data_map[cid] = {
                    # Snippet
                    "title": snippet.get("title"),
                    "description": snippet.get("description"),
                    "customUrl": snippet.get("customUrl", "N/A"),
                    "publishedAt": snippet.get("publishedAt", "").split("T")[0],
                    "country": snippet.get("country", "N/A"),
                    "defaultLanguage": snippet.get("defaultLanguage", "N/A"),
                    "thumbnail": thumb_url,
                    
                    # Statistics
                    "viewCount": int(stats.get("viewCount", 0)),
                    "subscriberCount": stats.get("subscriberCount", "0"),
                    "hiddenSubscriberCount": stats.get("hiddenSubscriberCount", False),
                    "videoCount": int(stats.get("videoCount", 0)),
                    
                    # Content Details (Uploads Playlist)
                    "uploadsPlaylist": content.get("relatedPlaylists", {}).get("uploads", ""),
                    
                    # Topic Details
                    "topicCategories": ", ".join(clean_topics),
                    
                    # Status
                    "privacyStatus": status.get("privacyStatus", "N/A"),
                    "isLinked": status.get("isLinked", False),
                    "madeForKids": status.get("madeForKids", False),
                    "selfDeclaredMadeForKids": status.get("selfDeclaredMadeForKids", False),
                    
                    # Branding
                    "keywords": keywords
                }
        except HttpError: pass
    return channel_data_map, quota_cost

@st.cache_data(show_spinner=False, ttl=3600)
def search_videos(_youtube, query, exclude_words_list, target_total, year, region_code, sort_order, relevance_language, video_duration, video_type):
    """ Search Mode Logic """
    all_videos = []
    next_page_token = None
    max_per_page = 50
    max_pages_to_fetch = (target_total // 5) + 5 
    quota_cost = 0

    try:
        for i in range(max_pages_to_fetch):
            search_params = {
                'part': 'snippet', 'q': query, 'type': 'video', 'order': sort_order,
                'maxResults': max_per_page, 'pageToken': next_page_token, 'regionCode': region_code
            }
            if relevance_language: search_params['relevanceLanguage'] = relevance_language
            if video_duration and video_duration != "any": search_params['videoDuration'] = video_duration
            if video_type and video_type != "any": search_params['videoType'] = video_type
            if year:
                search_params['publishedAfter'] = f"{year}-01-01T00:00:00Z"
                search_params['publishedBefore'] = f"{int(year) + 1}-01-01T00:00:00Z"

            search_response = _youtube.search().list(**search_params).execute()
            quota_cost += 100

            video_ids = []
            for item in search_response.get("items", []):
                if "id" in item and "videoId" in item["id"] and item["id"]["videoId"]:
                    video_ids.append(item["id"]["videoId"])

            if not video_ids: break

            video_response = _youtube.videos().list(part="snippet,statistics,contentDetails", id=",".join(video_ids)).execute()
            quota_cost += 1
            
            fetched_videos = video_response.get("items", [])
            for video in fetched_videos:
                title = video['snippet']['title'].lower()
                desc = video['snippet'].get('description', '').lower()
                if any(w.lower() in title or w.lower() in desc for w in exclude_words_list):
                    continue
                
                all_videos.append(video)
                if len(all_videos) >= target_total: return all_videos[:target_total], quota_cost
            
            next_page_token = search_response.get("nextPageToken")
            if not next_page_token: break
            
    except HttpError as e: st.error(f"API Error: {e}")
    return all_videos, quota_cost

def batch_analyze_videos(_youtube, video_ids, cat_map):
    """ Analyzer Mode Logic """
    analyzed_data = []
    quota_cost = 0
    unique_ids = list(set(video_ids))
    
    for i in range(0, len(unique_ids), 50):
        batch = unique_ids[i:i+50]
        try:
            response = _youtube.videos().list(
                part="snippet,statistics,status,contentDetails",
                id=",".join(batch)
            ).execute()
            quota_cost += 1
            
            for item in response.get("items", []):
                snippet = item.get("snippet", {})
                stats = item.get("statistics", {})
                status = item.get("status", {})
                
                cat_id = snippet.get("categoryId")
                cat_name = cat_map.get(cat_id, str(cat_id))
                
                # Auto-Fix: Lofi/Music as "Music"
                title_lower = snippet.get("title", "").lower()
                music_keywords = ["lofi", "music", "playlist", "track list", "tracklist"]
                if any(kw in title_lower for kw in music_keywords):
                    cat_name = "Music"
                
                view_count = int(stats.get("viewCount", 0))
                like_count = int(stats.get("likeCount", 0))
                comment_count = int(stats.get("commentCount", 0))
                
                vl_ratio = (like_count / view_count * 100) if view_count > 0 else 0
                
                analyzed_data.append({
                    "Video ID": item["id"],
                    "URL": f"https://www.youtube.com/watch?v={item['id']}",
                    "Title": snippet.get("title"),
                    "Channel": snippet.get("channelTitle"),
                    "Channel ID": snippet.get("channelId"),
                    "Category": cat_name,
                    "Views": view_count,
                    "Likes": like_count,
                    "Comments": comment_count,
                    "V/L Ratio (%)": round(vl_ratio, 2),
                    "Status": "Active" if status.get("uploadStatus") == "processed" else status.get("uploadStatus", "Unknown"),
                    "Privacy": status.get("privacyStatus", "Unknown"),
                    "Published": snippet.get("publishedAt", "").split("T")[0]
                })
                
        except HttpError as e:
            st.error(f"Batch Error: {e}")

    if not analyzed_data:
        return pd.DataFrame(columns=[
            "Video ID", "URL", "Title", "Channel", "Channel ID", "Category", 
            "Views", "Likes", "Comments", "V/L Ratio (%)", "Status", 
            "Privacy", "Published"
        ]), quota_cost

    df = pd.DataFrame(analyzed_data)
    return df, quota_cost

# --- MAIN UI ---
def main():
    try:
        load_css("style.css")
    except FileNotFoundError:
        st.error("style.css not found. Please create the file.")

    if 'visit_id' not in st.session_state:
        st.session_state.visit_id = str(uuid.uuid4())
        log_usage(st.session_state.visit_id, "New Visit", quota_units=0)

    st.title("🎯 ContextLab - Placement Finder")

    # --- API KEY SETUP ---
    with st.sidebar:
        st.header("Credentials")
        use_shared_key = st.checkbox("Use Shared Key (Free)", value=False)
        api_key = ""
        
        if use_shared_key:
            if "YOUTUBE_API_KEY" in st.secrets:
                api_key = st.secrets["YOUTUBE_API_KEY"]
                usage_pct, used_units = estimate_daily_usage()
                st.info(f"ℹ️ Shared Quota: {int(usage_pct * 100)}% Used ({format_big_number(used_units)} units)")
                st.progress(usage_pct)
                if usage_pct >= 1.0: st.error("⚠️ Shared quota exhausted.")
            else: st.error("🚨 Shared key not found.")
        else:
            url_key = st.query_params.get("api_key", "")
            user_input_key = st.text_input("Your YouTube API Key", value=url_key, type="password")
            if user_input_key:
                api_key = user_input_key
                st.query_params["api_key"] = user_input_key

        if not api_key:
            st.info("👈 Please enter credentials to start.")
            return

        st.divider()
        tool_mode = st.radio("Select Tool", ["🔎 Placement Finder", "📊 List Analyzer"], 
                             help="Search for new videos OR Analyze an existing list from Google Ads.")
        st.divider()

    try:
        youtube = build("youtube", "v3", developerKey=api_key)
        cat_map = get_category_map(youtube, region_code="US")
    except Exception as e:
        st.error(f"Failed to init API: {e}")
        return

    # ==========================================
    # TOOL 1: PLACEMENT FINDER (SEARCH)
    # ==========================================
    if tool_mode == "🔎 Placement Finder":
        with st.sidebar:
            st.header("Search Filters")
            search_topic = st.text_input("Search Query", placeholder="e.g. coffee reviews")
            exclude_words = st.text_input("Exclude Keywords", placeholder="e.g. dropshipping")
            exclude_list = [w.strip() for w in exclude_words.split() if w.strip()]
            
            final_query = f'"{search_topic}"' if search_topic else ""
            if exclude_words: final_query += " " + " ".join([f"-{w}" for w in exclude_list])

            sel_country = st.selectbox("Country", options=sorted(list(ALL_COUNTRY_CODES.keys())), index=sorted(list(ALL_COUNTRY_CODES.keys())).index("United States"))
            sel_lang = st.selectbox("Language", options=list(LANGUAGE_CODES.keys()))
            sel_duration = st.selectbox("Duration", options=["Any", "Short (<4m)", "Medium (4-20m)", "Long (>20m)"])
            duration_map = {"Any": "any", "Short (<4m)": "short", "Medium (4-20m)": "medium", "Long (>20m)": "long"}
            
            target_total = st.number_input("Max Results", min_value=1, value=20)
            year = st.text_input("Year", placeholder="2025")

        if st.button("🚀 Run Search", type="primary"):
            if not search_topic:
                st.warning("Please enter a Search Query.")
                return

            with st.spinner(f"Searching for '{final_query}'..."):
                videos, cost = search_videos(
                    youtube, final_query, exclude_list, target_total, 
                    int(year) if year.isdigit() else None, 
                    ALL_COUNTRY_CODES[sel_country], 'relevance', 
                    LANGUAGE_CODES[sel_lang], duration_map[sel_duration], 'any'
                )

                if not videos:
                    st.warning("No results found.")
                    return

                # Get Channel Stats
                c_ids = {v['snippet']['channelId'] for v in videos}
                c_stats, c_cost = get_channel_stats(youtube, c_ids)
                
                log_usage(st.session_state.visit_id, "Search Run", query=final_query, quota_units=cost+c_cost)

                data = []
                for v in videos:
                    snip = v['snippet']
                    stats = v['statistics']
                    cid = snip['channelId']
                    c_data = c_stats.get(cid, {})
                    
                    data.append({
                        "Title": snip['title'],
                        "Channel": snip['channelTitle'],
                        "Channel Subs": c_data.get('subscriberCount', "N/A"),
                        "Channel Country": c_data.get('country', "N/A"),
                        "Channel Keywords": c_data.get('keywords', ""),
                        "Views": int(stats.get('viewCount', 0)),
                        "Category": cat_map.get(snip.get('categoryId'), str(snip.get('categoryId'))),
                        "URL": f"https://www.youtube.com/watch?v={v['id']}"
                    })
                
                df = pd.DataFrame(data)
                st.success(f"Found {len(df)} videos. Quota Used: {cost+c_cost} units.")
                
                csv = df.to_csv(index=False).encode('utf-8')
                st.download_button("📥 Download CSV", csv, "results.csv", "text/csv", type="primary")
                st.dataframe(df, use_container_width=True)

    # ==========================================
    # TOOL 2: LIST ANALYZER (UPLOAD)
    # ==========================================
    elif tool_mode == "📊 List Analyzer":
        st.markdown("""
        ### 📊 Bulk Placement Analyzer
        Upload your Google Ads placement report (CSV). We will fetch detailed channel info.
        """)
        
        uploaded_file = st.file_uploader("Upload CSV File", type=["csv"])
        
        if uploaded_file:
            df_upload = pd.read_csv(uploaded_file)
            st.write(f"Preview of uploaded file ({len(df_upload)} rows):")
            st.dataframe(df_upload.head(3))
            
            cols = df_upload.columns.tolist()
            default_idx = 0
            for i, col in enumerate(cols):
                if "placement" in col.lower() or "url" in col.lower() or "video" in col.lower():
                    default_idx = i
                    break
            
            target_col = st.selectbox("Select the column containing Video URLs or IDs:", cols, index=default_idx)
            
            if st.button("⚡ Analyze Placements", type="primary"):
                video_ids = []
                for val in df_upload[target_col].dropna().astype(str):
                    vid_id = extract_video_id(val)
                    if vid_id:
                        video_ids.append(vid_id)
                
                unique_ids = list(set(video_ids))
                
                if not unique_ids:
                    st.warning(f"⚠️ No valid YouTube Video IDs found in column '{target_col}'. Please check your CSV.")
                    st.stop()
                
                st.info(f"Found {len(unique_ids)} unique video IDs. Analyzing now...")
                
                with st.spinner("Fetching data from YouTube API..."):
                    # 1. Fetch Video Data
                    result_df, v_cost = batch_analyze_videos(youtube, unique_ids, cat_map)
                    
                    if result_df.empty:
                        st.warning("No data retrieved.")
                        st.stop()

                    # 2. Fetch Channel Data (DEEP LOOKUP)
                    unique_channels = result_df['Channel ID'].dropna().unique().tolist()
                    channel_stats, c_cost = get_channel_stats(youtube, unique_channels)
                    
                    # 3. Merge Channel Data - MAPPING ALL NEW FIELDS
                    def get_c_val(cid, key, default=""):
                        return channel_stats.get(cid, {}).get(key, default)

                    result_df['Channel Subs'] = result_df['Channel ID'].map(lambda x: get_c_val(x, 'subscriberCount', '0'))
                    result_df['Channel Total Views'] = result_df['Channel ID'].map(lambda x: get_c_val(x, 'viewCount', 0))
                    result_df['Channel Country'] = result_df['Channel ID'].map(lambda x: get_c_val(x, 'country', 'N/A'))
                    result_df['Channel Keywords'] = result_df['Channel ID'].map(lambda x: get_c_val(x, 'keywords', ''))
                    result_df['Channel Topics'] = result_df['Channel ID'].map(lambda x: get_c_val(x, 'topicCategories', ''))
                    result_df['Made For Kids'] = result_df['Channel ID'].map(lambda x: get_c_val(x, 'madeForKids', False))
                    result_df['Channel Custom URL'] = result_df['Channel ID'].map(lambda x: get_c_val(x, 'customUrl', 'N/A'))
                    result_df['Channel Published'] = result_df['Channel ID'].map(lambda x: get_c_val(x, 'publishedAt', ''))

                    total_cost = v_cost + c_cost
                    log_usage(st.session_state.visit_id, "List Analysis", result_count=len(result_df), quota_units=total_cost)
                    
                    st.success(f"Analysis Complete! Processed {len(result_df)} videos. Quota Used: {total_cost} units.")
                    
                    # Metrics Summary
                    c1, c2, c3 = st.columns(3)
                    with c1: st.metric("Active Videos", len(result_df[result_df['Status']=='Active']))
                    with c2: st.metric("Avg Views", format_big_number(result_df['Views'].mean()))
                    with c3: st.metric("Avg Engagement", f"{result_df['V/L Ratio (%)'].mean():.2f}%")
                    
                    # Clean Column Ordering
                    ordered_cols = [
                        "Title", "Category", "Status", "Views", "V/L Ratio (%)",
                        "Channel", "Channel Subs", "Channel Country", "Channel Total Views", "Channel Keywords", "Channel Topics", "Made For Kids",
                        "Published", "URL"
                    ]
                    final_cols = [c for c in ordered_cols if c in result_df.columns]
                    remaining = [c for c in result_df.columns if c not in final_cols and c not in ['Video ID', 'Channel ID']]
                    
                    final_df = result_df[final_cols + remaining]
                    
                    csv_res = final_df.to_csv(index=False).encode('utf-8')
                    st.download_button("📥 Download Analyzed Data", csv_res, "analyzed_placements.csv", "text/csv", type="primary")
                    
                    st.dataframe(final_df, use_container_width=True)

    # Footer
    linkedin_url = "https://www.linkedin.com/in/milan-van-der-gronde-online-marketing-google-ads/"
    coffee_url = "https://buymeacoffee.com/youtubeplacementfinder"
    st.markdown(f"""
        <div class="creator-footer">
            <span>Created by <b>Milan van der Gronde</b></span> • 
            <span><a href="{linkedin_url}" target="_blank">Let's Connect on LinkedIn 🔗</a></span>
            <span style="margin-left: 15px; padding-left: 15px; border-left: 1px solid #dadce0;">
                <a href="{coffee_url}" target="_blank" style="text-decoration: none;">☕ Buy me a coffee</a>
            </span>
        </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()