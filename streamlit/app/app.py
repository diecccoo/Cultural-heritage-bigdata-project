import streamlit as st
import psycopg2
from qdrant_client import QdrantClient
import pandas as pd
from typing import List, Dict, Optional, Tuple
import logging
from qdrant_client.models import NamedVector
import random

from PIL import Image
import requests
from io import BytesIO

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Flag for session state to track connection errors
if 'db_conn_error_shown' not in st.session_state:
    st.session_state.db_conn_error_shown = False
if 'qdrant_conn_error_shown' not in st.session_state:
    st.session_state.qdrant_conn_error_shown = False

# Configure database
DB_CONFIG = {
    'host': 'postgres',
    'port': 5432,
    'database': 'heritage',
    'user': 'postgres',
    'password': 'postgres'
}

# Configure Qdrant
QDRANT_CONFIG = {
    'host': 'qdrant',
    'port': 6333
}

# Configure app
PAGE_SIZE = 20
MAX_RESULTS = 60
PLACEHOLDER_IMAGE = "https://via.placeholder.com/300?text=Non+trovata"

@st.cache_resource
def get_db_connection():
    """Create database connection PostgreSQL"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        st.session_state.db_conn_error_shown = False
        return conn
    except Exception as e:
        if not st.session_state.db_conn_error_shown:
            logger.error(f"Connection failed to database PostgreSQL: {str(e)}")
            st.error(f"Connection failed to database PostgreSQL: {str(e)}")
            st.session_state.db_conn_error_shown = True
        return None

@st.cache_resource
def get_qdrant_client():
    """Create client Qdrant"""
    try:
        client = QdrantClient(host=QDRANT_CONFIG['host'], port=QDRANT_CONFIG['port'])
        client.get_collections()
        logger.info("Connection to Qdrant successfully established.")
        st.session_state.qdrant_conn_error_shown = False
        return client
    except Exception as e:
        if not st.session_state.qdrant_conn_error_shown:
            logger.error(f"Connection failed to Qdrant: {str(e)}")
            st.error(f"Connection failed to  Qdrant: {str(e)}")
            st.session_state.qdrant_conn_error_shown = True
        return None

def get_image_url(image_url_array: List[str], is_shown_by_array: List[str]) -> str:
    """Gets the first valid image URL or placeholder"""
    if image_url_array and len(image_url_array) > 0 and image_url_array[0]:
        return image_url_array[0]
    elif is_shown_by_array and len(is_shown_by_array) > 0 and is_shown_by_array[0]:
        return is_shown_by_array[0]
    else:
        return PLACEHOLDER_IMAGE

@st.cache_data
def get_filter_options() -> Dict[str, List[str]]:
    """Load options for dropdown and multiselect filters"""
    conn = get_db_connection()
    if not conn:
        return {}
    
    try:
        cursor = conn.cursor()
        options = {}
        
        cursor.execute("SELECT DISTINCT creator FROM join_metadata_deduplicated WHERE creator IS NOT NULL ORDER BY creator")
        options['creators'] = [row[0] for row in cursor.fetchall()]
        
        cursor.execute("SELECT DISTINCT provider FROM join_metadata_deduplicated WHERE provider IS NOT NULL AND provider != '' ORDER BY provider")
        options['provider'] = [row[0] for row in cursor.fetchall()]
        
        cursor.execute("SELECT DISTINCT UNNEST(tags) FROM join_metadata_deduplicated WHERE tags IS NOT NULL AND tags != '{}' ORDER BY 1")
        options['tags'] = [row[0] for row in cursor.fetchall()]
        
        cursor.close()
        return options
        
    except Exception as e:
        st.error(f"Error in charging filters: {str(e)}")
        return {}

def search_guids(filters: Dict, page: int, page_size: int = PAGE_SIZE, seed: Optional[float] = None) -> Tuple[List[Dict], int]:
    """Object search with filters, random sorting and pagination."""
    conn = get_db_connection()
    if not conn:
        return [], 0

    try:
        cursor = conn.cursor()

        if seed is not None:
            cursor.execute("SELECT setseed(%s)", (seed,))

        where_clauses = ["image_url IS NOT NULL AND image_url[1] IS NOT NULL"]
        params = []

        if filters.get('creator'):
            where_clauses.append("creator = %s")
            params.append(filters['creator'])
        
        if filters.get('provider'):
            placeholders = ','.join(['%s'] * len(filters['provider']))
            where_clauses.append(f"provider IN ({placeholders})")
            params.extend(filters['provider'])
        
        if filters.get('tags'):
            where_clauses.append("tags && %s::TEXT[]")
            params.append(filters['tags'])
        where_string = " AND ".join(where_clauses)

        # Queryfor total count of distinct GUIDs
        count_query = f"SELECT COUNT(DISTINCT guid) FROM join_metadata_deduplicated WHERE {where_string}"
        cursor.execute(count_query, params)
        total_count = cursor.fetchone()[0]
        total_count = min(total_count, MAX_RESULTS) # Limit to MAX_RESULTS

        # Subquery to get the distinct elements, then random sorting and pagination
        subquery = f"SELECT DISTINCT ON (guid) * FROM join_metadata_deduplicated WHERE {where_string} ORDER BY guid, id"
        query_paginated = f"SELECT * FROM ({subquery}) AS distinct_items ORDER BY RANDOM() LIMIT %s OFFSET %s"
        
        final_params = params + [page_size, (page - 1) * page_size]
        cursor.execute(query_paginated, final_params)
        
        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]

        cursor.close()
        return results, total_count

    except Exception as e:
        logger.error(f"Error in search: {str(e)}")
        st.error(f"Error in search: {str(e)}")
        return [], 0

def get_guid_details(guid: str) -> Optional[Dict]:
    """obtains details of a single object"""
    conn = get_db_connection()
    if not conn:
        return None
    
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM join_metadata_deduplicated WHERE guid = %s", (guid,))
        
        columns = [desc[0] for desc in cursor.description]
        row = cursor.fetchone()
        
        if row:
            result = dict(zip(columns, row))
            cursor.close()
            return result
        else:
            cursor.close()
            return None
            
    except Exception as e:
        st.error(f"Error in retrieving details: {str(e)}")
        return None

def get_all_annotations_for_guid(guid: str) -> List[Dict]:
    """Gets all annotations (rows) for a given guid."""
    conn = get_db_connection()
    if not conn:
        return []

    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM join_metadata_deduplicated WHERE guid = %s ORDER BY timestamp DESC", (guid,))

        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]

        cursor.close()
        return results

    except Exception as e:
        st.error(f"Error in retrieving all annotations for the object: {str(e)}")
        return []

def get_recommendations(guid: str) -> List[Dict]:
    """Gets similar recommendations from Qdrant"""
    client = get_qdrant_client()
    if not client:
        logger.warning("Qdrant client unavailable, unable to get recommendations.")
        return []
    
    try:
        logger.info(f"Searching vector for guid: {guid} in Qdrant.")
        search_result = client.scroll(
            collection_name="heritage_embeddings",
            scroll_filter={"must": [{"key": "guid", "match": {"value": guid}}]},
            limit=1,
            with_vectors=True
        )
        
        if not search_result[0]:
            logger.warning(f"No vectors found in Qdrant per guid: {guid}")
            return []
        
        current_named_vectors = search_result[0][0].vector
        query_vector_data = current_named_vectors.get("combined")
        
        if query_vector_data is None:
            logger.error("Vector ‘combined’ not found for current object.")
            return []

        logger.info(f"Vector found for guid: {guid}. Beginning search for similar items.")
        
        similar_results = client.search(
            collection_name="heritage_embeddings",
            query_vector=NamedVector(name="combined", vector=query_vector_data),
            limit=11,
            append_payload=True
        )
        
        similar_ids = [res.payload["guid"] for res in similar_results if res.payload and "guid" in res.payload and res.payload["guid"] != guid]
        similar_ids = similar_ids[:10]
        
        logger.info(f"Found {len(similar_ids)} similar ID objects : {similar_ids}")

        if similar_ids:
            conn = get_db_connection()
            if conn:
                cursor = conn.cursor()
                placeholders = ','.join(['%s'] * len(similar_ids))
                query = f"SELECT DISTINCT ON (guid) * FROM join_metadata_deduplicated WHERE guid IN ({placeholders}) ORDER BY guid, id"
                cursor.execute(query, similar_ids)
                
                columns = [desc[0] for desc in cursor.description]
                results = [dict(zip(columns, row)) for row in cursor.fetchall()]
                cursor.close()
                logger.info(f"Retrieved {len(results)} metadata for similar objects from PostgreSQL.")
                return results
        
        logger.info("No similar object ID to retrieve or DB connection failed.")
        return []
        
    except Exception as e:
        logger.error(f"Error in retrieving recommendations: {str(e)}")
        st.error(f"Error in retrieving recommendations: {str(e)}")
        return []

def reset_filters_callback():
    """Reset filters and generate a new random seed."""
    st.session_state.creator_filter = None
    st.session_state.provider_filter = []
    st.session_state.tags_filter = []
    st.session_state.current_page = 1
    st.session_state.random_seed = random.random()

def increment_page():
    st.session_state.current_page += 1
    logger.info(f"Next page (callback): {st.session_state.current_page}")

def decrement_page():
    st.session_state.current_page -= 1
    logger.info(f"Next page (callback): {st.session_state.current_page}")


def process_image(image_url: str, target_width: int = 200, target_height: int = 200) -> Image.Image:
    """
    Download and process an image, resizing it to fit the frame
 without cropping (emulates object-fit: contain), adding padding if necessary.
     """
    try:
        response = requests.get(image_url, timeout=10) 
        response.raise_for_status() 
        img = Image.open(BytesIO(response.content))

        
        img.thumbnail((target_width, target_height), Image.LANCZOS)

        background_color = (30, 30, 30) 
        
        
        new_img = Image.new('RGB', (target_width, target_height), background_color)
        
        
        left = (target_width - img.width) // 2
        top = (target_height - img.height) // 2
        
        
        new_img.paste(img, (left, top))
        
        return new_img

    except requests.exceptions.RequestException as req_err:
        logger.error(f"Network or HTTP error while downloading the {image_url}: {req_err}")
        return Image.new('RGB', (target_width, target_height), color = 'grey') # Placeholder
    except Exception as e:
        logger.error(f"Generic error in processing the image {image_url}: {e}")
        return Image.new('RGB', (target_width, target_height), color = 'grey') # Placeholder


def render_gallery_view():
    """Render gallery view with filters and grid"""
    st.title("🏛️ Cultural Heritage Dashboard")

    if 'filter_options' not in st.session_state:
        st.session_state.filter_options = get_filter_options()

    with st.sidebar:
        st.header("Filters")
        
        selected_creator = st.selectbox(
            "Creator",
            options=[None] + st.session_state.filter_options.get('creators', []),
            key="creator_filter"
        )
        selected_provider = st.multiselect(
            "Provider",
            options=st.session_state.filter_options.get('provider', []),
            key="provider_filter"
        )
        selected_tags = st.multiselect(
            "Tags",
            options=st.session_state.filter_options.get('tags', []),
            key="tags_filter"
        )

        st.button("Reset Filters", on_click=reset_filters_callback)

    filters_changed = (
        st.session_state.get('selected_creator') != selected_creator or
        st.session_state.get('selected_provider') != selected_provider or
        st.session_state.get('selected_tags') != selected_tags
    )

    if filters_changed:
        st.session_state.selected_creator = selected_creator
        st.session_state.selected_provider = selected_provider
        st.session_state.selected_tags = selected_tags
        st.session_state.current_page = 1
        st.session_state.random_seed = random.random()
        st.rerun()

    filters = {
        'creator': st.session_state.get('selected_creator'),
        'provider': st.session_state.get('selected_provider'),
        'tags': st.session_state.get('selected_tags')
    }

    current_page = st.session_state.get('current_page', 1)
    seed = st.session_state.get('random_seed')

    with st.spinner("Charging results..."):
        gallery_data, total_results = search_guids(filters, current_page, seed=seed)

    st.session_state.gallery_data = gallery_data
    st.session_state.total_results = total_results

    st.info(f"Found {total_results} objects")

    if gallery_data:
        for row in range(5): 
            cols = st.columns(4) 
            for col_idx, col in enumerate(cols):
                item_idx = row * 4 + col_idx
                if item_idx < len(gallery_data):
                    item = gallery_data[item_idx]
                    image_url = get_image_url(item.get('image_url', []), item.get('isShownBy', []))
                    
                    with col:
                        processed_img = process_image(image_url, target_width=200, target_height=200)
                        st.image(processed_img, use_column_width=True, caption=item.get('title', ''))
                        if st.button(f"More details", key=f"detail_{item['id']}"):
                            st.session_state.current_view = 'detail'
                            st.session_state.current_guid = item['guid']
                            st.rerun()

    max_pages = min(3, (total_results + PAGE_SIZE - 1) // PAGE_SIZE)

    if max_pages > 1:
        col1, col2, col3 = st.columns([1, 2, 1])
        with col1:
            if st.session_state.current_page > 1:
                st.button("Previous", on_click=decrement_page)
        with col2:
            st.write(f"Page {st.session_state.current_page} of {max_pages}")
        with col3:
            if st.session_state.current_page < max_pages:
                st.button("Next", on_click=increment_page)
   

def render_detail_view():
    """Render object detail view"""
    if st.button("Back to Gallery"):
        st.session_state.current_view = 'gallery'
        st.rerun()

    guid = st.session_state.current_guid
    guid_data = get_guid_details(guid)

    if not guid_data:
        st.error("Object not found")
        return

    col_left, col_right = st.columns([3, 2])

    with col_left:
        image_url = get_image_url(guid_data.get('image_url', []), guid_data.get('isShownBy', []))
        processed_img = process_image(image_url, target_width=450, target_height=450)
        st.image(processed_img, use_column_width=True)
        if guid_data.get('title'):
            st.caption(guid_data['title'])

    with col_right:
        st.subheader("Description")
        metadata_fields = [
            ('Title', 'title'), ('Creator', 'creator'), ('Description', 'description'),
            ('Type', 'type'), ('Rights', 'edm_rights'),
            ('Data Provider', 'provider'),
        ]
        for label, field in metadata_fields:
            value = guid_data.get(field)
            
            if isinstance(value, list):
                display_value = ', '.join(value) if value else "N/A"
            else:
                display_value = value if value else "N/A"
                
            st.write(f"**{label}:** {display_value}")

        st.markdown("---") 
        
        
        with st.expander("Show Comments"):
            all_annotations = get_all_annotations_for_guid(guid)
            meaningful_annotations = [
                ann for ann in all_annotations
                if ann.get('comment') or ann.get('user_id') or (ann.get('tags') and len(ann['tags']) > 0)
            ]

            if meaningful_annotations:
                for i, ann in enumerate(meaningful_annotations):
                    
                    st.markdown(f"**Comment #{i+1}**")
                    if ann.get('user_id'): st.write(f"**User ID:** {ann['user_id']}")
                    if ann.get('timestamp'): st.write(f"**Timestamp:** {ann['timestamp']}")
                    if ann.get('comment'): st.write(f"**Comment:** {ann['comment']}")
                    
                   
                    if ann.get('tags'):
                        tags_list = ann['tags'] if isinstance(ann['tags'], list) else [ann['tags']]
                        if tags_list:
                            tags_formatted = " ".join([f"``{tag.strip()}``" for tag in tags_list if tag.strip()])
                            st.markdown(f"**Tags:** {tags_formatted}")
                        else:
                            st.write("**Tags:** N/A")
                    else:
                        st.write("**Tags:** N/A")
                    
                   
                    if i < len(meaningful_annotations) - 1:
                        st.divider() 
            else:
                st.info("No comments for this object.")

    st.subheader("Similar objects")
    recommendations = get_recommendations(guid)
    if recommendations:
        for row in range(2):
            cols = st.columns(5) 
            for col_idx, col in enumerate(cols):
                item_idx = row * 5 + col_idx 
                if item_idx < len(recommendations):
                    item = recommendations[item_idx]
                    image_url = get_image_url(item.get('image_url', []), item.get('isShownBy', []))
                    with col:
                        processed_img = process_image(image_url, target_width=180, target_height=180) 
                        st.image(processed_img, use_column_width=True)
                        if st.button(f"More details", key=f"rec_{item['id']}"):
                            st.session_state.current_guid = item['guid']
                            st.rerun()
    else:
        st.info("No recommendations available for this object.")

        
def initialize_session_state():
    """Initialize session state"""
    if 'current_view' not in st.session_state:
        st.session_state.current_view = 'gallery'
    if 'current_page' not in st.session_state:
        st.session_state.current_page = 1
    if 'selected_creator' not in st.session_state:
        st.session_state.selected_creator = None
    if 'selected_provider' not in st.session_state:
        st.session_state.selected_provider = []
    if 'selected_tags' not in st.session_state:
        st.session_state.selected_tags = []
    if 'random_seed' not in st.session_state:
        st.session_state.random_seed = random.random()

def main():
    """main"""
    st.set_page_config(page_title="Cultural Heritage Dashboard", page_icon="🏛️", layout="wide")
    
    initialize_session_state()
    
    if st.session_state.current_view == 'gallery':
        render_gallery_view()
    elif st.session_state.current_view == 'detail':
        render_detail_view()

if __name__ == "__main__":
    main()