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

# Configurazione logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Flag per errori di connessione
if 'db_conn_error_shown' not in st.session_state:
    st.session_state.db_conn_error_shown = False
if 'qdrant_conn_error_shown' not in st.session_state:
    st.session_state.qdrant_conn_error_shown = False

# Configurazione database
DB_CONFIG = {
    'host': 'postgres',
    'port': 5432,
    'database': 'heritage',
    'user': 'postgres',
    'password': 'postgres'
}

# Configurazione Qdrant
QDRANT_CONFIG = {
    'host': 'qdrant',
    'port': 6333
}

# Configurazione app
PAGE_SIZE = 20
MAX_RESULTS = 60
PLACEHOLDER_IMAGE = "https://via.placeholder.com/300?text=Non+trovata"

@st.cache_resource
def get_db_connection():
    """Crea connessione al database PostgreSQL"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        st.session_state.db_conn_error_shown = False
        return conn
    except Exception as e:
        if not st.session_state.db_conn_error_shown:
            logger.error(f"⚠️ Connessione fallita al database PostgreSQL: {str(e)}")
            st.error(f"⚠️ Connessione fallita al database PostgreSQL: {str(e)}")
            st.session_state.db_conn_error_shown = True
        return None

@st.cache_resource
def get_qdrant_client():
    """Crea client Qdrant"""
    try:
        client = QdrantClient(host=QDRANT_CONFIG['host'], port=QDRANT_CONFIG['port'])
        client.get_collections()
        logger.info("Connessione a Qdrant stabilita con successo.")
        st.session_state.qdrant_conn_error_shown = False
        return client
    except Exception as e:
        if not st.session_state.qdrant_conn_error_shown:
            logger.error(f"⚠️ Connessione fallita a Qdrant: {str(e)}")
            st.error(f"⚠️ Connessione fallita a Qdrant: {str(e)}")
            st.session_state.qdrant_conn_error_shown = True
        return None

def get_image_url(image_url_array: List[str], is_shown_by_array: List[str]) -> str:
    """Ottiene il primo URL di immagine valido o placeholder"""
    if image_url_array and len(image_url_array) > 0 and image_url_array[0]:
        return image_url_array[0]
    elif is_shown_by_array and len(is_shown_by_array) > 0 and is_shown_by_array[0]:
        return is_shown_by_array[0]
    else:
        return PLACEHOLDER_IMAGE

@st.cache_data
def get_filter_options() -> Dict[str, List[str]]:
    """Carica opzioni per filtri dropdown e multiselect"""
    conn = get_db_connection()
    if not conn:
        return {}
    
    try:
        cursor = conn.cursor()
        options = {}
        
        cursor.execute("SELECT DISTINCT creator FROM join_metadata_deduplicated WHERE creator IS NOT NULL ORDER BY creator")
        options['creators'] = [row[0] for row in cursor.fetchall()]
        
        cursor.execute("SELECT DISTINCT UNNEST(provider) FROM join_metadata_deduplicated WHERE provider IS NOT NULL ORDER BY 1")
        options['provider'] = [row[0] for row in cursor.fetchall()]
        
        cursor.execute("SELECT DISTINCT UNNEST(tags) FROM join_metadata_deduplicated WHERE tags IS NOT NULL ORDER BY 1")
        options['tags'] = [row[0] for row in cursor.fetchall()]
        
        cursor.close()
        return options
        
    except Exception as e:
        st.error(f"Error in charging filters: {str(e)}")
        return {}
    
def search_guids(filters: Dict, page: int, page_size: int = PAGE_SIZE, seed: Optional[float] = None) -> Tuple[List[Dict], int]:
    """Ricerca oggetti con filtri, ordinamento casuale e paginazione."""
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
            where_clauses.append("provider && %s")
            params.append(filters['provider'])
        if filters.get('tags'):
            where_clauses.append("tags && %s")
            params.append(filters['tags'])
        
        where_string = " AND ".join(where_clauses)

        count_query = f"SELECT COUNT(DISTINCT guid) FROM join_metadata_deduplicated WHERE {where_string}"
        cursor.execute(count_query, params)
        total_count = cursor.fetchone()[0]
        total_count = min(total_count, MAX_RESULTS)

        subquery = f"SELECT DISTINCT ON (guid) * FROM join_metadata_deduplicated WHERE {where_string} ORDER BY guid, id"
        query_paginated = f"SELECT * FROM ({subquery}) AS distinct_items ORDER BY RANDOM() LIMIT %s OFFSET %s"
        
        final_params = params + [page_size, (page - 1) * page_size]
        cursor.execute(query_paginated, final_params)
        
        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]

        cursor.close()
        return results, total_count

    except Exception as e:
        st.error(f"Errore nella ricerca: {str(e)}")
        return [], 0

def get_guid_details(guid: str) -> Optional[Dict]:
    """Ottiene dettagli di un singolo oggetto"""
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
        st.error(f"Errore nel recupero dettagli: {str(e)}")
        return None

def get_all_annotations_for_guid(guid: str) -> List[Dict]:
    """Ottiene tutte le annotazioni (righe) per un dato guid."""
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
        st.error(f"Errore nel recupero di tutte le annotazioni per l'oggetto: {str(e)}")
        return []

# <--- MODIFICA: Funzione `get_recommendations` aggiornata --->
def get_recommendations(guid: str) -> List[Dict]:
    """Ottiene raccomandazioni simili da Qdrant"""
    client = get_qdrant_client()
    if not client:
        logger.warning("Client Qdrant non disponibile, impossibile ottenere raccomandazioni.")
        return []
    
    try:
        logger.info(f"Cercando vettore per guid: {guid} in Qdrant.")
        search_result = client.scroll(
            collection_name="heritage_embeddings",
            scroll_filter={"must": [{"key": "guid", "match": {"value": guid}}]},
            limit=1,
            with_vectors=True
        )
        
        if not search_result[0]:
            logger.warning(f"Nessun vettore trovato in Qdrant per guid: {guid}")
            return []
        
        current_named_vectors = search_result[0][0].vector
        query_vector_data = current_named_vectors.get("combined")
        
        if query_vector_data is None:
            logger.error("Vettore 'combined' non trovato per l'oggetto corrente.")
            return []

        logger.info(f"Vettore trovato per guid: {guid}. Inizio ricerca oggetti simili.")
        
        similar_results = client.search(
            collection_name="heritage_embeddings",
            query_vector=NamedVector(name="combined", vector=query_vector_data),
            limit=11,
            # score_threshold=0.75, # <--- MODIFICA: Rimosso per garantire sempre dei risultati
            append_payload=True
        )
        
        similar_ids = [res.payload["guid"] for res in similar_results if res.payload and "guid" in res.payload and res.payload["guid"] != guid]
        similar_ids = similar_ids[:10]
        
        logger.info(f"Trovati {len(similar_ids)} ID oggetti simili: {similar_ids}")

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
                logger.info(f"Recuperati {len(results)} metadati per oggetti simili da PostgreSQL.")
                return results
        
        logger.info("Nessun ID oggetto simile da recuperare o connessione DB fallita.")
        return []
        
    except Exception as e:
        logger.error(f"Errore nel recupero raccomandazioni: {str(e)}")
        st.error(f"Errore nel recupero raccomandazioni: {str(e)}")
        return []

# <--- MODIFICA: Aggiunta la funzione di callback per il reset dei filtri --->
def reset_filters_callback():
    """Resetta i filtri e genera un nuovo seed casuale."""
    st.session_state.creator_filter = None
    st.session_state.provider_filter = []
    st.session_state.tags_filter = []
    st.session_state.current_page = 1
    st.session_state.random_seed = random.random()

def increment_page():
    st.session_state.current_page += 1
    logger.info(f"Pagina successiva (callback): {st.session_state.current_page}")

def decrement_page():
    st.session_state.current_page -= 1
    logger.info(f"Pagina precedente (callback): {st.session_state.current_page}")


# @st.cache_data # Potresti voler cachare anche questa funzione per performance
def process_image(image_url: str, target_width: int = 200, target_height: int = 200) -> Image.Image:
    """
    Scarica e processa un'immagine, ridimensionandola per adattarsi al riquadro
    senza tagliare (emula object-fit: contain), aggiungendo padding se necessario.
    """
    try:
        response = requests.get(image_url, timeout=10) # Aggiunto timeout
        response.raise_for_status() # Lancia un errore per risposte HTTP errate
        img = Image.open(BytesIO(response.content))

        # Ridimensiona l'immagine mantenendo l'aspect ratio per adattarsi al riquadro
        img.thumbnail((target_width, target_height), Image.LANCZOS)

        # Crea un'immagine di sfondo (canvas) delle dimensioni target
        # E incolla l'immagine ridimensionata al centro.
        # Il colore di sfondo sarà trasparente se l'immagine originale ha un canale alpha,
        # altrimenti sarà nero per impostazione predefinita. Puoi specificarlo: color=(0,0,0) per nero
        # o color=(240,240,240) per un grigio chiaro.
        
        # Per un background solido e visibile, puoi creare una nuova immagine con il colore desiderato
        # e poi incollare l'immagine processata sopra.
        
        background_color = (30, 30, 30) # Un colore scuro che si abbini al tema di Streamlit
        
        # Creiamo un'immagine con il colore di sfondo desiderato
        new_img = Image.new('RGB', (target_width, target_height), background_color)
        
        # Calcoliamo la posizione per centrare l'immagine ridimensionata
        left = (target_width - img.width) // 2
        top = (target_height - img.height) // 2
        
        # Incolliamo l'immagine ridimensionata sul nuovo sfondo
        new_img.paste(img, (left, top))
        
        return new_img

    except requests.exceptions.RequestException as req_err:
        logger.error(f"Errore di rete o HTTP durante il download di {image_url}: {req_err}")
        return Image.new('RGB', (target_width, target_height), color = 'grey') # Placeholder
    except Exception as e:
        logger.error(f"Errore generico nel processare l'immagine {image_url}: {e}")
        return Image.new('RGB', (target_width, target_height), color = 'grey') # Placeholder


def render_gallery_view():
    """Renderizza la vista gallery con filtri e griglia"""
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

        # <--- MODIFICA: Il pulsante ora usa la funzione di callback --->
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
        for row in range(5): # Assicurati che questo sia corretto per le tue 5 pagine x 20 = 100 risultati
            cols = st.columns(4) # 4 colonne
            for col_idx, col in enumerate(cols):
                item_idx = row * 4 + col_idx # 4 immagini per riga
                if item_idx < len(gallery_data):
                    item = gallery_data[item_idx]
                    image_url = get_image_url(item.get('image_url', []), item.get('isShownBy', []))
                    
                    with col:
                        # Processa l'immagine prima di visualizzarla
                        # Puoi scegliere dimensioni diverse per la galleria
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
                # Usa la callback on_click per il pulsante "Previous"
                st.button("Previous", on_click=decrement_page)
        with col2:
            st.write(f"Page {st.session_state.current_page} of {max_pages}")
        with col3:
            if st.session_state.current_page < max_pages:
                # Usa la callback on_click per il pulsante "Next"
                st.button("Next", on_click=increment_page)
   

def render_detail_view():
    """Renderizza la vista dettagli oggetto"""
    if st.button("Back to Gallery"):
        st.session_state.current_view = 'gallery'
        # Rimuovi l'associazione esplicita dei filtri per permettere a st.rerun() di ridisegnare i widget con i valori di session_state
        st.rerun()

    guid = st.session_state.current_guid
    guid_data = get_guid_details(guid)

    if not guid_data:
        st.error("Object not found")
        return

    col_left, col_right = st.columns([3, 2])

    with col_left:
        image_url = get_image_url(guid_data.get('image_url', []), guid_data.get('isShownBy', []))
        # Per la vista dettagli, potresti volere un'immagine più grande
        processed_img = process_image(image_url, target_width=450, target_height=450) #MODIFICARE: Dimensioni per la vista dettagli
        st.image(processed_img, use_column_width=True)
        if guid_data.get('title'):
            st.caption(guid_data['title'])

    with col_right:
        st.subheader("Description")
        metadata_fields = [
            ('Title', 'title'), ('Creator', 'creator'), ('Description', 'description'),
            ('Type', 'type'), ('Subject', 'subject'), ('Rights', 'rights'),
            ('Data Provider', 'dataProvider'), ('Language', 'language')
        ]
        for label, field in metadata_fields:
            value = guid_data.get(field)
            if value:
                st.write(f"**{label}:** {', '.join(value) if isinstance(value, list) else value}")

        st.subheader("Comments")
        all_annotations = get_all_annotations_for_guid(guid)
        meaningful_annotations = [
            ann for ann in all_annotations
            if ann.get('comment') or ann.get('user_id') or (ann.get('tags') and len(ann['tags']) > 0)
        ]
        if meaningful_annotations:
            for i, ann in enumerate(meaningful_annotations):
                st.markdown(f"---")
                st.write(f"**Comment #{i+1}**")
                if ann.get('user_id'): st.write(f"**User ID:** {ann['user_id']}")
                if ann.get('timestamp'): st.write(f"**Timestamp:** {ann['timestamp']}")
                if ann.get('comment'): st.write(f"**Comment:** {ann['comment']}")
                if ann.get('tags'): st.write(f"**Tags:** {', '.join(ann['tags']) if isinstance(ann['tags'], list) else ann['tags']}")
        else:
            st.info("No comments for this object.")

    st.subheader("Similar objects")
    recommendations = get_recommendations(guid)
    if recommendations:
        for row in range(2):
            cols = st.columns(5) # 5 colonne per le raccomandazioni
            for col_idx, col in enumerate(cols):
                item_idx = row * 5 + col_idx # 5 immagini per riga
                if item_idx < len(recommendations):
                    item = recommendations[item_idx]
                    image_url = get_image_url(item.get('image_url', []), item.get('isShownBy', []))
                    with col:
                        # Processa anche qui le immagini simili
                        processed_img = process_image(image_url, target_width=180, target_height=180) # Dimensioni adatte per la sezione simili
                        st.image(processed_img, use_column_width=True)
                        if st.button(f"More details", key=f"rec_{item['id']}"):
                            st.session_state.current_guid = item['guid']
                            st.rerun()
    else:
        st.info("No recommendations available for this object.")

        
def initialize_session_state():
    """Inizializza session state"""
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
    """Funzione principale"""
    st.set_page_config(page_title="Cultural Heritage Dashboard", page_icon="🏛️", layout="wide")
    
    initialize_session_state()
    
    if st.session_state.current_view == 'gallery':
        render_gallery_view()
    elif st.session_state.current_view == 'detail':
        render_detail_view()

if __name__ == "__main__":
    main()