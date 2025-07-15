import streamlit as st
import psycopg2
from qdrant_client import QdrantClient
import pandas as pd
from typing import List, Dict, Optional, Tuple
import logging

# Configurazione logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
        return conn
    except Exception as e:
        st.error(f"⚠️ Connessione fallita al database PostgreSQL: {str(e)}")
        return None

@st.cache_resource
def get_qdrant_client():
    """Crea client Qdrant"""
    try:
        client = QdrantClient(host=QDRANT_CONFIG['host'], port=QDRANT_CONFIG['port'])
        return client
    except Exception as e:
        st.error(f"⚠️ Connessione fallita a Qdrant: {str(e)}")
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
        
        # Creators
        cursor.execute("SELECT DISTINCT creator FROM join_metadata_deduplicated WHERE creator IS NOT NULL ORDER BY creator")
        options['creators'] = [row[0] for row in cursor.fetchall()]
        
        # Subjects
        cursor.execute("SELECT DISTINCT UNNEST(subject) FROM join_metadata_deduplicated WHERE subject IS NOT NULL ORDER BY 1")
        options['subjects'] = [row[0] for row in cursor.fetchall()]
        
        # Types
        cursor.execute("SELECT DISTINCT type FROM join_metadata_deduplicated WHERE type IS NOT NULL ORDER BY type")
        options['types'] = [row[0] for row in cursor.fetchall()]
        
        # Tags
        cursor.execute("SELECT DISTINCT UNNEST(tags) FROM join_metadata_deduplicated WHERE tags IS NOT NULL ORDER BY 1")
        options['tags'] = [row[0] for row in cursor.fetchall()]
        
        cursor.close()
        return options
        
    except Exception as e:
        st.error(f"Errore nel caricamento filtri: {str(e)}")
        return {}

def search_objects(filters: Dict, page: int, page_size: int = PAGE_SIZE) -> Tuple[List[Dict], int]:
    """Ricerca oggetti con filtri e paginazione, gestendo i duplicati per id_object."""
    conn = get_db_connection()
    if not conn:
        return [], 0

    try:
        cursor = conn.cursor()

        # Usiamo DISTINCT ON (id_object) per prendere solo una riga per ogni id_object
        # e ordiniamo per id_object per una consistenza nella selezione del "primo" duplicato
        # Inseriamo l'ordinamento anche all'interno del DISTINCT ON per determinare quale riga viene selezionata
        # E poi un secondo ORDER BY per la paginazione, che può essere lo stesso.

        query_base = "SELECT DISTINCT ON (id_object) * FROM join_metadata_deduplicated WHERE image_url IS NOT NULL AND image_url[1] IS NOT NULL"
        params = []

        # Aggiunta filtri
        if filters.get('creator'):
            query_base += " AND creator = %s"
            params.append(filters['creator'])

        if filters.get('type'):
            query_base += " AND type = %s"
            params.append(filters['type'])

        if filters.get('subjects'):
            query_base += " AND subject && %s"
            params.append(filters['subjects'])

        if filters.get('tags'):
            query_base += " AND tags && %s"
            params.append(filters['tags'])

        # Per il conteggio totale, dobbiamo contare gli id_object distinti dopo i filtri
        count_query = f"SELECT COUNT(DISTINCT id_object) FROM join_metadata_deduplicated WHERE image_url IS NOT NULL AND image_url[1] IS NOT NULL"
        count_params = []
        if filters.get('creator'):
            count_query += " AND creator = %s"
            count_params.append(filters['creator'])
        if filters.get('type'):
            count_query += " AND type = %s"
            count_params.append(filters['type'])
        if filters.get('subjects'):
            count_query += " AND subject && %s"
            count_params.append(filters['subjects'])
        if filters.get('tags'):
            count_query += " AND tags && %s"
            count_params.append(filters['tags'])


        cursor.execute(count_query, count_params)
        total_count = cursor.fetchone()[0]

        # Limitazione risultati
        total_count = min(total_count, MAX_RESULTS)

        # Query paginata
        # Aggiungiamo un ORDER BY per garantire la consistenza di DISTINCT ON
        # e un secondo ORDER BY per la paginazione
        query_paginated = f"{query_base} ORDER BY id_object, id LIMIT %s OFFSET %s" # Ordina per id_object e poi per id (l'ID univoco dell'annotazione)
        params.extend([page_size, (page - 1) * page_size])

        cursor.execute(query_paginated, params)
        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]

        cursor.close()
        return results, total_count

    except Exception as e:
        st.error(f"Errore nella ricerca: {str(e)}")
        return [], 0

def search_objects(filters: Dict, page: int, page_size: int = PAGE_SIZE) -> Tuple[List[Dict], int]:
    """Ricerca oggetti con filtri e paginazione"""
    conn = get_db_connection()
    if not conn:
        return [], 0
    
    try:
        cursor = conn.cursor()
        
        query = "SELECT * FROM join_metadata_deduplicated WHERE image_url IS NOT NULL AND image_url[1] IS NOT NULL"
        params = []
        
        # Aggiunta filtri
        if filters.get('creator'):
            query += " AND creator = %s"
            params.append(filters['creator'])
            
        if filters.get('type'):
            query += " AND type = %s"
            params.append(filters['type'])
            
        if filters.get('subjects'):
            query += " AND subject && %s"
            params.append(filters['subjects'])
            
        if filters.get('tags'):
            query += " AND tags && %s"
            params.append(filters['tags'])
        
        # Count totale
        count_query = query.replace("SELECT *", "SELECT COUNT(*)")
        cursor.execute(count_query, params)
        total_count = cursor.fetchone()[0]
        
        # Limitazione risultati
        total_count = min(total_count, MAX_RESULTS)
        
        # Query paginata
        query += " ORDER BY id_object LIMIT %s OFFSET %s"
        params.extend([page_size, (page - 1) * page_size])
        
        cursor.execute(query, params)
        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        cursor.close()
        return results, total_count
        
    except Exception as e:
        st.error(f"Errore nella ricerca: {str(e)}")
        return [], 0

def get_object_details(object_id: str) -> Optional[Dict]:
    """Ottiene dettagli di un singolo oggetto"""
    conn = get_db_connection()
    if not conn:
        return None
    
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM join_metadata_deduplicated WHERE id_object = %s", (object_id,))
        
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

def get_recommendations(object_id: str) -> List[Dict]:
    """Ottiene raccomandazioni simili da Qdrant"""
    client = get_qdrant_client()
    if not client:
        return []
    
    try:
        # Ricerca vettore dell'oggetto corrente
        search_result = client.scroll(
            collection_name="heritage_embeddings",
            scroll_filter={
                "must": [
                    {
                        "key": "id_object",
                        "match": {"value": object_id}
                    }
                ]
            },
            limit=1,
            with_vectors=True
        )
        
        if not search_result[0]:
            return []
        
        # Ottieni vettore dell'oggetto
        current_vector = search_result[0][0].vector["combined"]
        
        # Ricerca oggetti simili
        similar_results = client.search(
            collection_name="heritage_embeddings",
            query_vector=current_vector,
            limit=11,  # 10 + 1 (oggetto corrente)
            score_threshold=0.75
        )
        
        # Filtra oggetto corrente e ottieni ID
        similar_ids = [
            result.payload["id_object"] 
            for result in similar_results 
            if result.payload["id_object"] != object_id
        ][:10]
        
        # Ottieni metadati da PostgreSQL
        if similar_ids:
            conn = get_db_connection()
            if conn:
                cursor = conn.cursor()
                placeholders = ','.join(['%s'] * len(similar_ids))
                query = f"SELECT * FROM join_metadata_deduplicated WHERE id_object IN ({placeholders})"
                cursor.execute(query, similar_ids)
                
                columns = [desc[0] for desc in cursor.description]
                results = [dict(zip(columns, row)) for row in cursor.fetchall()]
                cursor.close()
                return results
        
        return []
        
    except Exception as e:
        st.error(f"Errore nel recupero raccomandazioni: {str(e)}")
        return []

def render_gallery_view():
    """Renderizza la vista gallery con filtri e griglia"""
    st.title("🏛️ Cultural Heritage Dashboard")

    # Carica opzioni filtri
    if 'filter_options' not in st.session_state:
        st.session_state.filter_options = get_filter_options()

    # Sidebar con filtri
    with st.sidebar:
        st.header("🔍 Filtri")

        # Creator dropdown
        selected_creator = st.selectbox(
            "Creator",
            options=[None] + st.session_state.filter_options.get('creators', []),
            index=0 if not st.session_state.get('selected_creator') else
                  st.session_state.filter_options.get('creators', []).index(st.session_state.selected_creator) + 1
        )

        # Subject multiselect
        selected_subjects = st.multiselect(
            "Subject",
            options=st.session_state.filter_options.get('subjects', []),
            default=st.session_state.get('selected_subjects', [])
        )

        # Type dropdown
        selected_type = st.selectbox(
            "Type",
            options=[None] + st.session_state.filter_options.get('types', []),
            index=0 if not st.session_state.get('selected_type') else
                  st.session_state.filter_options.get('types', []).index(st.session_state.selected_type) + 1
        )

        # Tags multiselect
        selected_tags = st.multiselect(
            "Tags",
            options=st.session_state.filter_options.get('tags', []),
            default=st.session_state.get('selected_tags', [])
        )

        # Reset button
        if st.button("🔄 Reset Filters"):
            st.session_state.selected_creator = None
            st.session_state.selected_subjects = []
            st.session_state.selected_type = None
            st.session_state.selected_tags = []
            st.session_state.current_page = 1
            st.experimental_rerun()

    # Aggiorna filtri in session state
    filters_changed = (
        st.session_state.get('selected_creator') != selected_creator or
        st.session_state.get('selected_subjects') != selected_subjects or
        st.session_state.get('selected_type') != selected_type or
        st.session_state.get('selected_tags') != selected_tags
    )

    if filters_changed:
        st.session_state.selected_creator = selected_creator
        st.session_state.selected_subjects = selected_subjects
        st.session_state.selected_type = selected_type
        st.session_state.selected_tags = selected_tags
        st.session_state.current_page = 1

    # Costruisci filtri per query
    filters = {}
    if selected_creator:
        filters['creator'] = selected_creator
    if selected_subjects:
        filters['subjects'] = selected_subjects
    if selected_type:
        filters['type'] = selected_type
    if selected_tags:
        filters['tags'] = selected_tags

    # Ricerca oggetti
    current_page = st.session_state.get('current_page', 1)
    gallery_data, total_results = search_objects(filters, current_page)

    # Salva risultati in session state
    st.session_state.gallery_data = gallery_data
    st.session_state.total_results = total_results

    # Counter risultati
    st.info(f"📊 Trovati {total_results} oggetti")

    # Griglia immagini (4 colonne x 5 righe)
    if gallery_data:
        for row in range(5):
            cols = st.columns(4)
            for col_idx, col in enumerate(cols):
                item_idx = row * 4 + col_idx
                if item_idx < len(gallery_data):
                    item = gallery_data[item_idx]
                    image_url = get_image_url(item.get('image_url', []), item.get('isShownBy', []))

                    with col:
                        # AGGIUNGI QUESTA RIGA PER VISUALIZZARE L'IMMAGINE
                        st.image(image_url, use_column_width=True, caption=item.get('title', '')) # Aggiungi anche un caption opzionale

                        if st.button(f"📖 Dettagli", key=f"detail_{item['id']}"):
                            st.session_state.current_view = 'detail'
                            st.session_state.current_object_id = item['id_object']
                            st.experimental_rerun()
    # Paginazione
    max_pages = min(3, (total_results + PAGE_SIZE - 1) // PAGE_SIZE)

    if max_pages > 1:
        col1, col2, col3 = st.columns([1, 2, 1])

        with col1:
            if current_page > 1:
                if st.button("⬅️ Precedente"):
                    st.session_state.current_page = current_page - 1
                    st.experimental_rerun()

        with col2:
            st.write(f"Pagina {current_page} di {max_pages}")

        with col3:
            if current_page < max_pages:
                if st.button("Successiva ➡️"):
                    st.session_state.current_page = current_page + 1
                    st.experimental_rerun()

def render_detail_view():
    """Renderizza la vista dettagli oggetto"""
    # Pulsante back
    if st.button("⬅️ Torna alla ricerca"):
        st.session_state.current_view = 'gallery'
        st.experimental_rerun()
    
    # Ottieni dettagli oggetto
    object_id = st.session_state.current_object_id
    object_data = get_object_details(object_id)
    
    if not object_data:
        st.error("Oggetto non trovato")
        return
    
    # Layout principale (2 colonne)
    col_left, col_right = st.columns([3, 2])
    
    with col_left:
        # Immagine principale
        image_url = get_image_url(object_data.get('image_url', []), object_data.get('isShownBy', []))
        st.image(image_url, use_column_width=True)
        
        # Caption con titolo
        if object_data.get('title'):
            st.caption(object_data['title'])
    
    with col_right:
        # Metadati Europeana
        st.subheader("📚 Metadati Europeana")
        
        metadata_fields = [
            ('Title', 'title'),
            ('Creator', 'creator'),
            ('Description', 'description'),
            ('Type', 'type'),
            ('Subject', 'subject'),
            ('Rights', 'rights'),
            ('Data Provider', 'dataProvider'),
            ('Language', 'language')
        ]
        
        for label, field in metadata_fields:
            value = object_data.get(field)
            if value:
                if isinstance(value, list):
                    value = ', '.join(value)
                st.write(f"**{label}:** {value}")
        
        # User Generated Content
        st.subheader("💬 User Generated Content")
        
        ugc_fields = [
            ('Tags', 'tags'),
            ('Comment', 'comment'),
            ('User ID', 'user_id'),
            ('Timestamp', 'timestamp')
        ]
        
        for label, field in ugc_fields:
            value = object_data.get(field)
            if value:
                if isinstance(value, list):
                    value = ', '.join(value)
                st.write(f"**{label}:** {value}")
    
    # Sezione oggetti simili
    st.subheader("🔍 Oggetti simili")
    
    recommendations = get_recommendations(object_id)
    
    if recommendations:
        # Griglia 5x2
        for row in range(2):
            cols = st.columns(5)
            for col_idx, col in enumerate(cols):
                item_idx = row * 5 + col_idx
                if item_idx < len(recommendations):
                    item = recommendations[item_idx]
                    image_url = get_image_url(item.get('image_url', []), item.get('isShownBy', []))
                    
                    with col:
                        st.image(image_url, use_column_width=True)
                        if st.button(f"👁️", key=f"rec_{item['id']}"): # <-- cambiato da id_object a id
                            st.session_state.current_object_id = item['id_object']
                            st.experimental_rerun()
    else:
        st.info("Raccomandazioni non disponibili")

def initialize_session_state():
    """Inizializza session state"""
    if 'current_view' not in st.session_state:
        st.session_state.current_view = 'gallery'
    
    if 'current_page' not in st.session_state:
        st.session_state.current_page = 1
    
    if 'selected_creator' not in st.session_state:
        st.session_state.selected_creator = None
    
    if 'selected_subjects' not in st.session_state:
        st.session_state.selected_subjects = []
    
    if 'selected_type' not in st.session_state:
        st.session_state.selected_type = None
    
    if 'selected_tags' not in st.session_state:
        st.session_state.selected_tags = []

def main():
    """Funzione principale"""
    st.set_page_config(
        page_title="Cultural Heritage Dashboard",
        page_icon="🏛️",
        layout="wide"
    )
    
    # Inizializza session state
    initialize_session_state()
    
    # Routing principale
    if st.session_state.current_view == 'gallery':
        render_gallery_view()
    elif st.session_state.current_view == 'detail':
        render_detail_view()

if __name__ == "__main__":
    main()


