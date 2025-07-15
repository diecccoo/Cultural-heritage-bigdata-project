import streamlit as st
import psycopg2
from qdrant_client import QdrantClient
import pandas as pd
from typing import List, Dict, Optional, Tuple
import logging
from qdrant_client.models import NamedVector # <--- AGGIUNGI QUESTA RIGA

# Configurazione logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s') # Aggiunto format per più dettagli
logger = logging.getLogger(__name__)

# aggiungo flag per tenere traccia se è stato mostrato un errore di connessione principale per prevenire visualizzazione
# ripetuta di errori o tentativi di log ricorsivi
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
        st.session_state.db_conn_error_shown = False # Resetta il flag se la connessione ha successo
        return conn
    except Exception as e:
        # Solo loggare/mostrare l'errore una volta o con un limite
        if not st.session_state.db_conn_error_shown:
            logger.error(f"⚠️ Connessione fallita al database PostgreSQL: {str(e)}")
            st.error(f"⚠️ Connessione fallita al database PostgreSQL: {str(e)}")
            st.session_state.db_conn_error_shown = True # Imposta il flag a True
        return None

@st.cache_resource
def get_qdrant_client():
    """Crea client Qdrant"""
    try:
        client = QdrantClient(host=QDRANT_CONFIG['host'], port=QDRANT_CONFIG['port'])
        client.get_collections() 
        logger.info("Connessione a Qdrant stabilita con successo.")
        st.session_state.qdrant_conn_error_shown = False # Resetta il flag se la connessione ha successo
        return client
    except Exception as e:
        if not st.session_state.qdrant_conn_error_shown:
            logger.error(f"⚠️ Connessione fallita a Qdrant: {str(e)}")
            st.error(f"⚠️ Connessione fallita a Qdrant: {str(e)}")
            st.session_state.qdrant_conn_error_shown = True # Imposta il flag a True
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

    
def search_guids(filters: Dict, page: int, page_size: int = PAGE_SIZE) -> Tuple[List[Dict], int]:
    """Ricerca oggetti con filtri e paginazione, gestendo i duplicati per guid."""
    conn = get_db_connection()
    if not conn:
        return [], 0

    try:
        cursor = conn.cursor()

        # Usiamo DISTINCT ON (guid) per prendere solo una riga per ogni guid
        # e ordiniamo per guid per una consistenza nella selezione del "primo" duplicato
        # Inseriamo l'ordinamento anche all'interno del DISTINCT ON per determinare quale riga viene selezionata
        # E poi un secondo ORDER BY per la paginazione, che può essere lo stesso.

        query_base = "SELECT DISTINCT ON (guid) * FROM join_metadata_deduplicated WHERE image_url IS NOT NULL AND image_url[1] IS NOT NULL"
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

        # Per il conteggio totale, dobbiamo contare gli guid distinti dopo i filtri
        count_query = f"SELECT COUNT(DISTINCT guid) FROM join_metadata_deduplicated WHERE image_url IS NOT NULL AND image_url[1] IS NOT NULL"
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
        query_paginated = f"{query_base} ORDER BY guid, id LIMIT %s OFFSET %s" # Ordina per guid e poi per id (l'ID univoco dell'annotazione)
        params.extend([page_size, (page - 1) * page_size])

        cursor.execute(query_paginated, params)
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
        # Seleziona tutte le righe che hanno lo stesso guid
        cursor.execute("SELECT * FROM join_metadata_deduplicated WHERE guid = %s ORDER BY timestamp DESC", (guid,))

        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]

        cursor.close()
        return results

    except Exception as e:
        st.error(f"Errore nel recupero di tutte le annotazioni per l'oggetto: {str(e)}")
        return []

def get_recommendations(guid: str) -> List[Dict]:
    """Ottiene raccomandazioni simili da Qdrant"""
    client = get_qdrant_client()
    if not client:
        logger.warning("Client Qdrant non disponibile, impossibile ottenere raccomandazioni.")
        return []
    
    try:
        # Ricerca vettore dell'oggetto corrente
        logger.info(f"Cercando vettore per guid (che è guid): {guid} in Qdrant.")
        search_result = client.scroll(
            collection_name="heritage_embeddings",
            scroll_filter={
                "must": [
                    {
                        "key": "guid", # Campo corretto
                        "match": {"value": guid}
                    }
                ]
            },
            limit=1,
            with_vectors=True
        )
        
        if not search_result[0]:
            logger.warning(f"Nessun vettore trovato in Qdrant per guid (guid): {guid}")
            return []
        
        # Ottieni i vettori nominati dall'oggetto corrente
        current_named_vectors = search_result[0][0].vector # Questo sarà un dizionario come {"combined": [...], "image": [...]}
        
        # Seleziona il vettore specifico da usare per la query (es. "combined")
        query_vector_data = current_named_vectors.get("combined")
        
        if query_vector_data is None:
            logger.error("Vettore 'combined' non trovato per l'oggetto corrente. Impossibile cercare raccomandazioni.")
            return []

        logger.info(f"Vettore trovato per guid (guid): {guid}. Inizio ricerca oggetti simili.")
        
        # Ricerca oggetti simili
        similar_results = client.search(
            collection_name="heritage_embeddings",
            # Modifica qui: crea un'istanza di NamedVector
            query_vector=NamedVector(
                name="combined", # Specifica il nome del vettore
                vector=query_vector_data # Passa i dati del vettore
            ),
            limit=11,  # 10 + 1 (oggetto corrente)
            score_threshold=0.75,
            append_payload=True
        )
        
        # Filtra oggetto corrente e ottieni ID
        similar_ids = []
        for result in similar_results:
            # Assicurati che 'payload' e 'guid' esistano nel risultato prima di accedervi
            if result.payload and "guid" in result.payload and result.payload["guid"] != guid:
                similar_ids.append(result.payload["guid"])
        
        similar_ids = similar_ids[:10]
        
        logger.info(f"Trovati {len(similar_ids)} ID oggetti simili: {similar_ids}")

        # Ottieni metadati da PostgreSQL
        if similar_ids:
            conn = get_db_connection()
            if conn:
                cursor = conn.cursor()
                placeholders = ','.join(['%s'] * len(similar_ids))
                # La query dovrebbe essere corretta se 'guid' in PostgreSQL corrisponde a 'guid' in Qdrant
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

def render_gallery_view():
    """Renderizza la vista gallery con filtri e griglia"""
    st.title("🏛️ Cultural Heritage Dashboard")

    # Carica opzioni filtri
    if 'filter_options' not in st.session_state:
        st.session_state.filter_options = get_filter_options()

    # Sidebar con filtri
    with st.sidebar:
        st.header("🔍 Filters")

        # Creator dropdown
        # Calcola l'indice iniziale per il selectbox del Creator
        try:
            # Tenta di trovare l'indice del valore selezionato nella lista delle opzioni reali
            # e aggiungi 1 perché [None] è la prima opzione
            current_creator_index = st.session_state.filter_options.get('creators', []).index(st.session_state.get('selected_creator')) + 1
        except ValueError:
            # Se il valore selezionato non è nella lista (es. è None), imposta l'indice a 0 (che è [None])
            current_creator_index = 0

        selected_creator = st.selectbox(
            "Creator",
            options=[None] + st.session_state.filter_options.get('creators', []),
            index=current_creator_index, # Usa l'indice calcolato qui
            key="creator_filter" # Aggiungi una chiave univoca per il widget
        )

        # Subject multiselect
        selected_subjects = st.multiselect(
            "Subject",
            options=st.session_state.filter_options.get('subjects', []),
            default=st.session_state.get('selected_subjects', [])
        )

        # Type dropdown
        # Calcola l'indice iniziale per il selectbox del Type
        try:
            current_type_index = st.session_state.filter_options.get('types', []).index(st.session_state.get('selected_type')) + 1
        except ValueError:
            current_type_index = 0

        selected_type = st.selectbox(
            "Type",
            options=[None] + st.session_state.filter_options.get('types', []),
            index=current_type_index, # Usa l'indice calcolato qui
            key="type_filter" # Aggiungi una chiave univoca per il widget
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
            st.rerun()

    # Aggiorna filtri in session state
    filters_changed = (
        st.session_state.get('selected_creator') != selected_creator or
        st.session_state.get('selected_subjects') != selected_subjects or
        st.session_state.get('selected_type') != selected_type or
        st.session_state.get('selected_tags') != selected_tags
    )

    if filters_changed:
    # Evita rerun infiniti con un flag
        if 'last_filter_rerun' not in st.session_state or not st.session_state.last_filter_rerun:
            st.session_state.selected_creator = selected_creator
            st.session_state.selected_subjects = selected_subjects
            st.session_state.selected_type = selected_type
            st.session_state.selected_tags = selected_tags
            st.session_state.current_page = 1
            st.session_state.last_filter_rerun = True
            st.rerun()
    else:
        st.session_state.last_filter_rerun = False


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
    with st.spinner("Caricamento risultati..."): # AGGIUNGI QUESTA RIGA
        gallery_data, total_results = search_guids(filters, current_page)

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
                            st.session_state.current_guid = item['guid']
                            st.rerun()
    # Paginazione
    max_pages = min(3, (total_results + PAGE_SIZE - 1) // PAGE_SIZE)

    if max_pages > 1:
        col1, col2, col3 = st.columns([1, 2, 1])

        with col1:
            if current_page > 1:
                if st.button("⬅️ Precedente"):
                    st.session_state.current_page = current_page - 1
                    st.rerun()

        with col2:
            st.write(f"Pagina {current_page} di {max_pages}")

        with col3:
            if current_page < max_pages:
                if st.button("Successiva ➡️"):
                    st.session_state.current_page = current_page + 1
                    st.rerun()

def render_detail_view():
    """Renderizza la vista dettagli oggetto"""
    # Pulsante back
    if st.button("⬅️ Torna alla ricerca"):
        st.session_state.current_view = 'gallery'
        st.rerun()

    # Ottieni dettagli oggetto (prenderà la riga "principale" con quel guid)
    guid = st.session_state.current_guid
    guid_data = get_guid_details(guid) # Questa funzione va bene così com'è

    if not guid_data:
        st.error("Oggetto non trovato")
        return

    # Layout principale (2 colonne)
    col_left, col_right = st.columns([3, 2])

    with col_left:
        # Immagine principale
        image_url = get_image_url(guid_data.get('image_url', []), guid_data.get('isShownBy', []))
        st.image(image_url, use_column_width=True)

        # Caption con titolo
        if guid_data.get('title'):
            st.caption(guid_data['title'])

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
            value = guid_data.get(field)
            if value:
                if isinstance(value, list):
                    value = ', '.join(value)
                st.write(f"**{label}:** {value}")

        # Sezione per TUTTE le annotazioni (commenti e tags)
        st.subheader("💬 Annotazioni Utente")

        all_annotations = get_all_annotations_for_guid(guid)

        # Filtra le annotazioni per includere solo quelle che hanno almeno un campo significativo
        # (comment, user_id, o tags con almeno un elemento)
        meaningful_annotations = [
            ann for ann in all_annotations
            if ann.get('comment') or ann.get('user_id') or (ann.get('tags') and len(ann['tags']) > 0)
        ]

        if meaningful_annotations:
            # Per ogni annotazione trovata, mostra i suoi dettagli
            for i, annotation in enumerate(meaningful_annotations):
                st.markdown(f"---") # Separatore per chiarezza
                st.write(f"**Annotazione #{i+1}**")
                if annotation.get('user_id'):
                    st.write(f"**User ID:** {annotation['user_id']}")
                if annotation.get('timestamp'):
                    st.write(f"**Timestamp:** {annotation['timestamp']}")
                if annotation.get('comment'):
                    st.write(f"**Commento:** {annotation['comment']}")
                if annotation.get('tags'):
                    # Assicurati che 'tags' sia una lista prima di unirla
                    tags_value = annotation['tags']
                    if isinstance(tags_value, list):
                        st.write(f"**Tags:** {', '.join(tags_value)}")
                    else:
                        st.write(f"**Tags:** {tags_value}") # Per il caso non sia una lista
        else:
            st.info("Nessuna annotazione utente disponibile per questo oggetto.")


    # Sezione oggetti simili
    st.subheader("🔍 Oggetti simili")

    recommendations = get_recommendations(guid)

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
                        if st.button(f"👁️", key=f"rec_{item['id']}"):
                            st.session_state.current_guid = item['guid']
                            st.rerun()
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


