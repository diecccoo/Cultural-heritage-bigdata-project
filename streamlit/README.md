# Cultural Heritage Streamlit Dashboard

This folder contains the **Streamlit dashboard app** for interactive exploration, search, and recommendation of deduplicated cultural heritage objects and their user annotations.

---

## Features

- **Visual Exploration:** Browse a grid of deduplicated cultural heritage objects, each showing an image, title, and creator for easy exploration.
- **Filtering:** Filter by creator, provider, or tags (inclusive OR logic).
- **Detail View:** Click “More details” to see all metadata and a large image for any object/ Easily navigate back to the gallery/home view at any time.
- **User Annotation** View generated comments
- **Recommendations:** At the bottom of the detail view, up to 10 recommended objects are displayed, based on vector similarity in Qdrant using combined image+text CLIP embeddings (each recommended object can be directly accessed via “More details”).
- **Robust Image Handling:** Automatically fetches images or displays a placeholder if unavailable. Images are resized and centered for clean display in both gallery and detail views.
- **Paging & Randomization:** Results are paged (20 per page) and randomized for a dynamic browsing experience.
- **Error Handling:** UI feedback for database or Qdrant connection issues.

---

## File Structure

```text
streamlit/
│
├── app/
│   ├── app.py             # Main Streamlit dashboard app
│   ├── Dockerfile         # Container definition for the dashboard
│   └── requirements.txt   # Python dependencies for the dashboard
│
└── README.md              # This documentation file
