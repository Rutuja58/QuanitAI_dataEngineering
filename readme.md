# QuantifAI Data Engineering Challenge 

This project solves a real-world messy data integration scenario across multiple e-commerce platforms. It includes an end-to-end ETL pipeline, data cleaning logic, a normalized SQLite database, and an interactive Streamlit dashboard to explore business insights.

---

## 🔧 Setup Instructions

### 1. Clone the repository & install dependencies

Ensure Python 3.10+ is installed. The key libraries used include:

- `pandas`
- `numpy`
- `streamlit`
- `plotly`
- `sqlite3`


### 2. Run the solution 

-cd ETL_pipeline

-Run cleaning function.py

-Run  ETL_Pipeline.py

It Loads raw data from dataset/raw_data , Cleans & saves to dataset/cleaned and Populates ecommerce.db with normalized tables


### 3. Launch streamlit dashboard 

--cd streamlite_app
then run --streamlit run dashboard.py

![Dashboard Screenshot](./dashboard.png)



### 4. Run schema reconcilliation

-- python gemini_schema_reconciliation.py


It Transforms mismatched schema in the reconciliation CSV , Aligns fields with the primary schema and Compares with the cleaned customer table to find new/unmatched entries



