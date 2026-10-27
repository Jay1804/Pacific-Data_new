"""Streamlit app: builds the client tracker for any list of client_external_id values.

Run:  streamlit run pacific_app.py
"""
import re

import pandas as pd
import streamlit as st

import build_tracker as bt

st.set_page_config(page_title="Pacific Tracker", layout="wide")
st.title("Client Tracker Builder")


def parse_ids(text):
    """All integer tokens in free text (comma / space / newline separated)."""
    return [int(x) for x in re.findall(r"\d+", text or "")]


def ids_from_file(f):
    """client_external_id column if present, otherwise the first column."""
    df = pd.read_csv(f, dtype=str) if f.name.lower().endswith(".csv") else pd.read_excel(f, dtype=str)
    col = next((c for c in df.columns if re.sub(r"[^a-z]", "", str(c).lower()) in
                ("clientexternalid", "clientid", "externalid")), df.columns[0])
    return parse_ids(" ".join(df[col].dropna().astype(str)))


with st.sidebar:
    st.header("Settings")
    process_name = st.text_input("Process name", bt.DEFAULT_PROCESS)
    st.subheader("MIS portal (iBridge export)")
    mis_user = st.text_input("User ID", help="Your own MIS portal login. Used only for this session, never saved.")
    mis_password = st.text_input("Password", type="password")

st.subheader("Client external IDs")
c1, c2 = st.columns(2)
with c1:
    text = st.text_area("Paste IDs (comma, space or new-line separated)", height=160,
                        placeholder="7840, 7838, 7825 ...")
with c2:
    upload = st.file_uploader("...or upload a CSV / Excel file", type=["csv", "xlsx", "xls"],
                              help="Uses the 'client_external_id' column if there is one, otherwise the first column.")

ids = []
try:
    ids = parse_ids(text) + (ids_from_file(upload) if upload else [])
except Exception as e:
    st.error(f"Could not read the uploaded file: {e}")
ids = list(dict.fromkeys(ids))  # de-duplicate, keep order
st.caption(f"{len(ids)} unique client IDs")

if st.button("Build tracker", type="primary", disabled=not ids):
    if not (mis_user and mis_password):
        st.warning("Enter your MIS portal user ID and password in the sidebar, otherwise the 'iBridge Case Initiation Date' column will be empty.")
    log = st.status("Running...", expanded=True)
    try:
        df, ars = bt.build(ids, process_name, mis_user, mis_password, progress=log.write)
        log.update(label="Done", state="complete")
        st.session_state["result"] = df
    except Exception as e:
        log.update(label="Failed", state="error")
        st.exception(e)

df = st.session_state.get("result")
if df is not None:
    if df.empty:
        st.info("No cases found for these client IDs and process.")
    else:
        m1, m2 = st.columns(2)
        m1.metric("Check rows", f"{len(df):,}")
        m2.metric("Cases", f"{df['case_ars_no'].nunique():,}")
        st.download_button("Download Excel", bt.to_excel_bytes(df), "Tracker.xlsx",
                           "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        st.dataframe(df, width='stretch', hide_index=True,
                     column_config={c: st.column_config.DateColumn(c, format='DD-MMM-YYYY')
                                    for c in bt.DATE_COLS})
