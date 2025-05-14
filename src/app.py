import streamlit as st
from snowflake.snowpark import Session
from config import SNOWFLAKE_CONFIG, validate_config
from datetime import datetime, timezone
import time
import os


#vérifier la config
validate_config(SNOWFLAKE_CONFIG)

def create_snowpark_session():
    return Session.builder.configs(SNOWFLAKE_CONFIG).create()

#crée et conserve la session
session = create_snowpark_session()

#ui
st.set_page_config(page_title="Explorateur SPARKPLUG_RAW", layout="wide")
st.title("Explorateur SPARKPLUG_RAW")
st.write("Cette application permet d'explorer la table SPARKPLUG_RAW dans Snowflake.")

#charger la table
table_ref = "CL_BRIDGE_STAGE_DB.STAGE_DB.SPARKPLUG_RAW"
t = session.table(table_ref)

#exécution d'une requête pour obtenir min et max
res = (
    session
    .sql(f"SELECT MIN (INSERTED_AT) AS MN, MAX(INSERTED_AT) AS MX FROM {table_ref}") # requête pour obtenir min et max
    .collect()[0] # collect() renvoie une liste de Row, on prend le premier élément
    .as_dict() # convertit en dictionnaire
)

#conversion des résultats en datetime
min_ts = datetime.fromtimestamp(float(res["MN"])/1000, tz=timezone.utc) #conversion de la timestamp (qui est le nombre de millisecondes depuis 1970) en datetime
max_ts = datetime.fromtimestamp(float(res["MX"])/1000, tz=timezone.utc) #là pareil

st.write(min_ts, max_ts) #test affichage

#on va créer dans la sidebar des filtres interactifs sur les colonnes de la table

# charger les valeurs distinctes pour les filtres
namespaces = [r[0] for r in t.select("NAMESPACE").distinct().collect()]

groups = [r[0] for r in t.select("GROUP_ID").distinct().collect()]
msg_types = [r[0] for r in t.select("MESSAGE_TYPE").distinct().collect()]
edge_nodes = [r[0] for r in t.select("EDGE_NODE_ID").distinct().collect()]
devices = [r[0] for r in t.select("DEVICE_ID").distinct().collect()]

st.sidebar.header("Filtres")

sel_ns = st.sidebar.multiselect("Namespace", namespaces, default=namespaces)
sel_grp = st.sidebar.multiselect("Group ID", groups, default=groups)
sel_msgtp = st.sidebar.multiselect("Message Type", msg_types, default=msg_types)
sel_edge = st.sidebar.multiselect("Edge Node ID", edge_nodes, default=edge_nodes)
sel_dev = st.sidebar.multiselect("Device ID", devices, default=devices)

print("Warehouse utilisé :", os.getenv("SNOWFLAKE_WAREHOUSE"))

start_dt, end_dt = st.sidebar.slider(
    "Date d'insertion (UTC)",
    value=(min_ts, max_ts),
    format="DD/MM/YYYY HH:mm",
)

start_ms = int(start_dt.timestamp() * 1000)
end_ms = int(end_dt.timestamp() * 1000)

from snowflake.snowpark.functions import col

#construction du dataframe filtré
#càd on va filtrer les données en fonction des valeurs sélectionnées dans la sidebar
#df_raw est le dataframe brut, c'est-à-dire celui qui contient toutes les données
df_raw = (
    t
    .filter(col("NAMESPACE").isin(sel_ns))
    #filter permet de filtrer les lignes qui ne sont pas dans la liste sel_ns
    #col permet de sélectionner une colonne
    #isin permet de vérifier si la valeur de la colonne est dans la liste car isin renvoie un booléen
    #
    .filter(col("GROUP_ID").isin(sel_grp))
    .filter(col("MESSAGE_TYPE").isin(sel_msgtp))
    .filter(col("EDGE_NODE_ID").isin(sel_edge))
    .filter(col("DEVICE_ID").isin(sel_dev))
    .filter(col("INSERTED_AT").between(start_ms, end_ms))

)

st.write("Nombre de lignes :", df_raw.count()) #affiche le nombre de lignes

total_messages = df_raw.count() #compte le nombre de lignes
disinct_topics = df_raw.select("MSG_TOPIC").distinct().count() #compte le nombre de topics distincts

k1, k2 = st.columns(2) #crée deux colonnes
k1.metric("Nombre de messages", total_messages) #affiche le nombre de messages
k2.metric("Nombre de topics distincts", disinct_topics) #affiche le nombre de topics distincts



st.subheader("Aperçu brut (100 premières lignes)")
st.dataframe(df_raw.limit(100).to_pandas(), use_container_width=True)