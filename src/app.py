import streamlit as st
from snowflake.snowpark import Session
from config import SNOWFLAKE_CONFIG, validate_config
from datetime import datetime, timezone

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

