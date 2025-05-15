
# Sparkplug Dashboard

This dashboard allows users to explore Sparkplug B data stored in Snowflake, apply filters, and visualize key metrics from the SPARKPLUG_RAW table. Built with Streamlit and Snowflake Snowpark.

## Features

- Connects to Snowflake using Snowpark
- Displays min/max insertion dates
- Interactive sidebar filters (Namespace, Group, Edge, Device...)
- Date range selection
- Live KPIs (message count, unique topics)
- Raw data preview


## Getting Started

### 1. Clone the repo

```bash
git clone https://github.com/manea-palluat/sparkplug-dashboard
cd sparkplug-dashboard
```

### 2. Set up virtual environment

```powershell
# From PowerShell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope Process
python -m venv venv
venv\Scripts\activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

> Create a requirements.txt with:
```txt
streamlit
python-dotenv
snowflake-snowpark-python
```

### 4. Configure Snowflake

Copy .env.template to .env and fill in your credentials:

```bash
cp .env.template .env
```

```env
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account

SNOWFLAKE_ROLE=your_role
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_db
SNOWFLAKE_SCHEMA=your_schema
```

### 5. Run the app

```bash
streamlit run app.py
```

## Project Structure

```
├── app.py                  # Main Streamlit app
├── config.py               # Loads and validates .env variables
├── utils.py                # Helper functions (session, table)
├── .env.template           # Sample environment file
├── requirements.txt        # Python dependencies
```

## Development & CI

Make sure to activate your venv each time before launching:

```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope Process
venv\Scripts\activate
```

A GitHub Actions workflow (ci.yml) can be added to check code style, run tests, or validate deployment.

## Built With

- Streamlit
- Snowflake
- Snowpark Python
