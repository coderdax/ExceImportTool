import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# Database setup (SQLite for demo; replace with your DB URL, e.g., 'postgresql://user:pass@localhost/db')
DB_URL = 'sqlite:///imported_data.db'
engine = create_engine(DB_URL)

# Define table schemas and validation rules per dataset
DATASETS = {
    'Valuations': {
        'table_name': 'valuations',
        'columns': {'date': 'datetime', 'asset': 'str', 'value': 'float'},
        'required_cols': ['date', 'asset', 'value'],
        'numeric_cols': ['value']
    },
    'Risk': {
        'table_name': 'risk',
        'columns': {'date': 'datetime', 'risk_factor': 'str', 'exposure': 'float'},
        'required_cols': ['date', 'risk_factor', 'exposure'],
        'numeric_cols': ['exposure']
    },
    'P&L': {
        'table_name': 'pnl',
        'columns': {'date': 'datetime', 'account': 'str', 'profit_loss': 'float'},
        'required_cols': ['date', 'account', 'profit_loss'],
        'numeric_cols': ['profit_loss']
    }
}


# Create tables if they don't exist
def create_tables():
    for dataset, info in DATASETS.items():
        table = info['table_name']
        cols = ', '.join(
            [f"{col} {dtype.upper() if dtype != 'datetime' else 'DATE'}" for col, dtype in info['columns'].items()])
        create_query = f"CREATE TABLE IF NOT EXISTS {table} (id INTEGER PRIMARY KEY AUTOINCREMENT, {cols})"
        with engine.connect() as conn:
            conn.execute(text(create_query))


create_tables()


# Validation function
def validate_data(df, config):
    errors = []

    # Check required columns
    missing_cols = set(config['required_cols']) - set(df.columns)
    if missing_cols:
        errors.append(f"Missing columns: {missing_cols}")

    # Data type validation and conversion
    for col, dtype in config['columns'].items():
        if col in df.columns:
            try:
                if dtype == 'datetime':
                    df[col] = pd.to_datetime(df[col], errors='raise')
                elif dtype == 'float':
                    df[col] = pd.to_numeric(df[col], errors='raise')
                elif dtype == 'str':
                    df[col] = df[col].astype(str)
            except ValueError as e:
                errors.append(f"Invalid data in {col}: {str(e)}")

    # Check for missing values in required columns
    for col in config['required_cols']:
        if df[col].isnull().any():
            errors.append(f"Missing values in {col}")

    # Checksum: Simple sum check for numeric columns (e.g., ensure row sums > 0)
    if config['numeric_cols']:
        df['checksum'] = df[config['numeric_cols']].sum(axis=1)
        invalid_checksums = df[df['checksum'] <= 0]
        if not invalid_checksums.empty:
            errors.append(f"Invalid checksums in {len(invalid_checksums)} rows (sums <= 0)")

    # Drop temp column
    if 'checksum' in df.columns:
        df.drop('checksum', axis=1, inplace=True)

    return df, errors


# Streamlit UI
st.title("Excel Data Import App")

# Step 1: Select dataset
dataset = st.selectbox("Select Dataset", list(DATASETS.keys()))

# Step 2: Upload file
uploaded_file = st.file_uploader("Browse and Select Excel File", type=["xlsx", "xls"])

if uploaded_file:
    try:
        # Read Excel (assuming data starts at row 0, sheet 0; adjust if needed)
        df = pd.read_excel(uploaded_file, sheet_name=0)

        config = DATASETS[dataset]

        # Step 3: Validate
        df, errors = validate_data(df, config)

        if errors:
            st.error("Validation Errors:")
            for err in errors:
                st.write(f"- {err}")
        else:
            # Step 4: Display preview
            st.success("Validation Passed! Preview of data ready for import:")
            st.dataframe(df)

            # Step 5: Publish button
            if st.button("Publish to SQL"):
                try:
                    df.to_sql(config['table_name'], engine, if_exists='append', index=False)
                    st.success(f"Data published to {config['table_name']} table successfully!")
                except SQLAlchemyError as e:
                    st.error(f"Error publishing to SQL: {str(e)}")
    except Exception as e:
        st.error(f"Error reading file: {str(e)}")