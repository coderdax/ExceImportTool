import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# Database setup (SQLite for demo; replace with your DB URL)
DB_URL = 'sqlite:///imported_data.db'
engine = create_engine(DB_URL)

# Define table schemas and validation rules per dataset
# For P&L, use a list of sheets with configs
DATASETS = {
    'Valuations': {
        'sheets': [{
            'sheet_name': 0,  # Default single sheet
            'table_name': 'valuations',
            'columns': {'date': 'datetime', 'asset': 'str', 'value': 'float'},
            'required_cols': ['date', 'asset', 'value'],
            'numeric_cols': ['value']
        }]
    },
    'Risk': {
        'sheets': [{
            'sheet_name': 0,
            'table_name': 'risk',
            'columns': {'date': 'datetime', 'risk_factor': 'str', 'exposure': 'float'},
            'required_cols': ['date', 'risk_factor', 'exposure'],
            'numeric_cols': ['exposure']
        }]
    },
    'P&L': {
        'sheets': [
            {
                'sheet_name': 'Actuals',  # Or 0 if by index
                'table_name': 'pnl_actuals',
                'columns': {'date': 'datetime', 'account': 'str', 'profit_loss': 'float'},
                'required_cols': ['date', 'account', 'profit_loss'],
                'numeric_cols': ['profit_loss']
            },
            {
                'sheet_name': 'KPIs',  # Or 1 if by index
                'table_name': 'pnl_kpis',
                'columns': {'date': 'datetime', 'kpi_type': 'str', 'kpi_name': 'str', 'kpi_value': 'float'},
                'required_cols': ['date', 'kpi_type', 'kpi_name', 'kpi_value'],
                'numeric_cols': ['kpi_value']
            }
        ]
    }
}

# Create tables if they don't exist
def create_tables():
    for dataset, info in DATASETS.items():
        for sheet in info['sheets']:
            table = sheet['table_name']
            cols = ', '.join([f"{col} {dtype.upper() if dtype != 'datetime' else 'DATE'}" for col, dtype in sheet['columns'].items()])
            create_query = f"CREATE TABLE IF NOT EXISTS {table} (id INTEGER PRIMARY KEY AUTOINCREMENT, {cols})"
            with engine.connect() as conn:
                conn.execute(text(create_query))

create_tables()

# Validation function - returns df, check_results (dict of check: (pass/fail, message)), error_locations (for highlighting)
def validate_data(df, config):
    check_results = {}
    error_locations = []  # List of (row_index, column_name) for errors

    # Log the loaded columns
    st.write(f"Loaded columns for sheet '{config.get('sheet_name', 'unknown')}': {list(df.columns)}")

    # Check 1: Required columns with detailed error message
    missing_cols = set(config['required_cols']) - set(df.columns)
    sheet_name = config.get('sheet_name', 'unknown sheet')
    if missing_cols:
        error_msg = f"Missing column(s) from sheet '{sheet_name}': {', '.join(missing_cols)}"
        check_results['Columns Check'] = (False, error_msg)
    else:
        check_results['Columns Check'] = (True, f"All required columns present in sheet '{sheet_name}'")

    # Check 2: Data types and conversion
    type_errors = {}
    for col, dtype in config['columns'].items():
        if col in df.columns:
            try:
                if dtype == 'datetime':
                    df[col] = pd.to_datetime(df[col], errors='coerce')  # Coerce to find invalids
                    invalid = df[df[col].isnull()].index.tolist()
                    if invalid:
                        type_errors[col] = invalid
                        for idx in invalid:
                            error_locations.append((idx, col))
                elif dtype == 'float':
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    invalid = df[df[col].isnull()].index.tolist()
                    if invalid:
                        type_errors[col] = invalid
                        for idx in invalid:
                            error_locations.append((idx, col))
                elif dtype == 'str':
                    df[col] = df[col].astype(str)
            except Exception as e:
                type_errors[col] = f"Conversion failed: {str(e)}"
    if type_errors:
        check_results['Data Types Check'] = (False, f"Invalid data types in: {type_errors}")
    else:
        check_results['Data Types Check'] = (True, "All data types valid")

    # Check 3: Missing values in required columns
    missing_vals = {}
    for col in config['required_cols']:
        if df[col].isnull().any():
            invalid = df[df[col].isnull()].index.tolist()
            missing_vals[col] = invalid
            for idx in invalid:
                error_locations.append((idx, col))
    if missing_vals:
        check_results['Missing Values Check'] = (False, f"Missing values in: {missing_vals}")
    else:
        check_results['Missing Values Check'] = (True, "No missing values in required columns")

    # Check 4: Checksum (sum of numerics > 0 per row)
    if config['numeric_cols']:
        df['checksum'] = df[config['numeric_cols']].sum(axis=1)
        invalid_checksums = df[df['checksum'] <= 0]
        if not invalid_checksums.empty:
            invalid_rows = invalid_checksums.index.tolist()
            check_results['Checksum Check'] = (False, f"Invalid checksums in {len(invalid_rows)} rows")
            for idx in invalid_rows:
                for col in config['numeric_cols']:
                    error_locations.append((idx, col))
        else:
            check_results['Checksum Check'] = (True, "All checksums valid")
        df.drop('checksum', axis=1, inplace=True)  # Drop temp column

    # Overall errors
    errors = [msg for passed, msg in check_results.values() if not passed]

    return df, check_results, errors, error_locations

# Styler function to highlight errors
def highlight_errors(df, error_locations):
    def color_red(row):
        # Convert error_locations to a set of tuples for O(1) lookup
        error_set = set(error_locations)
        return ['background-color: red' if (row.name, col) in error_set else '' for col in df.columns]

    # Apply the styling row-wise
    styler = df.style.apply(color_red, axis=1)
    return styler

# Streamlit UI
st.title("Excel Data Import App")

# Step 1: Select dataset
dataset = st.selectbox("Select Dataset", list(DATASETS.keys()))

# Step 2: Upload file
uploaded_file = st.file_uploader("Browse and Select Excel File", type=["xlsx", "xls"])

if uploaded_file:
    try:
        config = DATASETS[dataset]
        sheets = config['sheets']
        all_dfs = {}
        all_check_results = {}
        all_errors = []
        all_error_locations = {}
        all_valid = True

        # Read sheets
        for sheet in sheets:
            sheet_name = sheet['sheet_name']
            df = pd.read_excel(uploaded_file, sheet_name=sheet_name)
            df, check_results, errors, error_locations = validate_data(df, sheet)
            all_dfs[sheet['table_name']] = df
            all_check_results[sheet['table_name']] = check_results
            all_errors.extend(errors)
            all_error_locations[sheet['table_name']] = error_locations
            if errors:
                all_valid = False

        # Display validation summary
        st.subheader("Validation Summary")
        for table_name, check_results in all_check_results.items():
            if len(sheets) > 1:
                st.write(f"**Sheet: {table_name.replace('pnl_', '').capitalize()}**")
            for check, (passed, msg) in check_results.items():
                icon = "✅" if passed else "❌"
                st.markdown(f"{check}: {icon} - {msg}")

        if all_errors:
            st.error("Validation Errors Found - Fix issues before publishing.")
        else:
            st.success("All Validations Passed!")

        # Display previews with highlights
        st.subheader("Data Preview (Ready for Import)")
        for table_name, df in all_dfs.items():
            if len(sheets) > 1:
                st.write(f"**Sheet: {table_name.replace('pnl_', '').capitalize()}**")
            error_locs = all_error_locations.get(table_name, [])
            styler = highlight_errors(df, set(error_locs))  # Use set to avoid duplicates
            st.dataframe(styler)

        # Publish button - only if valid
        if all_valid and st.button("Save"):
            try:
                for sheet in sheets:
                    table_name = sheet['table_name']
                    df = all_dfs[table_name]
                    df.to_sql(table_name, engine, if_exists='append', index=False)
                st.success("Data published successfully!")
            except SQLAlchemyError as e:
                st.error(f"Error publishing: {str(e)}")
    except Exception as e:
        st.error(f"Error processing file: {str(e)}")