# import pandas as pd
# from functools import reduce
# import urllib
# from sqlalchemy import create_engine

# conn_str = (
#     r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
#     r"DBQ=.\data\raw_data\IPEDS202223.accdb;"
# )

# quoted = urllib.parse.quote_plus(conn_str)
# engine = create_engine(f"access+pyodbc:///?odbc_connect={quoted}")

# table_cols = {
#     "IC2022_AY":   ["TUITION1", "TUITION2", "TUITION3", "CHG4AY0", "CHG4AY1", "CHG4AY2", "CHG4AY3"],
#     "HD2022":      ["CONTROL", "ICLEVEL", "SECTOR", "LOCALE"],
#     "SFA2122_P1":  ["LOAN_A", "FLOAN_A", "OLOAN_A", "AGRNT_A", "PGRNT_A"],
#     "SFA2122_P2":  ["NPIST0", "NPIST1", "NPIST2", "NPIS412", "NPIS422", "NPIS432", "NPIS442", "NPIS452"],
#     "IC2022_PY":   ["CIPCODE2", "CIPCODE3", "CIPTUIT2", "CIPTUIT3"],
# }

# # Pull each table into a DataFrame
# dfs = []
# for tbl, cols in table_cols.items():
#     all_cols = ["[UNITID]"] + [f"[{c}]" for c in cols]
#     sql = f"SELECT {', '.join(all_cols)} FROM [{tbl}]"
#     df = pd.read_sql_query(sql, engine)
#     dfs.append(df)

# #Merge everything on UNITID. Used outer so we don’t lose any schools
# merged = reduce(lambda a, b: pd.merge(a, b, on="UNITID", how="outer"), dfs)

# out_path = r".\data\parsed_data\selected_socioeconomic_factors.csv"
# merged.to_csv(out_path, index=False)
# print(f"Wrote combined data to {out_path}")

#!/usr/bin/env python3



# ============================================================================================================


import os
import sys
import argparse
import urllib
import pandas as pd
from functools import reduce
from sqlalchemy import create_engine, text




# def get_table_columns(engine, table_name):
#     """
#     Returns the list of column names available in a given table.
#     """
#     with engine.connect() as conn:
#         result = conn.execute(text(f"SELECT * FROM [{table_name}] WHERE 1=0"))
#         return list(result.keys())


# def preprocess_df(df):
#     """
#     Basic preprocessing:
#     - Drop duplicates on UNITID
#     - Remove columns with >50% missing values
#     - Fill numeric NaNs with median
#     - Fill object NaNs with 'Unknown'
#     """
#     # 1) Drop duplicate UNITID
#     df = df.drop_duplicates(subset="UNITID")

#     # 2) Drop columns with more than 50% missing
#     thresh = len(df) * 0.5
#     df = df.dropna(axis=1, thresh=len(df) - thresh)

#     # 3) Fill numeric missing with median
#     for col in df.select_dtypes(include='number').columns:
#         df[col] = df[col].fillna(df[col].median())

#     # 4) Fill object missing with 'Unknown'
#     for col in df.select_dtypes(include='object').columns:
#         df[col] = df[col].fillna('Unknown')

#     return df


# def main(db_name):
#     # # Extract base filename (e.g. IPEDS202223) for output suffix
#     # base = os.path.basename(db_path)
#     # name, _ = os.path.splitext(base)

#     # Build connection string and engine
#     conn_str = (
#         r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
#         f"DBQ=.\\data\\raw_data\\{db_name}.accdb;"        
#     )
#     print(f"DBQ=.\\data\\raw_data\\{db_name}.accdb;")
#     quoted = urllib.parse.quote_plus(conn_str)
#     engine = create_engine(f"access+pyodbc:///?odbc_connect={quoted}")

#     last_four = int(db_name[-4:])
#     target_year = db_name[5:-2]
#     last_four -= 101
#     print(last_four)
#     print(target_year)
    
#     # Tables and their desired columns
#     table_cols = {
#         f"IC{target_year}_AY":   ["TUITION1", "TUITION2", "TUITION3", "CHG4AY0", "CHG4AY1", "CHG4AY2", "CHG4AY3"],
#         f"HD{target_year}":      ["CONTROL", "ICLEVEL", "SECTOR", "LOCALE"],
#         f"SFA{last_four}_P1":  ["LOAN_A", "FLOAN_A", "OLOAN_A", "AGRNT_A", "PGRNT_A"],
#         f"SFA{last_four}_P2":  ["NPIST0", "NPIST1", "NPIST2", "NPIS412", "NPIS422", "NPIS432", "NPIS442", "NPIS452"],
#         f"IC{last_four}_PY":   ["CIPCODE2", "CIPCODE3", "CIPTUIT2", "CIPTUIT3"],
#     }

#     dfs = []
#     for tbl, cols in table_cols.items():
#         # Check available columns in the table
#         available = get_table_columns(engine, tbl)
#         cols_found = [c for c in cols if c in available]
#         missing = set(cols) - set(cols_found)
#         if missing:
#             print(f"Warning: Columns {missing} not found in {tbl}, skipping.")

#         # Always include UNITID if present
#         if "UNITID" not in available:
#             print(f"Error: UNITID not found in table {tbl}. Skipping table.")
#             continue
#         all_cols = ["UNITID"] + cols_found

#         if not cols_found:
#             continue

#         # Query only existing columns
#         col_list = ", ".join(f"[{c}]" for c in all_cols)
#         sql = f"SELECT {col_list} FROM [{tbl}]"
#         df = pd.read_sql_query(sql, engine)
#         dfs.append(df)

#     if not dfs:
#         print("No tables returned data. Exiting.")
#         return

#     # Merge on UNITID
#     merged = reduce(lambda a, b: pd.merge(a, b, on="UNITID", how="outer"), dfs)

#     # Preprocess merged DataFrame
#     merged = preprocess_df(merged)

#     # Ensure output directory exists
#     # os.makedirs(output_dir, exist_ok=True)
#     # out_file = os.path.join(output_dir, f"selected_socioeconomic_factors_{name}.csv")

#     out_file = os.path.join(r".\data\parsed_data", f"\\selected_socioeconomic_factors_{db_name}.csv")
#     # merged.to_csv(out_file, index=False)
#     print(f"Wrote combined data to {out_file}")


# if __name__ == "__main__":
#     parser = argparse.ArgumentParser(
#         description="Extract socioeconomic factors from an IPEDS Access database"
#     )
#     parser.add_argument(
#         "db_name", help="Path to the .accdb file (e.g., ./data/raw_data/IPEDS202223.accdb)"
#     )
#     # parser.add_argument(
#     #     "-o", "--output_dir",
#     #     default=".",
#     #     help="Directory where the output CSV will be saved"
#     # )
#     args = parser.parse_args()
#     main(args.db_name)

#=============================================================================================================

def get_table_columns(engine, table_name):
    """
    Returns the list of column names available in a given table.
    """
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT * FROM [{table_name}] WHERE 1=0"))
        return list(result.keys())


def preprocess_df(df):
    """
    Basic preprocessing:
    - Drop duplicates on UNITID
    - Remove columns with >50% missing values
    - Fill numeric NaNs with median
    - Fill object NaNs with 'Unknown'
    """
    # 1) Drop duplicate UNITID
    df = df.drop_duplicates(subset="UNITID")

    # 2) Drop columns with more than 50% missing
    thresh = len(df) * 0.5
    df = df.dropna(axis=1, thresh=len(df) - thresh)

    # 3) Fill numeric missing with median
    for col in df.select_dtypes(include='number').columns:
        df[col] = df[col].fillna(df[col].median())

    # 4) Fill object missing with 'Unknown'
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].fillna('Unknown')

    return df

#         r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
#         f"DBQ=.\\data\\raw_data\\{db_name}.accdb;"      
def main(db_path):
    # Normalize and verify the database path exists
    db_path_norm = os.path.normpath(db_path)
    db_path_abs = os.path.abspath(db_path_norm)
    if not os.path.isfile(db_path_abs):
        print(f"Error: Database file not found at '{db_path_abs}'.")
        print("Please ensure you are using a valid path, e.g. './data/raw_data/IPEDS202223.accdb' or '.\\data\\raw_data\\IPEDS202223.accdb'.")
        sys.exit(1)

    # Extract base filename (e.g. IPEDS202223) for output suffix
    base = os.path.basename(db_path_abs)
    name, _ = os.path.splitext(base)

    # Build connection string and engine
    conn_str = (
        r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
        f"DBQ={db_path_abs};"
    )
    quoted = urllib.parse.quote_plus(conn_str)
    try:
        engine = create_engine(f"access+pyodbc:///?odbc_connect={quoted}")
    except Exception as e:
        print(f"Error creating database engine: {e}")
        sys.exit(1)

    # Tables and their desired columns
    table_cols = {
        "IC2022_AY":   ["TUITION1", "TUITION2", "TUITION3", "CHG4AY0", "CHG4AY1", "CHG4AY2", "CHG4AY3"],
        "HD2022":      ["CONTROL", "ICLEVEL", "SECTOR", "LOCALE"],
        "SFA2122_P1":  ["LOAN_A", "FLOAN_A", "OLOAN_A", "AGRNT_A", "PGRNT_A"],
        "SFA2122_P2":  ["NPIST0", "NPIST1", "NPIST2", "NPIS412", "NPIS422", "NPIS432", "NPIS442", "NPIS452"],
        "IC2022_PY":   ["CIPCODE2", "CIPCODE3", "CIPTUIT2", "CIPTUIT3"],
    }

    dfs = []
    for tbl, cols in table_cols.items():
        # Check available columns in the table
        available = get_table_columns(engine, tbl)
        cols_found = [c for c in cols if c in available]
        missing = set(cols) - set(cols_found)
        if missing:
            print(f"Warning: Columns {missing} not found in '{tbl}', skipping these columns.")

        # Always include UNITID if present
        if "UNITID" not in available:
            print(f"Error: 'UNITID' not found in table '{tbl}'. Skipping table.")
            continue
        all_cols = ["UNITID"] + cols_found

        if not cols_found:
            continue

        # Query only existing columns
        col_list = ", ".join(f"[{c}]" for c in all_cols)
        sql = f"SELECT {col_list} FROM [{tbl}]"
        try:
            df = pd.read_sql_query(sql, engine)
        except Exception as e:
            print(f"Error querying table '{tbl}': {e}")
            continue
        dfs.append(df)

    if not dfs:
        print("No data retrieved from any table. Exiting.")
        sys.exit(1)

    # Merge on UNITID
    merged = reduce(lambda a, b: pd.merge(a, b, on="UNITID", how="outer"), dfs)

    # Preprocess merged DataFrame
    merged = preprocess_df(merged)

    # Ensure output directory exists
    # os.makedirs(output_dir, exist_ok=True)
    out_file = os.path.join(r".\data\parsed_data", f"\\selected_socioeconomic_factors_IPEDS202223.csv")
    # merged.to_csv(out_file, index=False)
    print(f"Wrote combined data to '{out_file}'")



if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Extract socioeconomic factors from an IPEDS Access database"
    )
    parser.add_argument(
        "db_name", help="Path to the .accdb file (e.g., ./data/raw_data/IPEDS202223.accdb)"
    )
    # parser.add_argument(
    #     "-o", "--output_dir",
    #     default=".",
    #     help="Directory where the output CSV will be saved"
    # )
    args = parser.parse_args()
    main(args.db_name)