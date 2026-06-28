import mysql.connector
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import pandas as pd
import os

default_args = {
    "owner": "Sachin",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2)
}

with DAG(
    dag_id="ecommerce_etl_pipeline",
    default_args=default_args,
    description="ETL pipeline for E-commerce dataset",
    schedule="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ecommerce", "2547245", "domain_5"]
) as dag:

    def extract_orders_csv(**context):
        file_path = "/opt/airflow/dags/domain_5_ecommerce.csv"

        df = pd.read_csv(file_path, sep=None, engine="python",on_bad_lines="skip")

        row_count = len(df)

        print(f"CSV loaded successfully with {row_count} rows")

        context["ti"].xcom_push(key="total_rows", value=row_count)

    extract_orders_task = PythonOperator(
        task_id="extract_orders_csv",
        python_callable=extract_orders_csv
    )

    def extract_metadata(**context):
        file_path = "/opt/airflow/dags/domain_5_ecommerce.csv"

        df = pd.read_csv(file_path, sep=None, engine="python", on_bad_lines="skip")

        column_count = len(df.columns)
        column_names = list(df.columns)
        null_count = int(df.isnull().sum().sum())

        print(f"Column count: {column_count}")
        print(f"Total null values: {null_count}")
        print(f"Columns: {column_names}")

        context["ti"].xcom_push(key="column_count", value=column_count)
        context["ti"].xcom_push(key="null_count", value=null_count)


    extract_metadata_task = PythonOperator(
        task_id="extract_metadata",
        python_callable=extract_metadata
    )

    def validate_data(**context):
        total_rows = context["ti"].xcom_pull(
            task_ids="extract_orders_csv",
            key="total_rows"
        )

        if total_rows and total_rows > 0:
            print("Valid data found. Proceeding to transform step.")
            return "transform_data"
        else:
            print("No valid data found.")
            return "notify_invalid_data"
    
    validate_task = BranchPythonOperator(
        task_id="branch_validate_data",
        python_callable=validate_data
    )

    invalid_data_task = EmptyOperator(
        task_id="notify_invalid_data"
    )


    def is_shifted_row(row):
        product_id = str(row.get("product_id", "")).strip()
        customer_id = str(row.get("customer_id", "")).strip()

        valid_product = product_id.startswith("P")
        valid_customer = (
            customer_id.startswith("C")
            or customer_id.startswith("TEST")
            or customer_id == ""
            or customer_id == "nan"
        )

        return not (valid_product and valid_customer)

    def repair_shifted_row(row):
        repaired = row.copy()

        repaired["city"] = row["return_status"]
        repaired["return_status"] = row["payment_mode"]
        repaired["payment_mode"] = row["delivery_date"]
        repaired["delivery_date"] = row["shipped_date"]
        repaired["shipped_date"] = row["order_date"]
        repaired["order_date"] = row["discount_pct"]
        repaired["discount_pct"] = row["unit_price"]
        repaired["unit_price"] = row["quantity"]
        repaired["quantity"] = row["brand"]
        repaired["brand"] = row["category"]
        repaired["category"] = row["product_name"]
        repaired["product_name"] = row["product_id"]
        repaired["product_id"] = row["customer_name"]
        repaired["customer_name"] = None

        return repaired

    def transform_data(**context):
        file_path = "/opt/airflow/dags/domain_5_ecommerce.csv"
        df = pd.read_csv(file_path, sep=None, engine="python", on_bad_lines="skip")
        repaired_rows = 0
        repaired_data = []

        for _, row in df.iterrows():
            if is_shifted_row(row):
                row = repair_shifted_row(row)
                repaired_rows += 1
            repaired_data.append(row)

        df = pd.DataFrame(repaired_data)

        print(f"Shifted rows repaired: {repaired_rows}")

        # Strip whitespace from string columns
        for col in df.columns:
            if df[col].dtype == "object":
                df[col] = df[col].astype(str).str.strip()

        # Remove TEST rows
        df = df[~df["customer_id"].astype(str).str.startswith("TEST", na=False)]

        # Fill missing values
        df["customer_id"] = df["customer_id"].replace("", pd.NA)
        df["customer_id"] = df["customer_id"].fillna("UNKNOWN_CUSTOMER")

        df["customer_name"] = df["customer_name"].replace("", pd.NA)
        df["customer_name"] = df["customer_name"].fillna("Guest Customer")

        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(1)

        # Fix negative price
        df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce").abs()

        df["discount_pct"] = pd.to_numeric(
            df["discount_pct"],
            errors="coerce"
        ).fillna(0)

        # Convert dates
        df["order_date"] = pd.to_datetime(df["order_date"], dayfirst=True, errors="coerce")
        df["shipped_date"] = pd.to_datetime(df["shipped_date"], dayfirst=True, errors="coerce")
        df["delivery_date"] = pd.to_datetime(df["delivery_date"], dayfirst=True, errors="coerce")

        # Compute revenue
        df["final_amount"] = (
            df["quantity"]
            * df["unit_price"]
            * (1 - df["discount_pct"] / 100)
        )

        # Delivery days
        df["delivery_days"] = (
            df["delivery_date"] - df["order_date"]
        ).dt.days

        # Missing delivery = pending
        df["delivery_days"] = df["delivery_days"].fillna(-1)

        # Delay rule for roll ending in 5
        df["is_delayed"] = df["delivery_days"].apply(
            lambda x: 1 if x > 5 else 0
        )

        # Return flag
        df["return_flag"] = df["return_status"].apply(
            lambda x: 1 if x == "Returned" else 0
        )

        cleaned_rows = len(df)

        print(f"Cleaned rows: {cleaned_rows}")

        output_path = "/opt/airflow/dags/cleaned_ecommerce.csv"
        df.to_csv(output_path, index=False)
        print(f"Saved cleaned CSV to {output_path}")

        context["ti"].xcom_push(
            key="cleaned_rows",
            value=cleaned_rows
        )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data
    )

    def load_to_mysql(**context):
        conn = mysql.connector.connect(
            host="host.docker.internal",
            user="root",
            password="Sachin10$$",
            port=3306
        )

        cursor = conn.cursor()

        # ---------- RAW LOAD ----------
        raw_df = pd.read_csv(
            "/opt/airflow/dags/domain_5_ecommerce.csv",
            sep=None,
            engine="python",
            on_bad_lines="skip"
        )

        cursor.execute("USE raw")
        cursor.execute("DELETE FROM ecommerce_orders")

        for _, row in raw_df.iterrows():
            values = tuple(None if pd.isna(x) else x for x in row)

            if len(values) != 16:
                print(f"Skipping malformed row: {values}")
                continue

            try:
                # Validate important columns before insert
                order_id = str(values[0])
                customer_id = str(values[1]) if values[1] is not None else ""
                discount_val = values[9]

                # order_id should start with EC
                if not order_id.startswith("EC"):
                    print(f"Skipping invalid order row: {values}")
                    continue

                # discount should be numeric, not date/string
                try:
                    float(discount_val)
                except:
                    print(f"Skipping shifted row (bad discount): {values}")
                    continue

                query = """
                INSERT INTO ecommerce_orders
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """
                cursor.execute(query, values)

            except Exception as e:
                print(f"Skipping bad row: {e}")
                continue

        conn.commit()
        print("Raw data loaded")

        # ---------- STAGING LOAD ----------
        clean_df = pd.read_csv("/opt/airflow/dags/cleaned_ecommerce.csv")

        cursor.execute("USE staging")
        cursor.execute("DELETE FROM ecommerce_cleaned")

        for _, row in clean_df.iterrows():
            values = tuple(None if pd.isna(x) else x for x in row)

            query = """
            INSERT INTO ecommerce_cleaned
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """
            cursor.execute(query, values)

        conn.commit()
        print("Staging data loaded")

        # ---------- REPORTING ----------
        cursor.execute("USE reporting")

        cursor.execute("DELETE FROM category_sales")
        cursor.execute("DELETE FROM city_sales")
        cursor.execute("DELETE FROM kpi_summary")

        # Category sales
        category_sales = clean_df.groupby("category").agg(
            total_orders=("order_id", "count"),
            total_quantity=("quantity", "sum"),
            total_revenue=("final_amount", "sum")
        ).reset_index()

        for _, row in category_sales.iterrows():
            cursor.execute(
                "INSERT INTO category_sales VALUES (%s,%s,%s,%s)",
                tuple(row)
            )

        # City sales
        city_sales = clean_df.groupby("city").agg(
            total_orders=("order_id", "count"),
            total_revenue=("final_amount", "sum")
        ).reset_index()

        for _, row in city_sales.iterrows():
            cursor.execute(
                "INSERT INTO city_sales VALUES (%s,%s,%s)",
                tuple(row)
            )

        # KPI summary
        kpis = [
            ("total_orders", len(clean_df)),
            ("total_revenue", round(clean_df["final_amount"].sum(), 2)),
            ("returned_orders", int(clean_df["return_flag"].sum())),
            ("avg_order_value", round(clean_df["final_amount"].mean(), 2)),
            ("delayed_orders", int(clean_df["is_delayed"].sum()))
        ]

        for kpi in kpis:
            cursor.execute(
                "INSERT INTO kpi_summary VALUES (%s,%s)",
                kpi
            )

        conn.commit()
        conn.close()

        print("Reporting tables loaded successfully")

    load_task = PythonOperator(
        task_id="load_to_mysql",
        python_callable=load_to_mysql
    )

    success_task = EmptyOperator(
        task_id="notify_success"
    )

    extract_orders_task >> extract_metadata_task >> validate_task
    validate_task >> transform_task >> load_task >> success_task
    validate_task >> invalid_data_task