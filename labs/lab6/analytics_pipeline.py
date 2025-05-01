from prefect import task, flow, get_run_logger
import pandas as pd
import matplotlib.pyplot as plt
import os


@task
def fetch_data(file_path):
    logger = get_run_logger()
    logger.info(f"Reading data from {file_path}")
    # Assume a dataset with sales figures and other fields is provided.
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    df = pd.read_csv(file_path)

    return df


@task
def validate_data(df):
    logger = get_run_logger()
    logger.info("Validating data...")
    missing_values = df.isnull().sum()
    logger.info("Missing values:\n", missing_values)
    # For simplicity, drop any rows with missing values
    df_clean = df.dropna()

    return df_clean


@task
def transform_data(df):
    logger = get_run_logger()
    logger.info("Transforming data...")
    # For example, if there is a "sales" column, create a normalized version.
    if "sales" in df.columns:
        df["sales_normalized"] = (
            df["sales"] - df["sales"].mean()) / df["sales"].std()

    return df


@task
def generate_analytics_report(df, output_file):
    logger = get_run_logger()
    logger.info("Generating analytics report...")
    summary = df.describe()
    summary.to_csv(output_file)
    logger.info(f"Summary statistics saved to {output_file}")


@task
def create_histogram(df):
    logger = get_run_logger()
    if "sales" in df.columns:
        plt.hist(df["sales"], bins=20)
        plt.title("Sales Distribution")
        plt.xlabel("Sales")
        plt.ylabel("Frequency")
        plt.savefig("sales_histogram.png")
        plt.close()
        logger.info("Sales histogram saved to sales_histogram.png")


@flow(name="analytics_pipeline")
def analytics_pipeline():
    # Step 1: Fetch Data
    df = fetch_data("analytics_data.csv")

    # Step 2: Validate Data
    df_clean = validate_data(df)

    # Step 3: Transform Data
    df_clean = transform_data(df_clean)

    # Step 4: Generate Analytics Report
    generate_analytics_report(df_clean, "analytics_summary.csv")

    # Step 5: Create a Histogram for Sales Distribution
    create_histogram(df_clean)
    logger = get_run_logger()
    logger.info("Analytics pipeline completed.")


if __name__ == "__main__":
    analytics_pipeline()
