from extract import Extractor
from transform import Transformer
from load import Loader

def main():
    print("Starting ETL pipeline...")

    # Extract data from CSV files to staging database
    extractor = Extractor()
    extractor.run()

    # Transform data from bronze layer (staging) to silver layer
    transformer = Transformer()
    transformer.run()

    # Load data mart and generate report
    loader = Loader()
    loader.run()

    print("ETL Pipeline completed.")

if __name__ == "__main__":
    main()