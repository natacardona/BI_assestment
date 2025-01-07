from database_manager import DatabaseManager
   
if __name__ == "__main__":
    db_config = {
        "host_name": "",
        "user_name": "",
        "user_password": "",
        "db_name": ""
    }

    db_manager = DatabaseManager(**db_config)
    db_manager.create_engine()
    db_manager.save_tables_to_parquet(output_dir="parquet_tables")
    db_manager.group_and_display_customers_by_country()
    db_manager.analyze_sales()
    db_manager.close_connection()