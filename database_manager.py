import os
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker

import urllib.parse
import pandas as pd
import calendar

class DatabaseManager:
    def __init__(self, host_name, user_name, user_password, db_name, driver="ODBC Driver 17 for SQL Server"):
        self.host_name = host_name
        self.user_name = user_name
        self.user_password = user_password
        self.db_name = db_name
        self.driver = driver
        self.engine = None
        self.Session = None

    def __enter__(self):
        self.create_engine()
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_connection()

    def create_engine(self):
        try:
            # Encode the password to handle special characters
            encoded_password = urllib.parse.quote(self.user_password)
            connection_string = (
                f"mssql+pyodbc://{self.user_name}:{encoded_password}"
                f"@{self.host_name}/{self.db_name}?driver={urllib.parse.quote(self.driver)}"
            )
            self.engine = create_engine(
                connection_string,
                echo=False,
                connect_args={}
            )
            print("Database engine created successfully.")
        except SQLAlchemyError as e:
            print(f"Error creating engine: {e}")
            self.engine = None


    def execute_query(self, query, data=None):
        """
        Execute a SQL query with optional parameters.
        Handles transactions explicitly and optimizes result fetching.
        """
        if not self.engine:
            print("Connection engine not initialized.")
            return None

        try:
            with self.engine.begin() as connection:
                if data:
                    result = connection.execute(text(query), data)
                else:
                    result = connection.execute(text(query))
                rows = [row for row in result]
                return rows
        except SQLAlchemyError as e:
            print(f"Error executing query: {e}")
            return None

    def close_connection(self):
        if self.Session:
            self.session.close()
            print("Session closed.")
        if self.engine:
            self.engine.dispose()
            print("Engine disposed.")

    def save_tables_to_parquet(self, output_dir="output"):
        """
        Saves all tables in the database to local Parquet files.

        Args:
            output_dir (str): The directory where the Parquet files will be stored.
        """
        if not self.engine:
            print("Connection engine not initialized.")
            return

        try:
            # Create output directory if it doesn't exist
            os.makedirs(output_dir, exist_ok=True)

            inspector = inspect(self.engine)
            tables = inspector.get_table_names()

            for table in tables:
                query = f"SELECT * FROM {table}"
                df = pd.read_sql(query, self.engine)

                # Save to Parquet format
                output_path = os.path.join(output_dir, f"{table}.parquet")
                df.to_parquet(output_path, index=False)

        except SQLAlchemyError as e:
            print(f"Error retrieving or saving tables: {e}")
    
    def group_and_display_customers_by_country(self):
        """
        Query the database to count customers by country
        and display the grouped DataFrame for the top 5 countries with the most customers.
        """
        if not self.engine:
            print("Connection engine not initialized.")
            return

        try:
            # SQL query to group and count customers by Country
            query = """
            SELECT Country, COUNT(CustomerId) AS CustomerCount
            FROM Customer
            GROUP BY Country
            """

            # Load query result into a Pandas DataFrame
            df = pd.read_sql(query, self.engine)

            # Get top 5 countries by customer count
            top_countries = df.nlargest(5, "CustomerCount")

            # Display the top 5 countries
            print("\nTop 5 Countries with the Most Customers:")
            print(top_countries.to_string(index=False))

        except Exception as e:
            print(f"Error querying or processing data: {e}")
    

    def analyze_sales(self):
        """
        Perform an analysis on sales data to find:
        1. The top 5 best-selling tracks
        2. The top 5 artists with the most sales
        3. The month with the most sales
        4. The top 5 best-selling genres
        """
        if not self.engine:
            print("Connection engine not initialized.")
            return

        try:
            # SQL query to get the necessary data
            query = """
            SELECT il.TrackId, t.Name AS TrackName, a.Name AS ArtistName, g.Name AS GenreName,
                SUM(il.Quantity) AS QuantitySold,
                SUM(il.Quantity * il.UnitPrice) AS SalesAmount,
                YEAR(i.InvoiceDate) AS InvoiceYear,
                MONTH(i.InvoiceDate) AS InvoiceMonth
            FROM InvoiceLine il
            JOIN Invoice i ON il.InvoiceId = i.InvoiceId
            JOIN Track t ON il.TrackId = t.TrackId
            JOIN Album al ON t.AlbumId = al.AlbumId
            JOIN Artist a ON al.ArtistId = a.ArtistId
            JOIN Genre g ON t.GenreId = g.GenreId
            GROUP BY il.TrackId, t.Name, a.Name, g.Name, YEAR(i.InvoiceDate), MONTH(i.InvoiceDate)
            """

            # Load the data into a DataFrame
            df = pd.read_sql(query, self.engine)

            # Top 5 Best-Selling Tracks
            top_tracks = df.groupby("TrackName")["QuantitySold"].sum().nlargest(5).reset_index()

            # Top 5 Best-Selling Artists
            top_artists = df.groupby("ArtistName")["SalesAmount"].sum().nlargest(5).reset_index()

            # Month with the Most Sales
            monthly_sales = df.groupby(["InvoiceYear", "InvoiceMonth"])["SalesAmount"].sum()
            max_sales = monthly_sales.idxmax()  # Returns a tuple (year, month)
            year_with_most_sales, month_with_most_sales = max_sales
            month_name = calendar.month_name[month_with_most_sales]  # Translate month number to name

            # Top 5 Best-Selling Genres
            top_genres = df.groupby("GenreName")["SalesAmount"].sum().nlargest(5).reset_index()

            # Display results
            print("\nTop 5 Best-Selling Tracks:")
            print(top_tracks.to_string(index=False))

            print("\nTop 5 Best-Selling Artists:")
            print(top_artists.to_string(index=False))

            print(f"\nMonth with the Most Sales: {month_name} {year_with_most_sales}")

            print("\nTop 5 Best-Selling Genres:")
            print(top_genres.to_string(index=False))

        except Exception as e:
            print(f"Error performing analysis: {e}")   