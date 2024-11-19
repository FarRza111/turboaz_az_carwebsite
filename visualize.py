import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from sqlalchemy import create_engine
import os

# Get database URL from environment variable or use default SQLite database
DATABASE_URL = os.getenv('DATABASE_URL', 'sqlite:///./data/cars.db')

def load_data():
    """Load data from the database"""
    engine = create_engine(DATABASE_URL)
    query = "SELECT price, year, mileage, brand FROM cars WHERE price > 0 AND year > 1950 AND mileage > 0"
    df = pd.read_sql(query, engine)
    return df

def create_scatter_plots(df):

    # Price vs Year
    fig1 = px.scatter(df, x='year', y='price', 
                     title='Car Prices vs Year',
                     labels={'year': 'Year', 'price': 'Price (AZN)'},
                     color='brand',
                     hover_data=['mileage'])
    fig1.show()

    # Price vs Mileage
    fig2 = px.scatter(df, x='mileage', y='price',
                     title='Car Prices vs Mileage',
                     labels={'mileage': 'Mileage', 'price': 'Price (AZN)'},
                     color='brand',
                     hover_data=['year'])
    fig2.show()

    # Year vs Mileage with Price as color
    fig3 = px.scatter(df, x='year', y='mileage',
                     title='Car Mileage vs Year (Color indicates Price)',
                     labels={'year': 'Year', 'mileage': 'Mileage', 'price': 'Price (AZN)'},
                     color='price',
                     hover_data=['brand'])
    fig3.show()

def main():

    print("Loading data from database...")
    df = load_data()
    
    print(f"Creating visualizations for {len(df)} cars...")
    create_scatter_plots(df)
    
    print("\nData Summary:")
    print(f"Average Price: AZN{df['price'].mean():,.2f}")
    print(f"Average Year: {df['year'].mean():.1f}")
    print(f"Average Mileage: {df['mileage'].mean():,.1f}")
    print(f"\nNumber of unique brands: {df['brand'].nunique()}")
    print("\nTop 5 brands by count:")
    print(df['brand'].value_counts().head())

if __name__ == "__main__":
    main()
