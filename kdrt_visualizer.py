import streamlit as st
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')  
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
from pymongo import MongoClient
from datetime import datetime, timedelta
import re
from wordcloud import WordCloud
import nltk
from nltk.corpus import stopwords
from collections import Counter
from textblob import TextBlob

# Download NLTK resources
try:
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('stopwords')

# Set page configuration
st.set_page_config(
    page_title="KDRT News Analysis Dashboard",
    page_icon="📰",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Connect to MongoDB
@st.cache_resource
def get_database_connection():
    client = MongoClient('mongodb://localhost:27017')
    return client

client = get_database_connection()
db = client['kdrt_news']
collection = db['beritaKDRT']

# Function to load data from MongoDB
@st.cache_data(ttl=3600)  # Cache for 1 hour
def load_data():
    try:
        st.write("Attempting to connect to MongoDB...")
        # Get collection statistics to verify connection
        stats = db.command("collstats", "kdrt")
        st.write(f"Connected to collection. Document count: {stats.get('count', 0)}")
        
        data = list(collection.find({}))
        st.write(f"Retrieved {len(data)} documents from MongoDB")
        
        if not data:
            st.warning("No data found in the collection. Database might be empty.")
            return pd.DataFrame()
            
        # Convert ObjectId to string for DataFrame compatibility
        for item in data:
            item['_id'] = str(item['_id'])
        
        df = pd.DataFrame(data)
        st.write(f"Created DataFrame with {len(df)} rows and {len(df.columns)} columns")
        st.write(f"DataFrame columns: {df.columns.tolist()}")
        
        # Check if required columns exist
        if 'tanggal' not in df.columns:
            st.warning("'tanggal' column not found in data. Check your MongoDB collection schema.")
            return df
            
        # Filter out any rows where 'tanggal' is None
        df_filtered = df[df['tanggal'].notna()]
        st.write(f"After filtering null dates: {len(df_filtered)} rows remain")
        
        return df_filtered
        
    except Exception as e:
        st.error(f"Error loading data from MongoDB: {str(e)}")
        import traceback
        st.code(traceback.format_exc())
        return pd.DataFrame()

# Indonesian stopwords
indo_stopwords = set(stopwords.words('indonesian'))
# Add more custom stopwords relevant to news articles
custom_stopwords = {
    'detik', 'com', 'detikcom', 'advertisement', 'scroll', 
    'content', 'jakarta', 'resume', 'selengkapnya', 'baca', 
    'hari', 'ini', 'juga', 'dari', 'yang', 'dengan', 'dan',
    'ini', 'itu', 'atau', 'pada', 'untuk', 'dalam', 'oleh',
    'ke', 'di', 'an', 'kan', 'nya', 'lah', 'kah', 'pun'
}
indo_stopwords.update(custom_stopwords)

# Helper functions for text analysis
def clean_text(text):
    if not isinstance(text, str):
        return ""
    # Remove special characters and digits
    text = re.sub(r'[^\w\s]', '', text)
    text = re.sub(r'\d+', '', text)
    # Convert to lowercase
    text = text.lower()
    # Remove stopwords
    words = text.split()
    words = [word for word in words if word not in indo_stopwords and len(word) > 2]
    return ' '.join(words)

def get_sentiment(text):
    if not text:
        return 0
    analysis = TextBlob(text)
    # Normalize between -1 and 1
    return analysis.sentiment.polarity

def extract_location(text):
    # Simplified location extraction - looking for common Indonesian cities
    common_cities = ['jakarta', 'surabaya', 'bandung', 'medan', 'makassar', 
                     'semarang', 'palembang', 'tangerang', 'depok', 'bogor']
    
    if not isinstance(text, str):
        return "Unknown"
    
    text_lower = text.lower()
    for city in common_cities:
        if city in text_lower:
            return city.capitalize()
    
    return "Unknown"

# Main application
def main():
    st.title("📰 KDRT News Analysis Dashboard")
    
    # Load data
    with st.spinner("Loading data from MongoDB..."):
        df = load_data()
    
    if df.empty:
        st.error("No data found in the database. Please run the scraper first.")
        
        # Add debugging options
        with st.expander("Debugging Options"):
            st.subheader("MongoDB Connection Test")
            if st.button("Test MongoDB Connection"):
                try:
                    # Get database information
                    db_names = client.list_database_names()
                    st.success(f"Connected to MongoDB. Available databases: {', '.join(db_names)}")
                    
                    if 'CrawlingScrapping' in db_names:
                        collections = db.list_collection_names()
                        st.success(f"Database 'CrawlingScrapping' found. Collections: {', '.join(collections)}")
                        
                        if 'kdrt' in collections:
                            count = collection.count_documents({})
                            st.info(f"Collection 'kdrt' has {count} documents")
                            
                            if count > 0:
                                sample = collection.find_one()
                                st.subheader("Sample document structure:")
                                st.json({k: str(v) for k, v in sample.items()})
                        else:
                            st.warning("Collection 'kdrt' not found. Check your collection name.")
                    else:
                        st.warning("Database 'CrawlingScrapping' not found. Check your database name.")
                        
                except Exception as e:
                    st.error(f"MongoDB connection error: {str(e)}")
            
            st.subheader("MongoDB Connection Configuration")
            mongo_uri = st.text_input("MongoDB URI", value="mongodb://localhost:27017")
            db_name = st.text_input("Database Name", value="CrawlingScrapping")
            coll_name = st.text_input("Collection Name", value="kdrt") 
            
            if st.button("Connect with Custom Settings"):
                try:
                    # Try connecting with custom settings
                    custom_client = MongoClient(mongo_uri)
                    custom_db = custom_client[db_name]
                    custom_collection = custom_db[coll_name]
                    
                    # Try getting a document count
                    count = custom_collection.count_documents({})
                    st.success(f"Successfully connected! Found {count} documents.")
                    
                    if count > 0:
                        sample = custom_collection.find_one()
                        st.subheader("Sample document structure:")
                        st.json({k: str(v) for k, v in sample.items()})
                        
                        # Option to use these settings
                        if st.button("Use These Settings"):
                            # Reset cache and update global variables
                            st.cache_data.clear()
                            st.cache_resource.clear()
                            # Note: This will require a refresh to take effect
                            st.rerun()
                except Exception as e:
                    st.error(f"Error with custom connection: {str(e)}")
        
        return
    
    # Display basic statistics
    st.write(f"Total articles: {len(df)}")
    
    # Convert 'tanggal' column to datetime if not already
    if 'tanggal' in df.columns and not pd.api.types.is_datetime64_any_dtype(df['tanggal']):
        df['tanggal'] = pd.to_datetime(df['tanggal'], errors='coerce')
    
    # Add clean text and sentiment columns
    df['clean_text'] = df['isi'].apply(clean_text)
    df['sentiment'] = df['clean_text'].apply(get_sentiment)
    df['location'] = df['isi'].apply(extract_location)
    
    # Extract year and month for time-based analysis
    if 'tanggal' in df.columns:
        df['year'] = df['tanggal'].dt.year
        df['month'] = df['tanggal'].dt.month
        df['month_name'] = df['tanggal'].dt.strftime('%B')
        df['date'] = df['tanggal'].dt.date
    
    # Sidebar filters
    st.sidebar.header("Filters")
    
    # Date range filter
    if 'tanggal' in df.columns:
        min_date = df['tanggal'].min().date()
        max_date = df['tanggal'].max().date()
        
        date_range = st.sidebar.date_input(
            "Select Date Range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date
        )
        
        if len(date_range) == 2:
            start_date, end_date = date_range
            filtered_df = df[(df['tanggal'].dt.date >= start_date) & 
                             (df['tanggal'].dt.date <= end_date)]
        else:
            filtered_df = df
    else:
        filtered_df = df
    
    # Location filter if available
    if 'location' in filtered_df.columns:
        locations = ['All'] + sorted(filtered_df['location'].unique().tolist())
        selected_location = st.sidebar.selectbox("Select Location", locations)
        
        if selected_location != 'All':
            filtered_df = filtered_df[filtered_df['location'] == selected_location]
    
    # Main dashboard content
    tab1, tab2, tab3, tab4 = st.tabs(["Overview", "Content Analysis", "Temporal Analysis", "Raw Data"])
    
    # Tab 1: Overview
    with tab1:
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Articles Over Time")
            if 'date' in filtered_df.columns:
                # Count articles by date
                articles_by_date = filtered_df.groupby('date').size().reset_index(name='count')
                
                # Create time series chart
                fig = px.line(articles_by_date, x='date', y='count', 
                            title='Number of KDRT Articles Published Over Time')
                fig.update_layout(xaxis_title='Date', yaxis_title='Number of Articles')
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.write("Date information not available")
        
        with col2:
            st.subheader("Sentiment Distribution")
            if 'sentiment' in filtered_df.columns:
                # Create sentiment distribution
                fig = px.histogram(filtered_df, x='sentiment', nbins=20,
                                title='Sentiment Distribution in KDRT Articles')
                fig.update_layout(xaxis_title='Sentiment Score', yaxis_title='Count')
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.write("Sentiment analysis not available")
        
        # Location distribution
        if 'location' in filtered_df.columns and 'Unknown' not in filtered_df['location'].unique():
            st.subheader("Geographic Distribution")
            location_counts = filtered_df['location'].value_counts().reset_index()
            location_counts.columns = ['Location', 'Count']
            
            fig = px.bar(location_counts, x='Location', y='Count',
                        title='KDRT Articles by Location')
            st.plotly_chart(fig, use_container_width=True)
    
    # Tab 2: Content Analysis
    with tab2:
        st.subheader("Word Cloud")
        
        if 'clean_text' in filtered_df.columns:
            # Combine all text for word cloud
            all_text = ' '.join(filtered_df['clean_text'].dropna().tolist())
            
            if all_text:
                # Generate word cloud
                wordcloud = WordCloud(
                    width=800, height=400,
                    background_color='white',
                    max_words=100,
                    contour_width=3,
                    contour_color='steelblue'
                ).generate(all_text)
                
                # Display word cloud
                plt.figure(figsize=(10, 5))
                plt.imshow(wordcloud, interpolation='bilinear')
                plt.axis('off')
                st.pyplot(plt)
            else:
                st.write("Not enough text data available for word cloud generation")
            
            # Common keywords
            st.subheader("Common Keywords")
            words = all_text.split()
            word_counts = Counter(words).most_common(20)
            
            if word_counts:
                keywords_df = pd.DataFrame(word_counts, columns=['Word', 'Count'])
                fig = px.bar(keywords_df, x='Word', y='Count',
                            title='Most Common Keywords in KDRT Articles')
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.write("Text analysis not available")
        
        # Sentiment analysis over time
        if 'sentiment' in filtered_df.columns and 'tanggal' in filtered_df.columns:
            st.subheader("Sentiment Trends Over Time")
            
            # Group by month and calculate average sentiment
            sentiment_by_month = filtered_df.groupby(['year', 'month', 'month_name'])['sentiment'].mean().reset_index()
            sentiment_by_month['year_month'] = sentiment_by_month['year'].astype(str) + '-' + sentiment_by_month['month'].astype(str)
            sentiment_by_month = sentiment_by_month.sort_values(['year', 'month'])
            
            fig = px.line(sentiment_by_month, x='year_month', y='sentiment',
                        title='Average Sentiment Score by Month',
                        labels={'year_month': 'Year-Month', 'sentiment': 'Average Sentiment'})
            st.plotly_chart(fig, use_container_width=True)
    
    # Tab 3: Temporal Analysis
    with tab3:
        if 'tanggal' in filtered_df.columns:
            st.subheader("Monthly Article Distribution")
            
            # Group by month and year
            monthly_counts = filtered_df.groupby(['year', 'month_name']).size().reset_index(name='count')
            
            # Create heatmap
            pivot_table = monthly_counts.pivot_table(index='month_name', columns='year', values='count', aggfunc='sum', fill_value=0)
            
            # Ensure month order is correct
            month_order = ['January', 'February', 'March', 'April', 'May', 'June', 
                        'July', 'August', 'September', 'October', 'November', 'December']
            pivot_table = pivot_table.reindex(month_order)
            
            fig = px.imshow(pivot_table,
                            labels=dict(x="Year", y="Month", color="Number of Articles"),
                            x=pivot_table.columns,
                            y=pivot_table.index,
                            aspect="auto",
                            title="Monthly Distribution of KDRT Articles")
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Daily distribution
            st.subheader("Day of Week Analysis")
            
            if 'tanggal' in filtered_df.columns:
                filtered_df['day_of_week'] = filtered_df['tanggal'].dt.day_name()
                
                # Order days of week correctly
                day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
                
                # Count by day of week
                day_counts = filtered_df['day_of_week'].value_counts().reindex(day_order).reset_index()
                day_counts.columns = ['Day', 'Count']
                
                fig = px.bar(day_counts, x='Day', y='Count',
                            title='KDRT Articles by Day of Week')
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.write("Temporal analysis not available without date information")
    
    # Tab 4: Raw Data
    with tab4:
        st.subheader("Raw Data Sample")
        
        # Display columns selector
        default_columns = ['judul', 'tanggal', 'link']
        if 'sentiment' in filtered_df.columns:
            default_columns.append('sentiment')
        
        selected_columns = st.multiselect(
            "Select columns to display",
            options=filtered_df.columns.tolist(),
            default=default_columns
        )
        
        if selected_columns:
            st.dataframe(filtered_df[selected_columns], use_container_width=True)
        else:
            st.dataframe(filtered_df, use_container_width=True)
        
        # Export option
        if st.button("Export Data to CSV"):
            csv = filtered_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="Download CSV",
                data=csv,
                file_name="kdrt_news_data.csv",
                mime="text/csv",
            )

if __name__ == "__main__":
    main()