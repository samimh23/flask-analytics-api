from flask import Flask, jsonify, request
from pymongo import MongoClient
from bson import ObjectId
from datetime import datetime
import pandas as pd
import numpy as np
import logging
import os
from flask_cors import CORS
from dotenv import load_dotenv
import sys

# Load environment variables
load_dotenv()

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Configure logging
if __name__ != '__main__':
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
else:
    logging.basicConfig(level=logging.INFO)

# MongoDB connection
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("DB_NAME", "analytics_db")
client = MongoClient(MONGO_URI)
db = client[DB_NAME]

print(f"Python version: {sys.version}")
def load_data(market_id=None):
    """Load data from MongoDB and convert to DataFrame"""
    try:
        query = {}
        if market_id:
            try:
                query["marketId"] = ObjectId(market_id)
            except:
                return pd.DataFrame(), "Invalid market ID format"

        # Projection to include only needed fields
        projection = {
            "userId": 1,
            "customerAge": 1,
            "customerGender": 1,
            "purchaseAmount": 1,
            "location": 1,
            "season": 1,
            "categories": 1,
            "orderDate": 1,
            "_id": 0
        }

        cursor = db.analytics.find(query, projection)
        df = pd.DataFrame(list(cursor))

        # Convert data types
        if not df.empty:
            if 'orderDate' in df:
                df['orderDate'] = pd.to_datetime(df['orderDate'])
            if 'purchaseAmount' in df:
                df['purchaseAmount'] = pd.to_numeric(df['purchaseAmount'])
            if 'customerAge' in df:
                df['customerAge'] = pd.to_numeric(df['customerAge'])

        return df, None
    except Exception as e:
        app.logger.error(f"Error loading data: {str(e)}")
        return pd.DataFrame(), str(e)


@app.route('/api/stats', methods=['GET'])
def get_stats():
    """Get basic statistics"""
    try:
        market_id = request.args.get('market_id', None)
        df, error = load_data(market_id)

        if df.empty:
            return jsonify({"error": error or "No data found"}), 404

        stats = {
            'total_sales': float(df['purchaseAmount'].sum()),
            'avg_purchase': float(df['purchaseAmount'].mean()),
            'total_orders': int(len(df)),
            'unique_customers': int(df['userId'].nunique()),
            'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'market_id': market_id or 'all'
        }

        return jsonify({'stats': stats})
    except Exception as e:
        app.logger.error(f"Error in stats endpoint: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/categories', methods=['GET'])
def get_categories():
    """Get category analytics"""
    try:
        market_id = request.args.get('market_id', None)
        df, error = load_data(market_id)

        if df.empty:
            return jsonify({"error": error or "No data found"}), 404

        # Explode categories array into rows
        if 'categories' in df:
            category_data = []
            for _, row in df.iterrows():
                if isinstance(row.get('categories'), list):
                    for cat in row['categories']:
                        if isinstance(cat, dict):
                            category_data.append({
                                'category': cat.get('category', 'Unknown'),
                                'amount': float(cat.get('amount', 0))
                            })

            if category_data:
                category_df = pd.DataFrame(category_data)
                category_counts = category_df['category'].value_counts().to_dict()
                category_sales = category_df.groupby('category')['amount'].sum().to_dict()
                avg_by_category = category_df.groupby('category')['amount'].mean().to_dict()
            else:
                category_counts = category_sales = avg_by_category = {}
        else:
            category_counts = category_sales = avg_by_category = {}

        return jsonify({
            'distribution': category_counts,
            'sales': category_sales,
            'average_purchase': avg_by_category,
            'market_id': market_id or 'all'
        })
    except Exception as e:
        app.logger.error(f"Error in categories endpoint: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/locations', methods=['GET'])
def get_locations():
    """Get location analytics"""
    try:
        market_id = request.args.get('market_id', None)
        df, error = load_data(market_id)

        if df.empty:
            return jsonify({"error": error or "No data found"}), 404

        if 'location' in df:
            location_counts = df['location'].value_counts().to_dict()
            location_sales = df.groupby('location')['purchaseAmount'].sum().to_dict()

            # Top category per location
            top_categories = {}
            for location in df['location'].unique():
                location_df = df[df['location'] == location]
                if not location_df.empty and 'categories' in location_df:
                    categories = []
                    for _, row in location_df.iterrows():
                        if isinstance(row.get('categories'), list):
                            for cat in row['categories']:
                                if isinstance(cat, dict):
                                    categories.append(cat.get('category', 'Unknown'))
                    if categories:
                        top_cat = pd.Series(categories).value_counts().index[0]
                        top_categories[location] = top_cat
        else:
            location_counts = location_sales = top_categories = {}

        return jsonify({
            'distribution': location_counts,
            'sales': location_sales,
            'top_categories': top_categories,
            'market_id': market_id or 'all'
        })
    except Exception as e:
        app.logger.error(f"Error in locations endpoint: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/time_analysis', methods=['GET'])
def get_time_analysis():
    """Get time-based analytics"""
    try:
        market_id = request.args.get('market_id', None)
        df, error = load_data(market_id)

        if df.empty:
            return jsonify({"error": error or "No data found"}), 404

        # Season sales
        season_sales = {}
        if 'season' in df:
            season_sales = df.groupby('season')['purchaseAmount'].sum().to_dict()

        # Daily sales
        daily_sales = {}
        if 'orderDate' in df and not df['orderDate'].isnull().all():
            daily_sales = df.groupby(df['orderDate'].dt.date.astype(str))['purchaseAmount'].sum().to_dict()

        return jsonify({
            'season_sales': season_sales,
            'daily_sales': daily_sales,
            'market_id': market_id or 'all'
        })
    except Exception as e:
        app.logger.error(f"Error in time_analysis endpoint: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/demographics', methods=['GET'])
def get_demographics():
    """Get demographic analytics"""
    try:
        market_id = request.args.get('market_id', None)
        df, error = load_data(market_id)

        if df.empty:
            return jsonify({"error": error or "No data found"}), 404

        # Gender analytics
        gender_counts = gender_sales = {}
        if 'customerGender' in df:
            gender_counts = df['customerGender'].value_counts().to_dict()
            gender_sales = df.groupby('customerGender')['purchaseAmount'].sum().to_dict()

        # Age analytics
        age_stats = {}
        if 'customerAge' in df:
            df['customerAge'] = pd.to_numeric(df['customerAge'], errors='coerce')
            if not df['customerAge'].isnull().all():
                age_stats = {
                    'average': float(df['customerAge'].mean()),
                    'min': float(df['customerAge'].min()),
                    'max': float(df['customerAge'].max())
                }

                # Age groups
                bins = [0, 25, 35, 45, 55, 100]
                labels = ['<25', '25-34', '35-44', '45-54', '55+']
                df['Age Group'] = pd.cut(df['customerAge'], bins=bins, labels=labels, right=False)
                age_group_counts = df['Age Group'].value_counts().to_dict()
                age_stats['groups'] = age_group_counts

        return jsonify({
            'gender_distribution': gender_counts,
            'gender_sales': gender_sales,
            'age_stats': age_stats,
            'market_id': market_id or 'all'
        })
    except Exception as e:
        app.logger.error(f"Error in demographics endpoint: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/charts/category_sales', methods=['GET'])
def get_category_sales_chart():
    """Get category sales data for charts"""
    try:
        market_id = request.args.get('market_id', None)
        df, error = load_data(market_id)

        if df.empty:
            return jsonify({"error": error or "No data found"}), 404

        category_sales = []
        if 'categories' in df:
            category_data = []
            for _, row in df.iterrows():
                if isinstance(row.get('categories'), list):
                    for cat in row['categories']:
                        if isinstance(cat, dict):
                            category_data.append({
                                'category': cat.get('category', 'Unknown'),
                                'amount': float(cat.get('amount', 0))
                            })

            if category_data:
                category_df = pd.DataFrame(category_data)
                category_sales = category_df.groupby('category')['amount'].sum().reset_index()
                category_sales = category_sales.sort_values('amount', ascending=False)

        chart_data = {
            'labels': category_sales['category'].tolist() if not category_sales.empty else [],
            'values': category_sales['amount'].tolist() if not category_sales.empty else [],
            'title': 'Sales by Category',
            'market_id': market_id or 'all'
        }

        return jsonify(chart_data)
    except Exception as e:
        app.logger.error(f"Error generating category sales data: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/charts/location_sales', methods=['GET'])
def get_location_sales_chart():
    """Get location sales data for charts"""
    try:
        market_id = request.args.get('market_id', None)
        df, error = load_data(market_id)

        if df.empty:
            return jsonify({"error": error or "No data found"}), 404

        if 'location' in df:
            location_sales = df.groupby('location')['purchaseAmount'].sum().reset_index()
            location_sales = location_sales.sort_values('purchaseAmount', ascending=False)
        else:
            location_sales = pd.DataFrame()

        chart_data = {
            'labels': location_sales['location'].tolist() if not location_sales.empty else [],
            'values': location_sales['purchaseAmount'].tolist() if not location_sales.empty else [],
            'title': 'Sales by Location',
            'market_id': market_id or 'all'
        }

        return jsonify(chart_data)
    except Exception as e:
        app.logger.error(f"Error generating location sales data: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/charts/gender_distribution', methods=['GET'])
def get_gender_distribution_chart():
    """Get gender distribution data for charts"""
    try:
        market_id = request.args.get('market_id', None)
        df, error = load_data(market_id)

        if df.empty:
            return jsonify({"error": error or "No data found"}), 404

        if 'customerGender' in df:
            gender_counts = df['customerGender'].value_counts().reset_index()
        else:
            gender_counts = pd.DataFrame(columns=['customerGender', 'count'])

        chart_data = {
            'labels': gender_counts['customerGender'].tolist() if not gender_counts.empty else [],
            'values': gender_counts['count'].tolist() if not gender_counts.empty else [],
            'title': 'Gender Distribution',
            'market_id': market_id or 'all'
        }

        return jsonify(chart_data)
    except Exception as e:
        app.logger.error(f"Error generating gender distribution data: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/charts/seasonal_sales', methods=['GET'])
def get_seasonal_sales_chart():
    """Get seasonal sales data for charts"""
    try:
        market_id = request.args.get('market_id', None)
        df, error = load_data(market_id)

        if df.empty:
            return jsonify({"error": error or "No data found"}), 404

        if 'season' in df:
            seasons_order = ['Winter', 'Spring', 'Summer', 'Fall']
            season_sales = df.groupby('season')['purchaseAmount'].sum().reset_index()
            season_sales['season'] = pd.Categorical(season_sales['season'], categories=seasons_order, ordered=True)
            season_sales = season_sales.sort_values('season')
        else:
            season_sales = pd.DataFrame()

        chart_data = {
            'labels': season_sales['season'].tolist() if not season_sales.empty else [],
            'values': season_sales['purchaseAmount'].tolist() if not season_sales.empty else [],
            'title': 'Sales by Season',
            'market_id': market_id or 'all'
        }

        return jsonify(chart_data)
    except Exception as e:
        app.logger.error(f"Error generating seasonal sales data: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/available_markets', methods=['GET'])
def get_available_markets():
    """Get list of available markets"""
    try:
        market_ids = db.analytics.distinct("marketId")
        markets = [str(mid) for mid in market_ids]

        return jsonify({
            'available_markets': markets,
            'count': len(markets)
        })
    except Exception as e:
        app.logger.error(f"Error listing available markets: {str(e)}")
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    app.logger.info('Starting Flask API server for analytics')
    app.run(debug=True, host='0.0.0.0', port=5000)