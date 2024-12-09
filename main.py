import sqlite3
import csv
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

def create_database_from_csv(db_file, csv_file):
    try:
        #connect to SQLite database, or create it
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        #refresh database
        cursor.execute("DROP TABLE IF EXISTS Transactions")
        conn.commit()

        #create a new table 
        cursor.execute('''
        CREATE TABLE Transactions (
            Details TEXT,
            PostingDate TEXT,
            Description TEXT,
            Amount REAL,
            Type TEXT,
            Balance REAL,
            CheckOrSlipNumber TEXT
        )
        ''')
        conn.commit()

        #read and insert CSV data 
        with open(csv_file, "r") as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                cursor.execute('''
                INSERT INTO Transactions (Details, PostingDate, Description, Amount, Type, Balance, CheckOrSlipNumber)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    row["Details"],
                    row["Posting Date"],
                    row["Description"],
                    float(row["Amount"]),
                    row["Type"],
                    float(row["Balance"]),
                    row["Check or Slip #"]
                ))
            conn.commit()

        print(f"Database '{db_file}' has been refreshed and data from '{csv_file}' has been imported!")

    except Exception as e:
        print(f"An error occurred: {e}")
    
    finally:
        conn.close()

def load_data_from_database(db_file):
    try:
        conn = sqlite3.connect(db_file)
        data_query = pd.read_sql_query("SELECT * FROM Transactions", conn)
        conn.close()
        print("Loaded raw data from the database.")
        return data_query
    except Exception as e:
        print(f"An error occurred while loading data: {e}")
        return pd.DataFrame()

def categorize_transactions(db_file):

    #define categories 
    categories = {
        "Direct Deposits": ["AMAZON.COM SVCS  DIRECT DEP", "CALLEN LOGISTICS PAYROLL" ], 
        "Food": [
            "McDonald's", "Burger King", "Starbucks", "Pizza", "Subway", "Dunkin",
            "Chipotle", "Panera", "KFC", "Taco Bell", "Popeyes", "Chick-fil-A",
            "Wendy's", "Arby's", "Sonic", "Five Guys", "Domino's", "Papa John's",
            "Olive Garden", "Applebee's", "LongHorn", "Red Lobster", "Cheesecake Factory",
            "IHOP", "Denny's", "Buffalo Wild Wings", "Outback", "Cracker Barrel", "Texas Roadhouse",
            "Playa Bowls", "Cold Stone", "Shake Shack", "Jersey Mike's", "Wawa", "Tim Hortons",
            "Baskin Robbins", "Rita's", "Qdoba", "Boston Market", "Peet's Coffee", "Tropical Smoothie"
        ],
        "Subscriptions": ["Netflix", "Spotify", "Disney", "Hulu", "YouTube", "Amazon Prime"],
        "Gas": ["Gas", "Shell", "Exxon", "BP", "Chevron", "Mobil", "Texaco"],
        "Shopping": [
            "Amazon", "Walmart", "Target", "eBay", "Best Buy", "Costco", "Macy's",
            "Foot Locker", "Urban Outfitters", "Champs", "Adidas", "Nike", "Zara",
            "H&M", "Gap", "Old Navy", "Forever 21", "Nordstrom", "Sephora", "Ulta",
            "Bloomingdale's", "Anthropologie", "Victoria's Secret", "Banana Republic",
            "Lululemon", "Aerie", "American Eagle", "UNIQUE THRIFT STORE"
        ],
        "Tolls": ["EZPass", "E-ZPass", "Toll", "Turnpike", "Expressway", "Bridge"],
        "Other": []  #default
    }

    #generic items food items
    food_keywords = ["bagel", "coffee", "sandwich", "burrito", "smoothie", "salad", "donut", "croissant", "pizza", "burger", "taco"]

    def assign_category(description):
        #check for specific matches first, ie: DIRECT DEPOSITS
        for category, keywords in categories.items():
            if category == "Direct Deposits":
                if description.strip() in keywords:
                    return category

        #partial matches
        for category, keywords in categories.items():
            if any(keyword.lower() in description.lower() for keyword in keywords):
                return category

        #generic food item check
        if any(keyword.lower() in description.lower() for keyword in food_keywords):
            return "Food"

        #default
        return "Other"

    db_file['Category'] = db_file['Description'].apply(assign_category)
    print("Spending categorization completed.")

    return db_file

def save_categorized_data(db_file, categorized_data):
    try:
        conn = sqlite3.connect(db_file)
        categorized_data.to_sql('CategorizedTransactions', conn, if_exists='replace', index=False)
        conn.close()
        print("Categorized data saved to 'CategorizedTransactions' table in the database.")
    except Exception as e:
        print(f"An error occurred while saving categorized data: {e}")

def summarize_spending(categorized_data):
    summary = categorized_data.groupby('Category')['Amount'].sum()

    print("Spending Summary by Category:")
    for category, amount in summary.items():
        print(f"{category}: ${amount:,.2f}")

def plot_spending_by_category(categorized_data):
    try:
        #summarize spending by category
        category_summary = categorized_data.groupby('Category')['Amount'].sum()

        #sort categories
        category_summary = category_summary.sort_values()

        #green for positive money and red for negative money :-)
        colors = category_summary.apply(lambda x: 'green' if x > 0 else 'red')

        #graph stuff
        plt.figure(figsize=(10, 6))
        category_summary.plot(
            kind='bar', 
            color=colors, 
            edgecolor='black'
        )

        plt.title('Total Spending by Category', fontsize=16)
        plt.xlabel('Category', fontsize=14)
        plt.ylabel('Total Amount ($)', fontsize=14)
        plt.xticks(rotation=45, ha='right', fontsize=12)
        plt.grid(axis='y', linestyle='--', alpha=0.7)

        plt.tight_layout()
        plt.show()

    except Exception as e:
        print(f"An error occurred while plotting: {e}")

def plot_net_amount_per_month(categorized_data):
    try:
        categorized_data['PostingDate'] = pd.to_datetime(categorized_data['PostingDate'])
        categorized_data['Month'] = categorized_data['PostingDate'].dt.to_period('M').apply(lambda r: r.start_time)
        
        monthly_net_amount = categorized_data.groupby('Month')['Amount'].sum()
        monthly_net_amount.index = monthly_net_amount.index.to_series().dt.strftime('%B %Y')

        colors = monthly_net_amount.apply(lambda x: 'green' if x > 0 else 'red')

        #graph stuff
        plt.figure(figsize=(12, 6))
        monthly_net_amount.plot(kind='bar', color=colors, edgecolor='black')

        plt.title('Net Amount Per Month', fontsize=16)
        plt.xlabel('Month', fontsize=14)
        plt.ylabel('Net Amount ($)', fontsize=14)
        plt.xticks(rotation=45, ha='right', fontsize=10)
        plt.grid(axis='y', linestyle='--', alpha=0.7)

        plt.tight_layout()
        plt.show()

    except Exception as e:
        print(f"An error occurred while plotting net monthly amount: {e}")

def analyze_trends(categorized_data):
    """
    Analyze trends in net amounts per month.

    Args:
        categorized_data (pd.DataFrame): DataFrame with categorized transactions.

    Returns:
        dict: A dictionary summarizing trends.
    """
    try:
        # Ensure PostingDate is a datetime type
        categorized_data['PostingDate'] = pd.to_datetime(categorized_data['PostingDate'])

        # Group by month and calculate net amount
        categorized_data['Month'] = categorized_data['PostingDate'].dt.to_period('M').apply(lambda r: r.start_time)
        monthly_net_amount = categorized_data.groupby('Month')['Amount'].sum()

        # Calculate monthly change (exact amounts)
        monthly_change = monthly_net_amount.diff()

        # Analyze overall trend (linear regression on months vs. net amounts)
        x = np.arange(len(monthly_net_amount))  # Time index (0, 1, 2, ...)
        y = monthly_net_amount.values
        slope, intercept = np.polyfit(x, y, 1)  # Linear regression

        # Determine overall trend direction
        trend_direction = "increasing" if slope > 0 else "decreasing" if slope < 0 else "stable"

        # Identify months with highest positive and negative changes
        highest_increase = monthly_change.idxmax()
        highest_decrease = monthly_change.idxmin()

        # Format monthly changes as a friendly string
        formatted_changes = {
            month.strftime('%B %Y'): f"${change:,.2f}"
            for month, change in monthly_change.items()
        }

        # Trend summary
        trend_summary = {
            "Overall Trend": trend_direction,
            "Highest Increase": (highest_increase, monthly_change[highest_increase]),
            "Highest Decrease": (highest_decrease, monthly_change[highest_decrease]),
            "Monthly Changes (Exact)": formatted_changes,
        }

        # Print formatted trend analysis
        print("Trend Analysis:")
        print(f"Overall Trend: {trend_summary['Overall Trend']}")
        print(f"Month with Highest Increase: {highest_increase.strftime('%B %Y')} (+${trend_summary['Highest Increase'][1]:,.2f})")
        print(f"Month with Highest Decrease: {highest_decrease.strftime('%B %Y')} (-${abs(trend_summary['Highest Decrease'][1]):,.2f})")
        print("\nMonthly Changes (Exact):")
        for month, change in formatted_changes.items():
            print(f"{month}: {change}")

        return trend_summary

    except Exception as e:
        print(f"An error occurred during trend analysis: {e}")
        return {}

if __name__=="__main__":
    create_database_from_csv("data.db", "chase_bank_statement.csv")
    
    db_file = "data.db"
    transactions_data = load_data_from_database(db_file)

    if transactions_data.empty:
        print("No data found in the database.")
    else:
        categorized_data = categorize_transactions(transactions_data)

        save_categorized_data(db_file, categorized_data)

        print("") #space

        summarize_spending(categorized_data)

        print("") #space

        plot_spending_by_category(categorized_data)

        plot_net_amount_per_month(categorized_data)
    
        analyze_trends(categorized_data)
        
