import statistics
from collections import defaultdict
import pandas as pd
import numpy as np

users = pd.read_json('data/raw_v2/users.jsonl', lines=True)
products = pd.read_json('data/raw_v2/products.jsonl', lines=True)
sessions = pd.read_json('data/raw_v2/sessions.jsonl', lines=True)
deliveries = pd.read_json('data/raw_v2/deliveries.jsonl', lines=True)

wrong_price_products = list(products[products["price"] < 0]["product_id"].values) + list(products[products["price"] > 10000]["product_id"].values)

# products = products.drop(products[products["price"] < 0].index)
# products = products.drop(products[products["price"] > 10000].index)
# sessions = sessions.drop(sessions[sessions["user_id"].isnull()].index)
# sessions = sessions.drop(sessions[sessions["product_id"].isnull()].index)

# for idx in wrong_price_products:
#     sessions = sessions.drop(sessions[sessions["product_id"] == idx].index)
# sessions = sessions.astype({"user_id": np.int64, "product_id": np.int64})


def analyze_users():
    errors = 0
    if not users["user_id"].is_unique:
        print("User ids are not unique")
        errors += 1

    if users["user_id"].isnull().values.any():
        print("Some user ids are null")
        errors += 1

    if not users["user_id"].dtypes == np.int64:
        print("Some user ids are not int")
        errors += 1

    if users["name"].isnull().values.any() or not users[users["name"] == ""].empty:
        print("Some user names are null")
        print("User ids:", users[users["name"] == ""].user_id.values)
        errors += 1

    if users["city"].isnull().values.any() or not users[users["city"] == ""].empty:
        print("Some user cities are null")
        print("User ids:", users[users["city"] == ""].user_id.values)
        errors += 1

    if users["street"].isnull().values.any() or not users[users["street"] == ""].empty:
        print("Some user streets are null")
        print("User ids:", users[users["street"] == ""].user_id.values)
        errors += 1

    if errors == 0:
        print("Users are ok")


def analyze_products():
    errors = 0
    if not products["product_id"].is_unique:
        print("Product ids are not unique")
        errors += 1

    if products["product_id"].isnull().values.any():
        print("Some product ids are null")
        errors += 1

    if not products["product_id"].dtypes == np.int64:
        print("Some product ids are not int")
        errors += 1

    if products["product_name"].isnull().values.any() or not products[products["product_name"] == ""].empty:
        print("Some product names are null")
        print("Product ids:", products[products["product_name"] == ""].product_id.values)
        errors += 1

    if products["category_path"].isnull().values.any() or not products[products["category_path"] == ""].empty:
        print("Some product categories are null")
        print("Product ids:", products[products["category_path"] == ""].product_id.values)
        errors += 1

    if products["price"].isnull().values.any() \
            or not products[products["price"] < 0].empty \
            or not products[products["price"] > 10000].empty:
        print("Some product prices are null or wrong")
        errors += 1

    if products["rating"].isnull().values.any() \
            or not products[products["rating"] < 0].empty \
            or not products[products["rating"] > 5].empty:
        print("Some product ratings are null or wrong")
        errors += 1

    if errors == 0:
        print("Products are ok")

    mean_price = statistics.mean(list(products["price"].values))
    mean_rating = statistics.mean(list(products["rating"].values))
    print("Mean product price:", mean_price)
    print("Mean product rating:", mean_rating)


def analyze_sessions():
    errors = 0
    if sessions["session_id"].isnull().values.any():
        print("Some session ids are null")
        errors += 1

    if not sessions["session_id"].dtypes == np.int64:
        print("Some session ids are not int")
        errors += 1

    if sessions["user_id"].isnull().values.any():
        print("Some session user ids are null")
        errors += 1

    if not sessions["user_id"].dtypes == np.int64:
        print("Some session user ids are not int")
        errors += 1

    if sessions["product_id"].isnull().values.any():
        print("Some session product ids are null")
        errors += 1

    if not sessions["product_id"].dtypes == np.int64:
        print("Some session product ids are not int")
        errors += 1

    if sessions["offered_discount"].isnull().values.any() or not sessions[sessions["offered_discount"] < 0].empty:
        print("Some session discounts are null or less than 0")
        errors += 1

    if not sessions["offered_discount"].dtypes == np.int64:
        print("Some session discounts are not int")
        errors += 1

    user_ids = users["user_id"].unique()
    product_ids = products["product_id"].unique()
    session_user_ids = sessions["user_id"].unique()
    session_product_ids = sessions["product_id"].unique()

    if not set(session_user_ids).issubset(user_ids):
        print("Session contains non existent user")
        errors += 1
    if not set(session_product_ids).issubset(product_ids):
        print("Session contains non existent product")
        errors += 1

    if errors == 0:
        print("Sessions are ok")

    sessions_num = sessions["session_id"].nunique()
    print("Number of sessions: ", sessions_num)

    purchases_num = sessions["purchase_id"].count()
    print("Number of purchases: ", purchases_num)

    purchases_ratio = purchases_num / sessions_num
    print("Ratio of purchases: ", purchases_ratio)

    discounts = np.sort(sessions["offered_discount"].unique())
    print("Discounts: ", discounts)

    session_ids = sessions["session_id"].unique()
    session_lengths = []
    for session_id in session_ids:
        session = sessions[sessions.session_id == session_id]
        length = session.timestamp.max() - session.timestamp.min()
        session_lengths.append(length.seconds)

    session_lengths_mean = round(statistics.mean(session_lengths), 2)
    print("Mean session length in seconds: ", session_lengths_mean)

    # calculate_average_daily_session_length()
    calculate_average_daily_session_number()


def analyze_deliveries():
    delivery_lengths = (pd.to_datetime(deliveries.delivery_timestamp) - pd.to_datetime(deliveries.purchase_timestamp)).dt.total_seconds()

    print("Longest delivery ", max(delivery_lengths))
    print("Shortest delivery ", min(delivery_lengths))
    print("Average delivery ", statistics.mean(delivery_lengths))


def calculate_average_daily_session_length():
    session_unique_dates = sessions["timestamp"].map(pd.Timestamp.date).unique()
    average_session_length_by_day = defaultdict(float)
    for date in session_unique_dates:
        session = sessions[sessions.timestamp.map(pd.Timestamp.date) == date]
        day_session_ids = session["session_id"].unique()
        day_lengths = []
        for session_id in day_session_ids:
            day_session = session[session.session_id == session_id]
            length = day_session.timestamp.max() - day_session.timestamp.min()
            day_lengths.append(length.seconds)
        day_mean = statistics.mean(day_lengths)
        average_session_length_by_day[date] = day_mean
    print("Average daily session length: ", statistics.mean(average_session_length_by_day.values()))


def calculate_average_daily_session_number():
    session_unique_dates = sessions["timestamp"].map(pd.Timestamp.date).unique()
    session_num_by_day = defaultdict(float)
    purchase_num_by_day = defaultdict(float)
    for date in session_unique_dates:
        session = sessions[sessions.timestamp.map(pd.Timestamp.date) == date]
        session_num = len(session["session_id"].unique())
        purchase_num = session["purchase_id"].count()
        session_num_by_day[date] = session_num
        purchase_num_by_day[date] = purchase_num
    session_num_mean = statistics.mean(session_num_by_day.values())
    purchase_num_mean = statistics.mean(purchase_num_by_day.values())
    print("Average sessions per day: ", session_num_mean)
    print("Average purchases per day: ", purchase_num_mean)
    print("Average daily purchases ratio: ", purchase_num_mean / session_num_mean)


def check(row):
    if pd.isna(row["purchase_id"]):
        print("Hello")
    else:
        print("No")


def sort_sessions_by_date():
    sessions_sorted = sessions.sort_values(["user_id", "timestamp", "session_id"])
    print(sessions_sorted.dtypes)
    sessions_sorted.apply(lambda row: check(row), axis=1)
    print(sessions_sorted.dtypes)
    print(sessions_sorted)
    sessions_sorted.to_json("data/test.jsonl", date_format="iso", date_unit="s", orient='records', lines=True)


analyze_users()
analyze_products()
analyze_sessions()
analyze_deliveries()
# sort_sessions_by_date()

# print(products[products["price"] < 0].count())
# print(products[products["price"] > 10000].count())
# print(set(sessions["user_id"].unique()).issubset(users["user_id"].unique()))
# print(set(sessions["product_id"].unique()).issubset(products["product_id"].unique()))
# print(sessions["product_id"].unique())
# print(products["product_id"].unique())
