import pandas as pd
from sklearn.metrics import mutual_info_score


def parse_users(df):
    cities = pd.get_dummies(df["city"])
    df = df.drop(["name", "city", "street"], axis=1)

    return pd.concat([df, cities], axis=1)


def parse_products(df):
    encoded = df['category_path'].str.split(';')
    encoded = pd.get_dummies(encoded.apply(pd.Series).stack()).groupby(level=0).sum()
    encoded.columns = encoded.columns.str.replace(' ', '_')
    encoded.columns = ['category_' + str(col) for col in encoded.columns]
    df = pd.concat([df, encoded], axis=1)

    df = df.drop(['category_path'], axis=1, inplace=False)
    return df


def parse_sessions(df):
    df = df.drop(['purchase_id'], axis=1, inplace=False)
    return df


def merge_data(users, products, sessions):
    df = pd.merge(sessions, products, on="product_id", how="inner")
    df = df.sort_values(["user_id", "timestamp", "session_id"])

    session_ids = df["session_id"].unique()

    columns = df.columns.tolist()
    category_columns = [col for col in columns if col.startswith('category_')]

    merged_sessions = []
    for session_id in session_ids:
        session = df[df.session_id == session_id]

        prev_sessions = df[(df["timestamp"] < session["timestamp"].min()) & (df["user_id"].isin(session["user_id"]))]
        prev_session_ids = prev_sessions["session_id"].unique()
        prev_sessions_success = prev_sessions[prev_sessions["event_type"] == "BUY_PRODUCT"]
        prev_success_count = len(prev_sessions_success.index)
        prev_all_count = len(prev_session_ids)
        if prev_all_count == 0:
            prev_success_ratio = 0
        else:
            prev_success_ratio = round(prev_success_count / prev_all_count, 2)

        if len(session[session["event_type"] == "BUY_PRODUCT"]) > 0:
            success = 1
        else:
            success = 0

        session_length = (session.timestamp.max() - session.timestamp.min()).seconds
        discount = session["offered_discount"].unique()[0]
        user_id = session.user_id.unique()[0]
        mean_price = round(session["price"].mean(), 2)
        min_price = round(session["price"].min(), 2)
        max_price = round(session["price"].max(), 2)
        mean_rating = round(session["rating"].mean(), 2)
        min_rating = round(session["rating"].min(), 1)
        max_rating = round(session["rating"].max(), 1)

        summed_categories = session[category_columns].sum().to_dict()

        row = {"session_length": session_length,
               "discount": discount,
               "user_id": user_id,
               "successful": success,
               "mean_price": mean_price,
               "min_price": min_price,
               "max_price": max_price,
               "mean_rating": mean_rating,
               "min_rating": min_rating,
               "max_rating": max_rating,
               "prev_success_ratio": prev_success_ratio
               }
        row.update(summed_categories)
        merged_sessions.append(row)

    merged_sessions_df = pd.DataFrame(merged_sessions)
    merged_sessions_df = pd.merge(merged_sessions_df, users, on='user_id', how='inner')

    return merged_sessions_df


def read_and_parse_data():
    users_file = "data/raw_v2/users.jsonl"
    products_file = "data/raw_v2/products.jsonl"
    sessions_file = "data/raw_v2/sessions.jsonl"

    users_df = pd.read_json(users_file, lines=True)
    products_df = pd.read_json(products_file, lines=True)
    sessions_df = pd.read_json(sessions_file, lines=True)

    users_df = parse_users(users_df)
    products_df = parse_products(products_df)
    sessions_df = parse_sessions(sessions_df)

    ready_data = merge_data(users_df, products_df, sessions_df)
    return ready_data


if __name__ == '__main__':
    data = read_and_parse_data()
    data.to_csv('data/data_with_categories.csv', index=None)

    #korelacja
    data.corr(method="spearman").to_csv("data/correlation_spearman_with_categories.csv")

    #informacja wzajemna(przyklad)
    print(mutual_info_score(data["successful"], data["discount"]))



