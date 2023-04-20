import pandas as pd
import numpy as np
import requests
import datetime as dt


page = 1

result_list = list()

while True:

    url = f"https://apewisdom.io/api/v1.0/filter/all-stocks/page/{page}"

    response = requests.get(url)

    print(response.json())

    for result in response.json()["results"]:
        result_list.append(result)

    page += 1

    if not response.json()["results"]:
        break

dtypes = np.dtype(
    [
        ("rank", "int32"),
        ("ticker", "object"),
        ("name", "object"),
        ("mentions", "int32"),
        ("upvotes", "int32"),
        ("rank_24h_ago", "int32"),
        ("mentions_24h_ago", "int32"),
    ]
)

df = pd.DataFrame(result_list)


df.loc[df["rank_24h_ago"] == "0", "rank_24h_ago"] = None
df.loc[df["mentions_24h_ago"].isnull(), "mentions_24h_ago"] = "0"

print(df.info())
print(df)

df.astype({"rank": "int32",
        "ticker": "object",
        "name": "object",
        "mentions": "int32",
        "upvotes": "int32",
        "rank_24h_ago": "int32",
        "mentions_24h_ago": "int32"})
