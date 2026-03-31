import pymongo
import pandas as pd
import math
#using for charts
import altair as alt

# Connect to MongoDB
CWL = "jthomp20"
SNUM = "84340637"
connection_string = f"mongodb://{CWL}:a{SNUM}@localhost:27017/{CWL}"

client = pymongo.MongoClient(connection_string)
db = client[CWL]["songs"]


# 2022 query: use weeks_on_chart
pipeline_2022 = [
    {
        "$match": {
            "year": 2022,
            "metrics.weeks_on_chart": {"$exists": True, "$ne": None},
            "metrics.peak_rank": {"$exists": True, "$ne": None}
        }
    },
    {
        "$project": {
            "_id": 0,
            "track_name": 1,
            "artist_names": 1,
            "peak_rank": "$metrics.peak_rank",
            "weeks_on_chart": "$metrics.weeks_on_chart"
        }
    },
    {
        "$sort": {"weeks_on_chart": -1}
    }
]

# 2023 query: use streams
pipeline_2023 = [
    {
        "$match": {
            "year": 2023,
            "metrics.streams": {"$exists": True, "$ne": None}
        }
    },
    {
        "$project": {
            "_id": 0,
            "track_name": 1,
            "artist_names": 1,
            "streams": "$metrics.streams"
        }
    },
    {
        "$sort": {"streams": -1}
    }
]

# 2024 query: use streams
pipeline_2024 = [
    {
        "$match": {
            "year": 2024,
            "metrics.streams": {"$exists": True, "$ne": None}
        }
    },
    {
        "$project": {
            "_id": 0,
            "track_name": 1,
            "artist_names": 1,
            "streams": "$metrics.streams"
        }
    },
    {
        "$sort": {"streams": -1}
    }
]

# Run queries
data_2022 = list(db.aggregate(pipeline_2022))
data_2023 = list(db.aggregate(pipeline_2023))
data_2024 = list(db.aggregate(pipeline_2024))

df_2022 = pd.DataFrame(data_2022)
df_2023 = pd.DataFrame(data_2023)
df_2024 = pd.DataFrame(data_2024)


# Function to calculate top 10% and top 25% shares
def calculate_share(df, column_name, year):
    df = df.sort_values(column_name, ascending=False).reset_index(drop=True)

    total = df[column_name].sum()
    n = len(df)

    top_10_n = math.ceil(n * 0.10)
    top_25_n = math.ceil(n * 0.25)

    top_10_sum = df.head(top_10_n)[column_name].sum()
    top_25_sum = df.head(top_25_n)[column_name].sum()

    return {
        "year": year,
        "top_10_share": (top_10_sum / total) * 100,
        "top_25_share": (top_25_sum / total) * 100
    }


# Final results table
results = [
    calculate_share(df_2022, "weeks_on_chart", 2022),
    calculate_share(df_2023, "streams", 2023),
    calculate_share(df_2024, "streams", 2024)
]

results_df = pd.DataFrame(results)
print(results_df)


# Extra 2022 comparison table for peak rank and weeks on chart
df_2022["rank_group"] = pd.cut(
    df_2022["peak_rank"],
    bins=[0, 10, 25, 50, 1000],
    labels=["Top 10", "11-25", "26-50", "51+"]
)

comparison_2022 = df_2022.groupby("rank_group", as_index=False)["weeks_on_chart"].mean()
comparison_2022.columns = ["rank_group", "avg_weeks_on_chart"]

print("\n2022 comparison table:")
print(comparison_2022)

# +
df_melt = results_df.melt(
    id_vars='year', 
    value_vars=['top_25_share', 'top_10_share'],
    var_name='Category', 
    value_name='Percentage'
)


chart = alt.Chart(df_melted).mark_bar().encode(
    x=alt.X('year:O', title='Year'),
    y=alt.Y('Percentage:Q', title='Concentration of Streams(%)', stack=None), 
    color=alt.Color('Category:N', 
                    scale=alt.Scale(range=['#ff7f0e', '#1f77b4']),
                    legend=alt.Legend(title="Percentile")),
    size=alt.condition(
        alt.datum.Category == 'top_10_share',
        alt.value(25), 
        alt.value(45)  
    ),
    tooltip=['year', 'Category', 'Percentage']
).properties(
    width=400,
    title='Concentration of Total Streams Over Time (%)'
)

chart.display()
# -

df_melt


