import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

df = pd.read_csv("merged_taxi_population_data.csv")
SELECTED_COLUMNS = ["Data", "fhv_trips", "fhvhv_trips", "green_trips", "yellow_trips"]

subset_df = df[SELECTED_COLUMNS].rename(columns={"Data": "Population"})

clean_df = subset_df.dropna()

df_corr = clean_df.corr()

sns.heatmap(df_corr, annot=True, cmap="coolwarm")
plt.tight_layout()
plt.title("Correlation Heatmap")
plt.xticks(rotation=45)
plt.show(figsize = (20,15))