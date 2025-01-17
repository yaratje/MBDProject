import pandas as pd
import matplotlib.pyplot as plt

# Read the data from the specified sheet
df = pd.read_excel('./Output/yellow-fhv-fhvhv Dataset 2011-2024.xlsx', sheet_name='Blad4')

# Remove any rows with "Eindtotaal" in the Year column
df = df[df['Year'] != 'Eindtotaal']

# Convert columns to numeric where applicable
df['Year'] = pd.to_numeric(df['Year'])
df['fhv+fhvhv'] = pd.to_numeric(df['fhv+fhvhv'])
df['yellow'] = pd.to_numeric(df['yellow'])

# Plot the data
plt.figure(figsize=(12, 6))
plt.plot(df["Year"], df["fhv+fhvhv"], label="FHV+FHVHV", marker='o', linestyle='-', color='blue')
plt.plot(df["Year"], df["yellow"], label="Yellow", marker='o', linestyle='-', color='orange')

# Adding trendlines
z_fhv = pd.Series(df["fhv+fhvhv"]).rolling(window=4).mean()
z_yellow = pd.Series(df["yellow"]).rolling(window=4).mean()
plt.plot(df["Year"], z_fhv, linestyle="--", color="blue", alpha=0.5, label="Trendline FHV+FHVHV")
plt.plot(df["Year"], z_yellow, linestyle="--", color="orange", alpha=0.5, label="Trendline Yellow")

# Customizing the plot
plt.title("FHV+FHVHV vs Yellow Taxi Usage Over Years")
plt.xlabel("Year")
plt.ylabel("Counts")
plt.legend()
plt.grid(True)

# Display the plot
plt.show()