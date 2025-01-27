import pandas as pd
from fuzzywuzzy import process
import glob

# Define file paths
demographic_file = 'demographicData/Total Population.csv'
trip_data_base_path = 'Trips_per_PULocationID/{year}/part-00000-*.csv'

# Load data
demographic_data = pd.read_csv(demographic_file)

# Step 1: Clean column names and data
demographic_data['Location'] = demographic_data['Location'].str.strip().str.lower()
demographic_data['TimeFrame'] = demographic_data['TimeFrame'].astype(int)

# Step 2: Create a function for fuzzy matching
def match_regions(source_list, target_list):
    """
    Matches regions from the target list to the closest match in the source list using fuzzy matching.
    Returns a dictionary mapping target regions to the best match in the source list.
    """
    matches = {}
    for region in target_list:
        if type(region) is str:

            match, score = process.extractOne(region, source_list)
            matches[region] = {'match': match, 'score': score}
    return matches

# Step 3: Process data for each year
final_data_frames = []

for year in range(2019, 2025):
    trip_data_files = glob.glob(trip_data_base_path.format(year=year))
    if not trip_data_files:
        print(f"No trip data file found for year {year}. Skipping.")
        continue

    # Use the first matching file for the year
    trip_data_file = trip_data_files[0]
    trip_data = pd.read_csv(trip_data_file)

    # Clean trip data
    trip_data['Zone'] = trip_data['Zone'].str.strip().str.lower()

    # Perform matching
    demographic_regions = demographic_data['Location'].unique()
    trip_regions = trip_data['Zone'].unique()

    matches = match_regions(demographic_regions, trip_regions)

    # Create a DataFrame to review the matches
    match_results = pd.DataFrame.from_dict(matches, orient='index')
    match_results.reset_index(inplace=True)
    match_results.columns = ['trip_zone', 'matched_demographic_region', 'match_score']

    # Filter matches with low scores (threshold can be adjusted)
    threshold = 80  # Match confidence threshold
    high_confidence_matches = match_results[match_results['match_score'] >= threshold]

    # Merge data based on high-confidence matches
    merged_data = trip_data.merge(
        high_confidence_matches,
        left_on='Zone',
        right_on='trip_zone',
        how='left'
    ).merge(
        demographic_data,
        left_on='matched_demographic_region',
        right_on='Location',
        how='left'
    )

    # Filter by year and include only relevant columns
    merged_data = merged_data[merged_data['TimeFrame'] == year]
    merged_data = merged_data[['Zone', 'matched_demographic_region', 'PULocationID', 'TimeFrame', 'Data', 'match_score'] + list(trip_data.columns)]

    final_data_frames.append(merged_data)

# Step 4: Concatenate all yearly data
final_data = pd.concat(final_data_frames, ignore_index=True)

# Step 5: Save the merged data to a CSV file
output_file = 'merged_taxi_population_data.csv'
final_data.to_csv(output_file, index=False)

# Output a summary of the matching process
print("Summary of matching results:")
print(final_data['match_score'].describe())
print(f"Merged data saved to {output_file}")
