# This code generates the .txt file with all the to download links from TLC

taxiTypes = ["fhv", "fhvhv", "green", "yellow"]
years = [f"{y:02d}" for y in range(9, 25)]
months = [f"{m:02d}" for m in range(1, 13)]

# https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
initialPath = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
outputFile = "links.txt"

with open(outputFile, "w") as file:
    for taxiType in taxiTypes:
        catPath = initialPath + taxiType + "_tripdata_20"
        # print(catPath)
        for year in years:
            yearPath = catPath + year + "-"
            # print(yearPath)
            for month in months:
                monthPath = yearPath + month + ".parquet"
                file.write(monthPath + "\n")

print("Saved links to " + outputFile)