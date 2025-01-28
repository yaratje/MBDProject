import geopandas as gpd

# Load the shapefile
shapefile_path = "./zoneShape/taxi_zones.shp"
gdf = gpd.read_file(shapefile_path)
gdf = gdf.to_crs(epsg=4326)
print(gdf.crs)

# Extract centroids for polygons or use existing points
gdf["Longitude"] = gdf.geometry.centroid.x
gdf["Latitude"] = gdf.geometry.centroid.y

# Save as a CSV to use in Tableau
output_path = "Link/output_with_lat_lon.csv"
gdf[["LocationID", "Longitude", "Latitude"]].to_csv(output_path, index=False)
