import csv
import json
import requests
import shapely

"""
This program grabs the data from the PUD zones ArcGIS server through a series of queries, then writes them to
a CSV file. A PUD (Planned Unit Development) Zone is a special zoning boundary that is not subject to the 
normal zoning laws in that boundary, and it is created through negotiation between the interested party(s)
and the zoning authority. 

Link to ArcGIS server query page: 
https://services2.arcgis.com/cclAu9OKhOfjeUdr/ArcGIS/rest/services/Chatt_PUD_spermit_1_18_25/FeatureServer/0/query

As of now, all of the data can be retrieved in a single page. Should this change in the future, this program is
designed to query the data in successive pages. 
"""

# List that will contain a dictionary for each row to write to pud_zones.csv
pudZones = []

# Number of records we want to return per page (in case data grows beyond a single page in the future)
PAGE_SIZE = 100

# Used to gather results starting at a specific index, rather than at the very first record
resultOffset = 0

for i in range(1_000_000):

    # Construct the URL for the query, starting with the base, then adding the parameters
    url = "https://services2.arcgis.com/cclAu9OKhOfjeUdr/ArcGIS/rest/services/Chatt_PUD_spermit_1_18_25/FeatureServer/0/query"
    url += "?where=1%3D1"
    url += "&geometryType=esriGeometryEnvelope"
    url += "&spatialRel=esriSpatialRelIntersects"
    url += "&resultType=none"
    url += "&distance=0.0"
    url += "&units=esriSRUnit_Meter"
    url += "&returnGeodetic=false"
    url += "&outFields=*"
    url += "&returnGeometry=true"
    url += "&returnCentroid=false"
    url += "&returnEnvelope=false"
    url += "&featureEncoding=esriDefault"
    url += "&multipatchOption=xyFootprint"
    url += "&outSR=4326"
    url += "&applyVCSProjection=false"
    url += "&returnIdsOnly=false"
    url += "&returnUniqueIdsOnly=false"
    url += "&returnCountOnly=false"
    url += "&returnExtentOnly=false"
    url += "&returnQueryGeometry=false"
    url += "&returnDistinctValues=false"
    url += "&cacheHint=false"

    # Add the result offset if there is one for this query
    if resultOffset > 0:
        url += "&resultOffset="
        url += str(resultOffset)

    url += "&returnZ=false"
    url += "&returnM=false"
    url += "&returnTrueCurves=false"
    url += "&returnExceededLimitFeatures=true"
    url += "&sqlFormat=standard"
    url += "&f=geojson"

    # Issue the request to get the data
    content = requests.get(url=url).content

    # Convert the response data to an easy-to-use Python dictionary
    responseData = json.loads(content)

    # If this is less than PAGE_SIZE after the following "for" loop, then this is the last page of data
    countFeatures = 0

    # If the query returns nothing for features, then there is no more data to access
    if not responseData["features"]:
        break

    # Grab all data fields associated with the feature, including the geometry
    for feature in responseData["features"]:
        # Dictionary containing data for this PUD zone
        pudZone = {}

        pudZone["id"] = feature["id"]

        coordinates = feature["geometry"]["coordinates"]
        # Coordinates may correspond to either a Polygon or a MultiPolygon
        if feature["geometry"]["type"] == "Polygon":
            pudZone["geometry"] = shapely.Polygon(coordinates[0])
        elif feature["geometry"]["type"] == "MultiPolygon":
            pudZone["geometry"] = shapely.MultiPolygon(coordinates)

        pudZone["case_num"] = feature["properties"]["CASE_NUM"]

        # COND and/or ORDINANCE may be empty, represented by a single space; in our case, have it be None
        if feature["properties"]["COND"] == " ":
            pudZone["cond"] = None
        else:
            pudZone["cond"] = feature["properties"]["COND"]

        if feature["properties"]["ORDINANCE"] == " ":
            pudZone["ordinance"] = None
        else:
            pudZone["ordinance"] = feature["properties"]["ORDINANCE"]

        pudZone["shape_area"] = feature["properties"]["Shape__Area"]
        pudZone["shape_length"] = feature["properties"]["Shape__Length"]

        # Add this PUD zone to the CSV list
        pudZones.append(pudZone)

        # Count this record
        countFeatures += 1
    
    # Break the loop once the last page of results is read
    if countFeatures < PAGE_SIZE:
        break

    # Increment the result offset to start at the next page of results
    resultOffset += PAGE_SIZE

print(len(pudZones), "rows retrieved.\n")

# Once all data is gathered, it just needs to be written to the CSV file
with open("pud_zones.csv", "wt", newline="") as csvfile:
    print("Writing to CSV...")
    writer = csv.DictWriter(csvfile, fieldnames=pudZones[0].keys())
    writer.writeheader()
    writer.writerows(pudZones)

print("Success!")