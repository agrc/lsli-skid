# Lead Service Lines Inventory (LSLI) Skid

This is a simple, local, non-cloud-based skid for on-demand updates of the points and polygons behind the Division of Drinking Water's LSLI map (link to come). This map is the official reporting tool for the material of individual water lines for many smaller culinary water systems across the state. Some systems are certified completely lead-free and are represented only by a polygon of their service area. It represents other systems that have mixed lead and non-lead service lines through individual points for each property.

As of now, the Division of Drinking Water doesn't anticipate frequent updates outside of a yearly update cycle, so we've written this to just be run manually instead of relying on a scheduled cloud process.

The script gathers data from three sources:

1. A DTS-maintained database of records for individual properties. This is extracted via a GraphQL query, converted to a spatial format, and loaded into the points layer to be symbolized by the status of the service line for that property.
1. A Division of Drinking Water Google Sheet that tracks the lead-free status for those systems that apply for a system-wide certification.
1. Another Division of Drinking Water Sheet that contains links to systems that use their own map to display their lead-free status.

The two area-based record sets from the spreadsheets are joined to geometries from the Division of Water Resource's [Culinary Water Service Areas](https://opendata.gis.utah.gov/datasets/dc62a286013f447e88fc45480077c944_0/explore) dataset using the PWSID as a join key. As of initial development, there are over two dozen records from the sheets that don't have a corresponding polygon. Most of these systems are exempt from reporting and thus we don't need to worry about it, but there may be a few that we need to track down.

The script checks for three types of invalid data, drops any corresponding rows, and reports them in the status email:

1. Invalid PWSIDs in the Google Sheets. They must be in the form `Utah1234` (case insensitive, with or without leading zeros on the digits) or just `1234` (again, with or without leading zeros).
1. Duplicate PWSIDs in the certified areas spreadsheet. There was one or two in the early data so we included this check, but we don't anticipate this issue going forward.
1. PWSIDs that don't occur in the Culinary Water Service Area geometries. As noted above, this indicates WRe doesn't have a polygon for their boundary.
