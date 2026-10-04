# Results Log

Update this after every stage. The README is built from it.

## Data
- Source: NYC TLC Yellow Taxi trip records, Jan to Mar 2024
- Bronze rows: Jan 2,964,624 / Feb 3,007,526 / Mar 3,582,628 = **9,554,778**

## Exploration (January 2024 file)
- Pickups outside January 2024: 18 (earliest pickup was 2002-12-31)
- Distance <= 0: 60,371 (2.04%). Fare < 0: 37,448 (1.26%). Dropoff before pickup: 56
- Trip distance: median 1.68 mi, p99 20.0, p99.9 29.52, max 312,722.3
- Trip duration: median 11.6 min, p99 60.4, p99.9 115.15, max 9,455.4
- Payment types: card 2,319,046 / cash 439,191 / flex fare 140,162 / dispute 46,628 / no charge 19,597

## Silver (all 3 months)
| Rule | Rows failing | Share |
|---|---|---|
| wrong_month | 56 | 0.0006% |
| bad_duration (<1 or >180 min) | 115,627 | 1.21% |
| bad_distance (<0.1 or >100 mi) | 246,913 | 2.58% |
| bad_fare (<= 0) | 139,596 | 1.46% |
| missing_zone | 0 | 0% |

- Rows in: 9,554,778. Rows kept: **9,171,050 (95.98%)**. Removed: 383,728 (4.02%)
- Rule counts overlap, so they don't add up to the removed total
- Verified by reading silver back from disk: Jan 2,857,159 / Feb 2,889,150 / Mar 3,424,741 = 9,171,050
- Removal rate by month: Jan 3.62%, Feb 3.94%, Mar 4.41%
- Pickup range after cleaning: 2024-01-01 to 2024-03-31
- Known gaps: max fare is $999.00 and min fare is $0.01, so a fare-per-mile rule would be a good silver v2
## Gold
(fill in after Stage 3)

## Benchmarks
(fill in after Stage 5)

## What broke and how I fixed it
- Spark rejected my PC name `Harsh_dell` (underscore not allowed in a Spark URL): set SPARK_LOCAL_HOSTNAME=localhost
- Spark couldn't write files on Windows: added winutils.exe and hadoop.dll, set HADOOP_HOME
- Taxi data downloads kept dropping the connection: added retries with backoff, and a .part file so half-downloaded files can't be mistaken for complete ones
- ModuleNotFoundError for pyspark: I forgot to activate the virtual environment