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

### Silver v2 (fare rules added)
- Found from exploring fares: official initial charge is $3.00, so fares under $3 are impossible. Round-number fares like $999 and $840 appeared on trips of 1 mile or less. The ~$6 to $7 per mile on 90+ mile trips matches the doubled out-of-city rate, so a plain "fare above X" cap would have deleted real trips.
- New rules: fare_too_low (fare < $3.00) and fare_too_high (fare > 25 + 7 x miles + 1.4 x minutes)
- Report: wrong_month 56, bad_duration 115,627, bad_distance 246,913, fare_too_low 155,132, fare_too_high 44,181, missing_zone 0
- Rows kept: 9,150,502 (95.77%), removed 404,276 (4.23%). v1 kept 95.98%
- Rules overlap heavily (counts sum to 561,909 but only 404,276 rows were removed)
- Tips larger than the fare: 9,161 rows (0.10%), kept on purpose; gold uses sum(tips) / sum(fares) so they can't distort it

### Silver v3 (flat-fare exemption)
- Checked which rows fare_too_high removed on its own (5,830). 3,071 were rate code 2 (JFK) and 2,404 were rate code 5 (negotiated). 3,065 of the 3,130 trips with a fare of exactly $70.00 were rate code 2: the JFK flat fare. Their recorded distance is meaningless, so the distance-based fare ceiling was deleting real airport trips.
- Fix: flat/negotiated rate codes (2 and 5) get a ceiling of max(metered ceiling, $300). Kept ratecode as a silver column.
- Remaining removals by this rule: NULL rate code (294), code 99 (56), standard code 1 (5)
- Known gap: the distance rules (bad_distance) may also remove some flat-fare trips whose recorded distance is wrong. Not measured yet.
- Lesson: look at examples of what a rule removes, not just how many.

- Final silver report: wrong_month 56, bad_duration 115,627, bad_distance 246,913, fare_too_low 155,132, fare_too_high 2,424, missing_zone 0
- Rows kept: 9,155,947 (95.83%), removed 398,831 (4.17%)
- Version history: v1 95.98% kept, v2 95.77% (new fare rules), v3 95.83% (flat-fare exemption restored 5,445 rows)
- Known gap: about 36,000 flat-fare (code 2 and 5) trips fail the distance or duration rules, because a flat fare doesn't depend on distance. Some may be valid airport trips with a bad distance field. Airport counts in gold are slightly low. Not measured yet.
- Verified from disk: Jan 2,855,678 / Feb 2,885,773 / Mar 3,414,496 = 9,155,947. Fare range $3.00 to $661.70, distance 0.1 to 99.77 miles, pickups 2024-01-01 to 2024-03-31
- Removal rate by month (bronze to silver): Jan 3.67%, Feb 4.05%, Mar 4.69%. Cause of the rise not investigated yet

## Gold
(fill in after Stage 3)

## Benchmarks
(fill in after Stage 5)

## What broke and how I fixed it
- Spark rejected my PC name `Harsh_dell` (underscore not allowed in a Spark URL): set SPARK_LOCAL_HOSTNAME=localhost
- Spark couldn't write files on Windows: added winutils.exe and hadoop.dll, set HADOOP_HOME
- Taxi data downloads kept dropping the connection: added retries with backoff, and a .part file so half-downloaded files can't be mistaken for complete ones
- ModuleNotFoundError for pyspark: I forgot to activate the virtual environment