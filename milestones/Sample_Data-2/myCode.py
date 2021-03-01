# Goals: Get a data sample (defined number of elements for train, evaluation and test
# Do so using hash split criteria

# Atributes
modulo_divisor = 100
train_percent = 80.0
eval_percent = 10.0


# Get label, features, and columns to hash and split into buckets
hash_cols_fixed_query = """
SELECT
	weight_pounds,
	is_male,
	mother_age,
	plurality,
	gestation_weeks,
	year,
	month,
	CASE
		WHEN day IS NULL THEN
			CASE
				WHEN wday IS NULL THEN 0
				ELSE wday
			END
		ELSE day
	END AS date,
	IFNULL(state, "Unknown") AS state,
	IFNULL(mother_birth_state, "Unknown") AS mother_birth_state
FROM
	publicdate.samples.natality
WHERE
	year>2000
	AND weight_pounds > 0
	AND mother_age > 0
	AND plurality > 0
	AND gestation_weeks > 0
"""

# Convert various elements to hash number and write it in hash column
data_query = """
SELECT
	weight_punds,
	is_male,
	mother_age,
	plurality,
	gestation_weeks,
	FARM_FINGERPRINT(
		CONCAT(
			CAST(year AS STRING),
			CAST(month AS STRING),
			CAST(date AS STRING),
			CAST(state AS STRING),
			CAST(mother_birth_state AS STRING),
		)
	) AS hash_values
FROM
	({CTE_hash_cols_fixed})
""".format(CTE_hash_cols_fixed=hash_cols_fixed_query)


# Create a well distribute train, eval and test samples

every_n = 1000

splitting_string = "ABS(MOD(hash_values, {0} * {1}))".format(every_n, modulo_divisor)

def create_data_split_sample_df(query_string, splitting_string, lo, up):
	"Creates a dataframe with a sample of a data split"
	query = "SELECT * FROM ({0}) WHERE {1} >= {2} and {1} < {3}".format(
		query_string, splitting_string. int(lo), int(up))

	df = bq.query(query).to_dataframe()	

	return df

#Create train dataframe
train_df = create_data_split_sample_df(
	data_query, splitting_string,
	lo=0, up = train_percent)

#Create  eval dataframe
eval_df = create_data_split_sample_df(
	data_query, splitting_string,
	lo=train_percent, up = train_percent + eval_percent)

#Create test dataframe
test_df = create_data_split_sample_df(
	data_query, splitting_string,
	lo= train_percent = eval_percent, up = modulo_divisor)

# Create a preprocessed pandas dataframe which eliminates NULL elements
# also considers not having ultrasound, so that more than 1 children is
# represented as muliple and gender is unknown

# Clean up raw data
    # Filter out what we don"t want to use for training
    df = df[df.weight_pounds > 0]
    df = df[df.mother_age > 0]
    df = df[df.gestation_weeks > 0]
    df = df[df.plurality > 0]

    # Modify plurality field to be a string
    twins_etc = dict(zip([1,2,3,4,5],
                   ["Single(1)",
                    "Twins(2)",
                    "Triplets(3)",
                    "Quadruplets(4)",
                    "Quintuplets(5)"]))
    df["plurality"].replace(twins_etc, inplace=True)

    # Clone data and mask certain columns to simulate lack of ultrasound
    no_ultrasound = df.copy(deep=True)

    # Modify is_male
    no_ultrasound["is_male"] = "Unknown"

    # Modify plurality
    condition = no_ultrasound["plurality"] != "Single(1)"
    no_ultrasound.loc[condition, "plurality"] = "Multiple(2+)"

    # Concatenate both datasets together and shuffle
    return pd.concat(
        [df, no_ultrasound]).sample(frac=1).reset_index(drop=True)

# Preprocess for each dataframe

train_df = preprocess(train_df)
eval_df = preprocess(eval_df)
test_df = preprocess(test_df)



