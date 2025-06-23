#!/bin/bash

set -e

CLICKHOUSE_USER="${CLICKHOUSE_USER}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD}"
FILE_NAME="${FILE_NAME:-en.openfoodfacts.org.products.csv}"
DATA_DIR="/var/lib/clickhouse/user_files"
FULL_PATH="${DATA_DIR}/${FILE_NAME}"
SELECTED_COLUMNS="${SELECTED_COLUMNS:-}"

# Check if SELECTED_COLUMNS is provided
if [ -z "$SELECTED_COLUMNS" ]; then
    echo "ERROR: SELECTED_COLUMNS environment variable is not set." >&2
    exit 1
fi

# Split SELECTED_COLUMNS into array
IFS=' ' read -r -a COLUMNS_ARRAY <<< "$SELECTED_COLUMNS"

# Always include 'code' column for primary key
COLUMNS_ARRAY+=("code")

# Generate column definitions and select expressions
COL_DEFS=""
INSERT_COLS=""
SELECT_CLS=""

for col in "${COLUMNS_ARRAY[@]}"; do
    if [ "$col" == "prediction" ]; then
        continue
    fi

    if [ "$col" == "code" ]; then
      # Add column definition
      if [ -z "$COL_DEFS" ]; then
          COL_DEFS="\`$col\` String"
      else
          COL_DEFS="$COL_DEFS, \`$col\` String"
      fi
      
      # Add to insert columns list
      if [ -z "$INSERT_COLS" ]; then
          INSERT_COLS="\`$col\`"
      else
          INSERT_COLS="$INSERT_COLS, \`$col\`"
      fi
      
      # Add to select expression
      if [ -z "$SELECT_CLS" ]; then
          SELECT_CLS="toValidUTF8(\`$col\`) as \`$col\`"
      else
          SELECT_CLS="$SELECT_CLS, toValidUTF8(\`$col\`) as \`$col\`"
      fi
      continue
    fi
    
    # Add column definition
    if [ -z "$COL_DEFS" ]; then
        COL_DEFS="\`$col\` Nullable(Float64)"
    else
        COL_DEFS="$COL_DEFS, \`$col\` Nullable(Float64)"
    fi
    
    # Add to insert columns list
    if [ -z "$INSERT_COLS" ]; then
        INSERT_COLS="\`$col\`"
    else
        INSERT_COLS="$INSERT_COLS, \`$col\`"
    fi
    
    # Add to select expression
    if [ -z "$SELECT_CLS" ]; then
        SELECT_CLS="toFloat64OrNull(toString(\`$col\`)) as \`$col\`"
    else
        SELECT_CLS="$SELECT_CLS, toFloat64OrNull(toString(\`$col\`)) as \`$col\`"
    fi
done

echo "Creating database..."
clickhouse-client -h localhost \
--user "${CLICKHOUSE_USER}" \
--password "${CLICKHOUSE_PASSWORD}" \
-q "CREATE DATABASE IF NOT EXISTS foodfacts;"

echo "Creating new products table with selected columns..."
clickhouse-client -h localhost \
--user "${CLICKHOUSE_USER}" \
--password "${CLICKHOUSE_PASSWORD}" \
-q "
CREATE TABLE IF NOT EXISTS foodfacts.products (
    $COL_DEFS
) ENGINE = MergeTree()
ORDER BY (code);
"

echo "Importing selected columns..."
clickhouse-client -h localhost \
--user "${CLICKHOUSE_USER}" \
--password "${CLICKHOUSE_PASSWORD}" \
-q "
INSERT INTO foodfacts.products ($INSERT_COLS)
SELECT $SELECT_CLS
FROM file(
    '${FULL_PATH}',
    'TSVWithNames'
)
SETTINGS
    input_format_null_as_default = 1,
    format_csv_delimiter = '\t',
    input_format_allow_errors_num = 1000,
    input_format_allow_errors_ratio = 0.01;
"

echo "Creating predicts table..."
clickhouse-client -h localhost \
--user "${CLICKHOUSE_USER}" \
--password "${CLICKHOUSE_PASSWORD}" \
-q "
CREATE TABLE IF NOT EXISTS foodfacts.predicts (
    $COL_DEFS,
    \`prediction\` Nullable(Float32)
) ENGINE = MergeTree()
ORDER BY (code);
"

echo "Checking column stats..."
clickhouse-client -h localhost \
--user "${CLICKHOUSE_USER}" \
--password "${CLICKHOUSE_PASSWORD}" \
-q "SELECT name, type FROM system.columns WHERE database = 'foodfacts' AND table = 'products'" \
| while read -r column type; do
    # Escape column name for ClickHouse
    escaped_column="\`${column}\`"
    echo -n "Column: ${column} (${type}) "
    # Get distinct count
    distinct_count=$(clickhouse-client -h localhost \
      --user "${CLICKHOUSE_USER}" \
      --password "${CLICKHOUSE_PASSWORD}" \
      -q "SELECT countDistinct(${escaped_column}) FROM foodfacts.products FORMAT TSV")
    echo -n "| Distinct: ${distinct_count} "
    # Get non-null count
    non_null=$(clickhouse-client -h localhost \
      --user "${CLICKHOUSE_USER}" \
      --password "${CLICKHOUSE_PASSWORD}" \
      -q "SELECT countIf(${escaped_column} IS NOT NULL) FROM foodfacts.products FORMAT TSV")
    echo "| Non-null: ${non_null}"
done

echo "Initialization completed!"
