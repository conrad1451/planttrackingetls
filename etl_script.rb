# etl_script.rb

require 'net/http' # For making HTTP requests
require 'uri'      # For parsing and building URIs
require 'json'     # For parsing JSON responses
require 'pg'       # For connecting to PostgreSQL (Neon DB)
require 'time'     # For parsing and handling date/time objects
require 'logger'   # For robust logging

# --- Configure Logging ---
# Create a logger instance that writes to STDOUT
logger = Logger.new(STDOUT)
logger.level = Logger::INFO # Set default logging level to INFO
logger.formatter = proc do |severity, datetime, progname, msg|
  "#{datetime.strftime('%Y-%m-%d %H:%M:%S')} - #{severity} - #{msg}\n"
end

# --- GBIF API Configuration ---
GBIF_BASE_URL = "https://api.gbif.org/v1/occurrence/search"
# Default taxonKey for Common Milkweed (Asclepias syriaca)
DEFAULT_TAXON_KEY = "3170247"

# --- Neon Database Configuration (READ FROM ENVIRONMENT VARIABLES) ---
# Ensure these environment variables are set in your GitHub Actions secrets or local environment
# Provide default values for local testing if environment variables are not set
NEON_DB_HOST = ENV["NEON_DB_HOST"] || "localhost"
NEON_DB_NAME = ENV["NEON_DB_NAME"] || "your_neon_db"
NEON_DB_USER = ENV["NEON_DB_USER"] || "your_db_user"
NEON_DB_PASSWORD = ENV['NEON_DB_PASSWORD'] || "your_db_password"
# Neon's default PostgreSQL port is 5432. Corrected from original snippet.
NEON_DB_PORT = ENV['NEON_DB_PORT'] || '5432'

# --- AI Endpoint Configuration (READ FROM ENVIRONMENT VARIABLE) ---
# Ensure this environment variable is set in your GitHub Actions secrets or local environment
AI_ENDPOINT_BASE_URL = ENV['AI_ENDPOINT_BASE_URL']

# CHQ: Gemini AI debugged
# --- Helper function for robust date parsing ---
# Attempts to parse a date string into a Time object.
# Handles various formats that might come from GBIF.
def parse_date(date_str)
  return nil if date_str.nil? || date_str.strip.empty?

  begin
    # Try parsing as a full datetime
    Time.parse(date_str)
  rescue ArgumentError
    begin
      # Try parsing as a date only
      Date.parse(date_str).to_time
    rescue ArgumentError
      # If only year-month (e.g., "2023-01")
      if date_str =~ /^\d{4}-\d{2}$/
        Time.parse("#{date_str}-01") # Default to first day of month
      else
        logger.debug("Could not parse date string: '#{date_str}'")
        nil # Return nil if parsing fails
      end
    end
  end
end

# CHQ: Gemini AI debugged this function - originally translated from python version
# This function fetches data from a given endpoint with specified parameters.
# It's designed for Ruby and uses the 'net/http' library.
#
# Args:
#   endpoint (String): The URL of the API endpoint to fetch data from.
#   params (Hash): A hash of query parameters to send with the request.
#
# Returns:
#   Hash or Array: The JSON response from the API, parsed into a Ruby Hash or Array.
#
# Raises:
#   Net::HTTPError: If the HTTP request returns a bad status code (e.g., 4xx or 5xx).
#   StandardError: For other request-related or JSON parsing errors.
def fetch_gbif_page_etl(endpoint, params)
  # Build the URI with query parameters
  uri = URI(endpoint)
  uri.query = URI.encode_www_form(params)

  logger.info("Making GET request to: #{uri}")

  begin
    # Create an HTTP request object
    http = Net::HTTP.new(uri.host, uri.port)
    http.use_ssl = (uri.scheme == 'https') # Enable SSL for HTTPS
    http.read_timeout = 30 # Set read timeout to 30 seconds
    http.open_timeout = 10 # Set connection timeout to 10 seconds

    # Perform the GET request
    request = Net::HTTP::Get.new(uri.request_uri)
    response = http.request(request)

    # Raise an error for bad responses (4xx or 5xx status codes)
    response.value # This method raises Net::HTTPRetriableError or Net::HTTPServerException

    # Parse the JSON response body and return it.
    # This assumes the API always returns JSON.
    JSON.parse(response.body)

  rescue Net::HTTPClientException, Net::HTTPServerException => http_err
    logger.error("HTTP error occurred during extraction: #{http_err.message} (Code: #{http_err.response.code})")
    raise # Re-raise the exception after logging
  rescue Net::ReadTimeout, Net::OpenTimeout => timeout_err
    logger.error("Request timeout occurred during extraction: #{timeout_err.message}")
    raise
  rescue JSON::ParserError => json_err
    logger.error("JSON parsing error during extraction: #{json_err.message}. Response body (first 200 chars): #{response.body[0..199]}...")
    raise
  rescue StandardError => e
    logger.error("An unexpected error occurred during extraction: #{e.message}")
    raise
  end
end

# --- AI Endpoint Batch Fetch Function ---
# This function sends a batch of coordinates to the AI endpoint for enrichment.
#
# Args:
#   batch_payload (Array<Hash>): An array of hashes, each containing latitude, longitude,
#                                 coordinate_uncertainty, and original_index.
#
# Returns:
#   Array<Hash>: An array of results from the AI endpoint, or an empty array if an error occurs.
def fetch_ai_county_city_town_analysis_batch(batch_payload)
  return [] if AI_ENDPOINT_BASE_URL.nil? || batch_payload.empty?

  ai_uri = URI("#{AI_ENDPOINT_BASE_URL}/reverse-geocode-batch") # Assuming this is your batch endpoint
  logger.info("Sending batch of #{batch_payload.length} coordinates to AI endpoint: #{ai_uri}")

  begin
    ai_http = Net::HTTP.new(ai_uri.host, ai_uri.port)
    ai_http.use_ssl = (ai_uri.scheme == 'https')
    ai_http.read_timeout = 60 # AI calls might take longer
    ai_http.open_timeout = 15

    ai_request = Net::HTTP::Post.new(ai_uri.request_uri, 'Content-Type' => 'application/json')
    ai_request.body = batch_payload.to_json # Send the array of hashes as JSON

    ai_response = ai_http.request(ai_request)

    ai_response.value # Raise an error for bad responses

    JSON.parse(ai_response.body) # Return the parsed JSON response

  rescue Net::HTTPClientException, Net::HTTPServerException => http_err
    logger.error("HTTP error during batch AI endpoint call: #{http_err.message} (Code: #{http_err.response.code})")
    [] # Return empty array on error
  rescue Net::ReadTimeout, Net::OpenTimeout => timeout_err
    logger.error("AI endpoint batch request timeout: #{timeout_err.message}")
    []
  rescue JSON::ParserError => json_err
    logger.error("JSON parsing error from AI endpoint: #{json_err.message}. Response body (first 200 chars): #{ai_response.body[0..199]}...")
    []
  rescue StandardError => e
    logger.error("An unexpected error occurred during batch AI endpoint call: #{e.message}", e.backtrace.join("\n"))
    []
  end
end

# CHQ: Gemini AI debugged
# --- Extraction Function ---
def extract_gbif_data(
    taxon_key: DEFAULT_TAXON_KEY,
    country: 'US',
    has_coordinate: 'true',
    has_geospatial_issue: 'false',
    limit_per_request: 300, # GBIF API max limit is 300
    target_year: nil,
    target_month: nil,
    num_pages_to_extract: nil,
    limiting_page_count: nil # Renamed from `limiting_page_count` in Python to avoid confusion with bool
)
  """
  Extracts occurrence data from the GBIF API, supporting date range filtering.
  """
  all_records = []
  offset = 0
  end_of_records = false
  pages_fetched = 0 # To track how many pages we've actually fetched

  params = {
      'taxonKey' => taxon_key,
      'country' => country,
      'hasCoordinate' => has_coordinate,
      'hasGeospatialIssue' => has_geospatial_issue,
      'limit' => limit_per_request
  }

  params['year'] = target_year unless target_year.nil?
  params['month'] = target_month unless target_month.nil?

  while !end_of_records
    # CHQ: Gemini AI added logic for breaking out of the loop when num pages is specified and exceeded
    if !num_pages_to_extract.nil? && pages_fetched >= num_pages_to_extract
      logger.info("Reached num_pages_to_extract limit (#{num_pages_to_extract}). Stopping extraction.")
      break
    end

    current_params = params.dup # Use dup for a shallow copy of the hash
    current_params['offset'] = offset
    begin
      data = fetch_gbif_page_etl(GBIF_BASE_URL, current_params)
      records = data['results'] || []
      all_records.concat(records) # Use concat to add elements from one array to another

      count = data['count'] || 0
      end_of_records = data['endOfRecords'] || true
      offset += records.length # Use records.length to correctly advance offset
      pages_fetched += 1 # Increment page count

      logger.info("Fetched #{records.length} records. Total: #{all_records.length}. Next offset: #{offset}. End of records: #{end_of_records}")

      # CHQ: Gemini AI implemented limiting page count logic
      # Implement limiting_page_count logic
      if !limiting_page_count.nil? && pages_fetched >= limiting_page_count
        logger.info("Reached limiting_page_count (#{limiting_page_count}). Stopping extraction.")
        break # Break the loop if the limit is reached
      end

      # Implement a small delay between GBIF API calls to be polite and avoid rate limits
      if !end_of_records && records.length > 0
        sleep(0.5) # Half a second delay
      elsif records.length == 0 && offset > 0 # If no records but offset is not 0, it indicates no more data
        end_of_records = true
      end

    rescue StandardError => e # Catch any error from fetch_gbif_page_etl
      logger.error("Error during GBIF extraction: #{e.message}", e.backtrace.join("\n"))
      break # Break the loop on error
    end
  end

  logger.info("Finished extraction. Total raw records extracted: #{all_records.length}")
  all_records
end

# CHQ: Gemini AI debugged
# --- Transformation Function ---
# This function takes raw GBIF data (Array of Hashes) and transforms it.
# It applies cleaning, normalization, and potentially uses the AI endpoint for enrichment.
#
# Args:
#   raw_data (Array<Hash>): The data fetched from the GBIF API.
#
# Returns:
#   Array<Hash>: An array of transformed records, ready for loading.

# CHQ: Gemini AI debugged
def transform_gbif_data(raw_data)
  if raw_data.nil? || raw_data.empty?
    logger.warn("No raw data to transform.")
    return []
  end

  logger.info("Starting transformation of #{raw_data.length} records.")

  transformed_records = []

  raw_data.each_with_index do |record, index|
    # Skip record if essential coordinates are missing
    next if record['decimalLatitude'].nil? || record['decimalLongitude'].nil?

    # 1. Robust Date Parsing for 'eventDate'
    event_date_parsed = parse_date(record['eventDate'])
    next if event_date_parsed.nil? # Skip records where date cannot be parsed

    # 2. Convert coordinates to numeric, coercing errors to nil
    decimal_latitude = Float(record['decimalLatitude']) rescue nil
    decimal_longitude = Float(record['decimalLongitude']) rescue nil
    next if decimal_latitude.nil? || decimal_longitude.nil? # Skip if coordinates are not numeric

    # 3. Handle 'individualCount': Coerce to numeric, fill nil with 1
    individual_count = Integer(record['individualCount']) rescue 1 # Default to 1 if not present or invalid

    # --- Enrichment / Feature Engineering ---
    transformed_record = {
      'gbifID' => record['gbifID'].to_s, # Ensure it's a string
      'datasetKey' => record['datasetKey'],
      'datasetName' => record['datasetName'],
      'publishingOrgKey' => record['publishingOrgKey'],
      'publishingOrganizationTitle' => record['publishingOrganizationTitle'],
      'eventDate' => record['eventDate'], # Keep original string
      'eventDateParsed' => event_date_parsed,
      'year' => event_date_parsed.year,
      'month' => event_date_parsed.month,
      'day' => event_date_parsed.day,
      'day_of_week' => event_date_parsed.wday, # Sunday is 0, Saturday is 6
      'week_of_year' => event_date_parsed.strftime('%W').to_i, # ISO week number
      'date_only' => event_date_parsed.to_date, # Date object without time
      'scientificName' => record['scientificName'],
      'vernacularName' => record['vernacularName'],
      'taxonKey' => record['taxonKey'],
      'kingdom' => record['kingdom'],
      'phylum' => record['phylum'],
      'class_name' => record['class'], # 'class' is a reserved keyword in Ruby
      'order_name' => record['order'], # 'order' is a reserved keyword in Ruby
      'family' => record['family'],
      'genus' => record['genus'],
      'species' => record['species'],
      'decimalLatitude' => decimal_latitude,
      'decimalLongitude' => decimal_longitude,
      'coordinateUncertaintyInMeters' => Float(record['coordinateUncertaintyInMeters']) rescue nil,
      'countryCode' => record['countryCode'],
      'stateProvince' => record['stateProvince'],
      'locality' => record['locality'],
      'individualCount' => individual_count,
      'basisOfRecord' => record['basisOfRecord'],
      'recordedBy' => record['recordedBy'],
      'occurrenceID' => record['occurrenceID'],
      'collectionCode' => record['collectionCode'],
      'catalogNumber' => record['catalogNumber'],
      'county' => nil, # Placeholder for AI enrichment
      'cityOrTown' => nil # Placeholder for AI enrichment
    }
    transformed_records << transformed_record
  end

  # --- Add 'county' and 'cityOrTown' columns using the AI endpoint (BATCHED) ---
  coords_to_enrich_payload = []
  transformed_records.each_with_index do |record, original_idx|
    if !record['decimalLatitude'].nil? && !record['decimalLongitude'].nil?
      uncertainty = record['coordinateUncertaintyInMeters'] || 0
      coords_to_enrich_payload << {
        "gbifID_original_index" => original_idx, # Pass the original array index to map back
        "latitude" => record['decimalLatitude'],
        "longitude" => record['decimalLongitude'],
        "coordinate_uncertainty" => uncertainty
      }
    end
  end

  if !coords_to_enrich_payload.empty?
    logger.info("Sending #{coords_to_enrich_payload.length} coordinates in a batch to AI endpoint for enrichment.")
    BATCH_SIZE = 100 # Adjust based on your AI endpoint's capacity and rate limits
    all_batch_results = []

    # Iterate through coords_to_enrich_payload in chunks
    coords_to_enrich_payload.each_slice(BATCH_SIZE).each_with_index do |chunk, i|
      begin
        chunk_results = fetch_ai_county_city_town_analysis_batch(chunk)
        all_batch_results.concat(chunk_results)
        logger.info("Processed batch #{i + 1}. Total results collected: #{all_batch_results.length}")
        # Introduce a small delay between *chunks* of batch calls to prevent overloading
        if i + 1 < (coords_to_enrich_payload.length.to_f / BATCH_SIZE).ceil
          sleep(0.5) # Wait 0.5 seconds between batches
        end
      rescue StandardError => e
        logger.error("Error during batch AI endpoint call for chunk #{i + 1}: #{e.message}", e.backtrace.join("\n"))
        # Continue with other chunks even if one fails
      end
    end

    # Map the results back to the transformed_records array using the original index
    all_batch_results.each do |result|
      original_idx = result['gbifID_original_index']
      county = result['county']
      city = result['city/town']
      error = result['error'] # Check for individual errors from the AI endpoint

      if !original_idx.nil? && original_idx < transformed_records.length
        if error.nil? || error.empty? # No error for this specific record
          transformed_records[original_idx]['county'] = county
          transformed_records[original_idx]['cityOrTown'] = city
        else
          logger.warn("Error for record at original index #{original_idx} (Lat: #{result['latitude']}, Lon: #{result['longitude']}): #{error}")
        end
      else
        logger.warn("Could not find original index #{original_idx} in transformed_records for result: #{result}")
      end
    end
  end

  logger.info("Finished enriching location data with AI endpoint.")
  logger.info("Finished transformation. Transformed records: #{transformed_records.length}.")
  transformed_records
end

# CHQ: Gemini AI debugged
# --- Load Function ---
# This function loads the transformed data (Array of Hashes) into the Neon PostgreSQL database.
#
# Args:
#   data (Array<Hash>): An array of transformed records to load.
#   table_name (String): The name of the table to load data into.
#
# Returns:
#   nil
def load_data(data, table_name="gbif_occurrences")
  if data.nil? || data.empty?
    logger.info("No data to load into '#{table_name}'.")
    return
  end

  db_conn = nil
  begin
    logger.info("Connecting to Neon DB for loading...")
    db_conn = PG.connect(
      host: NEON_DB_HOST,
      dbname: NEON_DB_NAME,
      user: NEON_DB_USER,
      password: NEON_DB_PASSWORD,
      port: NEON_DB_PORT
    )
    logger.info("Successfully connected to Neon DB.")

    logger.info("Attempting to load #{data.length} records into '#{table_name}' table...")

    # Define your table schema. Adjust column names and types as necessary.
    # Note: 'class' and 'order' are reserved keywords in SQL, so use 'class_name' and 'order_name'.
    db_conn.exec("
      CREATE TABLE IF NOT EXISTS #{table_name} (
        id SERIAL PRIMARY KEY,
        gbifID VARCHAR(255),
        datasetKey VARCHAR(255),
        datasetName TEXT,
        publishingOrgKey VARCHAR(255),
        publishingOrganizationTitle TEXT,
        eventDate TEXT,
        eventDateParsed TIMESTAMP WITH TIME ZONE,
        year INTEGER,
        month INTEGER,
        day INTEGER,
        day_of_week INTEGER,
        week_of_year INTEGER,
        date_only DATE,
        scientificName TEXT,
        vernacularName TEXT,
        taxonKey INTEGER,
        kingdom VARCHAR(100),
        phylum VARCHAR(100),
        class_name VARCHAR(100),
        order_name VARCHAR(100),
        family VARCHAR(100),
        genus VARCHAR(100),
        species VARCHAR(100),
        decimalLatitude NUMERIC(10, 7),
        decimalLongitude NUMERIC(10, 7),
        coordinateUncertaintyInMeters NUMERIC(10, 3),
        countryCode VARCHAR(10),
        stateProvince TEXT,
        locality TEXT,
        county TEXT,
        cityOrTown TEXT,
        individualCount INTEGER,
        basisOfRecord VARCHAR(100),
        recordedBy TEXT,
        occurrenceID TEXT,
        collectionCode VARCHAR(100),
        catalogNumber TEXT
      );
    ")
    logger.info("Table '#{table_name}' ensured to exist.")

    # Prepare for bulk insertion using `exec_params` for safety
    # Construct the INSERT statement dynamically based on the keys in the first record
    # This assumes all records in `data` have the same keys after transformation.
    first_record_keys = data.first.keys.map { |k| "\"#{k}\"" }.join(', ') # Quote column names
    placeholders = (1..data.first.keys.length).map { |i| "$#{i}" }.join(', ')
    insert_sql = "INSERT INTO #{table_name} (#{first_record_keys}) VALUES (#{placeholders})"

    data.each_with_index do |record, i|
      begin
        # Convert Time/Date objects to strings for PG if needed, or rely on PG gem's conversion
        # Use .to_s for Time/Date objects to ensure consistent string representation for DB
        # Convert `nil` to `nil` for PG `NULL`
        values = record.values.map do |val|
          if val.is_a?(Time)
            val.iso8601 # Standard ISO 8601 format for timestamps
          elsif val.is_a?(Date)
            val.iso8601 # Standard ISO 8601 format for dates
          else
            val.nil? ? nil : val.to_s # Convert everything else to string, handle nil
          end
        end

        db_conn.exec_params(insert_sql, values)
        # logger.debug("Inserted record #{i+1}: #{record['gbifID']}") # Uncomment for verbose logging
      rescue PG::Error => e
        logger.error("Error inserting record #{record['gbifID'] || i}: #{e.message}")
        # Continue to next record even if one fails
      rescue StandardError => e
        logger.error("Unexpected error preparing record #{record['gbifID'] || i} for insertion: #{e.message}")
      end
    end
    logger.info("Finished loading data into '#{table_name}'.")

  rescue PG::Error => e
    logger.error("Database connection or operation error: #{e.message}")
    raise # Re-raise for main workflow to catch
  rescue StandardError => e
    logger.error("An unexpected error occurred during the load phase: #{e.message}", e.backtrace.join("\n"))
    raise
  ensure
    # Close database connection
    if db_conn
      db_conn.close
      logger.info("Database connection closed.")
    end
  end
end


# --- Main ETL Orchestration Function ---
def run_commonweed_etl(year, month)
  """
  Orchestrates the ETL process for Common milkweed data for a given month and year.
  """

  my_calendar = {
      1 => "January", 2 => "February", 3 => "March", 4 => "April",
      5 => "May", 6 => "June", 7 => "July", 8 => "August",
      9 => "September", 10 => "October", 11 => "November", 12 => "December"
  }

  logger.info("\n\nRunning ETL for #{year}-#{month} (#{my_calendar[month]} #{year})\n")
  logger.info("--- ETL process started ---")

  begin
    raw_data = extract_gbif_data(target_year: year, target_month: month, limiting_page_count: 10, num_pages_to_extract: 10)

    if raw_data && !raw_data.empty?
      transformed_data = transform_gbif_data(raw_data)
      if transformed_data && !transformed_data.empty?
        # Use the month name and year for the table name
        table_name = "#{my_calendar[month].downcase}_#{year}_gbif_occurrences"
        load_data(transformed_data, table_name)
      else
        logger.info("Transformed data is empty. No data to load.")
      end
    else
      logger.info("No raw data extracted. ETL process aborted.")
    end
  rescue StandardError => e
    logger.error("An error occurred during the main ETL workflow: #{e.message}", e.backtrace.join("\n"))
  end

  logger.info("--- ETL process finished ---")
end


# --- Script Entry Point ---
if __FILE__ == $0
  # --- Step 0: Install the 'pg' gem if you haven't already ---
  # You need to install the PostgreSQL gem for Ruby:
  # gem install pg

  # Example usage: Run ETL for June 2025
  # You can change these values to fetch data for different months/years
  target_year_example = 2025
  target_month_example = 6 # June

  run_commonweed_etl(target_year_example, target_month_example)
end
