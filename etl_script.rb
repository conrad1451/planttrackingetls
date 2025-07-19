# etl_script.rb
 
require 'net/http' # For making HTTP requests
require 'uri'      # For parsing and building URIs
require 'json'     # For parsing JSON responses
# --- Configure Logging ---
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# logger = logging.getLogger(__name__)

# --- GBIF API Configuration ---
GBIF_BASE_URL = "https://api.gbif.org/v1/occurrence/search"
# Default taxonKey for Common Milkweed (Asclepias syriaca)
DEFAULT_TAXON_KEY = "3170247"

# --- Neon Database Configuration (READ FROM ENVIRONMENT VARIABLES) ---
# Ensure these environment variables are set in your GitHub Actions secrets or local environment
NEON_DB_HOST = ENV["NEON_DB_HOST"]
NEON_DB_NAME = ENV["NEON_DB_NAME"]
NEON_DB_USER = ENV["NEON_DB_USER"]
NEON_DB_PASSWORD = ENV['NEON_DB_PASSWORD']
NEON_DB_PORT = ENV['NEON_DB_PORT', '5432']

# --- AI Endpoint Configuration (READ FROM ENVIRONMENT VARIABLE) ---
# Ensure this environment variable is set in your GitHub Actions secrets or local environment
AI_ENDPOINT_BASE_URL = ENV['AI_ENDPOINT_BASE_URL']

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

  begin
    # Create an HTTP request object
    http = Net::HTTP.new(uri.host, uri.port)
    http.use_ssl = (uri.scheme == 'https') # Enable SSL for HTTPS

    # Perform the GET request
    request = Net::HTTP::Get.new(uri.request_uri)
    response = http.request(request)

    # Raise an error for bad responses (4xx or 5xx status codes)
    response.value # This method raises Net::HTTPRetriableError or Net::HTTPServerException

    # Parse the JSON response body and return it.
    # This assumes the API always returns JSON.
    JSON.parse(response.body)

  rescue Net::HTTPClientException, Net::HTTPServerException => http_err
    puts "HTTP error occurred: #{http_err.message} (Code: #{http_err.response.code})"
    raise # Re-raise the exception after logging
  rescue Net::ReadTimeout, Net::OpenTimeout => timeout_err
    puts "Request timeout occurred: #{timeout_err.message}"
    raise
  rescue JSON::ParserError => json_err
    puts "JSON parsing error: #{json_err.message}. Response body: #{response.body}"
    raise
  rescue StandardError => e
    puts "An unexpected error occurred: #{e.message}"
    raise
  end
end

# Example usage (assuming you have a GBIF-like endpoint)
if __FILE__ == $0
  # Example GBIF API endpoint for occurrences (this is a placeholder)
  # You would replace this with an actual GBIF API URL
  gbif_endpoint = "https://api.gbif.org/v1/occurrence/search"
  
  # Example parameters for the GBIF API
  gbif_params = {
    "country" => "US",
    "limit" => 5
  }

  begin
    puts "Attempting to fetch data from: #{gbif_endpoint} with params: #{gbif_params}"
    data = fetch_gbif_page_etl(gbif_endpoint, gbif_params)
    puts "\nSuccessfully fetched data:"
    
    # Print the first few items or a summary of the data
    if data.is_a?(Hash) && data.key?("results")
      data["results"].take(2).each_with_index do |record, i| # Print first 2 records
        puts "Record #{i+1}: #{record.fetch("scientificName", "N/A")}"
      end
    else
      puts data # Print the whole response if not in expected format
    end

  rescue StandardError => e
    puts "\nAn error occurred during example usage: #{e.message}"
  end
end
