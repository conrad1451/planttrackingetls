# etl_script.rb
 

# --- Configure Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- GBIF API Configuration ---
GBIF_BASE_URL = "https://api.gbif.org/v1/occurrence/search"
# Default taxonKey for Common Milkweed (Asclepias syriaca)
DEFAULT_TAXON_KEY = "3170247"

# --- Neon Database Configuration (READ FROM ENVIRONMENT VARIABLES) ---
# Ensure these environment variables are set in your GitHub Actions secrets or local environment
NEON_DB_HOST = ENV["NEON_DB_HOST"]
NEON_DB_NAME = ENV["NEON_DB_NAME"]

NEON_DB_USER = os.getenv('NEON_DB_USER')
NEON_DB_USER = ENV["NEON_DB_USER"]


NEON_DB_PASSWORD = ENV['NEON_DB_PASSWORD']
 
NEON_DB_PORT = ENV['NEON_DB_PORT', '5432']

# --- AI Endpoint Configuration (READ FROM ENVIRONMENT VARIABLE) ---
# Ensure this environment variable is set in your GitHub Actions secrets or local environment
AI_ENDPOINT_BASE_URL = ENV['AI_ENDPOINT_BASE_URL']
 