#!/usr/bin/env python3
import re
import json
import pandas as pd
import requests
import anthropic
import logging
import sys
from datetime import datetime
from typing import Dict, List, Union
from dataclasses import dataclass
from enum import Enum
import numpy as np
from time import sleep

# Set up logging to display debug-level messages and force the configuration.
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
    force=True
)

# -------------------------------
# GEOCODING HELPER FUNCTION
# -------------------------------
try:
    from geopy.geocoders import Nominatim
except ImportError:
    Nominatim = None

def get_geolocation(country: str, city: str, specific_location: str = "") -> Dict[str, str]:
    """
    Given a country, city and optionally a more specific location, return a dictionary
    with latitude and longitude. Uses Nominatim from geopy if available.
    """
    if not Nominatim:
        logging.debug("Nominatim is not available. Skipping geolocation.")
        return {"latitude": "", "longitude": ""}
    
    geolocator = Nominatim(user_agent="scam_center_dashboard")
    query = ", ".join(filter(None, [specific_location, city, country]))
    logging.debug("Geocoding query: %s", query)
    try:
        location = geolocator.geocode(query, timeout=10)
        sleep(1)  # Respect Nominatim usage policies
        if location:
            logging.debug("Geo location found: %s, %s", location.latitude, location.longitude)
            return {"latitude": str(location.latitude), "longitude": str(location.longitude)}
        else:
            logging.debug("No geolocation found for query: %s", query)
    except Exception as e:
        logging.error("Error geocoding query '%s': %s", query, e)
    return {"latitude": "", "longitude": ""}

# -------------------------------
# DATA MODELS
# -------------------------------
class LaborType(Enum):
    FORCED = "forced"
    DECEPTIVE = "deceptive"
    VOLUNTARY = "voluntary"
    UNKNOWN = "unknown"

@dataclass
class ScamIncident:
    incident_date: str
    source: str
    source_url: str
    incident_location: Dict[str, str]  # country, city, specific_location
    victim_demographics: Dict[str, Union[str, List[str]]]  # nationalities, age_ranges, gender
    employer_details: Dict[str, str]  # nationality, organization_name
    labor_conditions: Dict[str, Union[bool, str]]  # forced_labor, passport_confiscation, debt_bondage
    operation_type: List[str]  # Types of scams conducted
    law_enforcement_action: Dict[str, Union[bool, str]]  # raid_conducted, arrests_made, victims_rescued
    incident_scale: Dict[str, int]  # num_victims, num_perpetrators
    verification_status: str  # verified, partially_verified, unverified

# -------------------------------
# CONFIGURATION
# -------------------------------
class Config:
    """
    Configuration for interacting with APIs.
    """
    def __init__(self, api_key: str, model: str, api_url: str):
        self.api_key = api_key
        self.model = model
        self.api_url = api_url

    @property
    def headers(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"}

# -------------------------------
# PERPLEXITY API INTERACTION
# -------------------------------
class PerplexityClient:
    def __init__(self, config: Config):
        self.config = config

    def query_articles(self, month_label: str) -> str:
        """
        Query Perplexity for scam center articles specifically for a given month of 2024.
        Returns the raw text response.
        """
        payload = {
            "model": self.config.model,
            "messages": [
                {
                    "role": "system",
                    "content": "Be precise and concise."
                },
                {
                    "role": "user",
                    "content": (
                        f"Return a detailed list of news articles about scam centers, focusing on forced labor aspects, "
                        f"published specifically during {month_label} 2024. "
                        f"Include details such as publication date, source, URL, location, victim information, "
                        f"perpetrator details, operation information, and law enforcement response."
                    )
                }
            ],
            "max_tokens": 12700,
            "temperature": 0.2,
            "top_p": 0.9,
            "search_domain_filter": None,
            "return_images": False,
            "return_related_questions": False,
            # We remove or set search_recency_filter to None so we can focus on the exact month (2024)
            "search_recency_filter": None,
            "top_k": 0,
            "stream": False,
            "presence_penalty": 0,
            "frequency_penalty": 1,
            "response_format": None
        }
        logging.debug("Perplexity payload: %s", json.dumps(payload, indent=2))
        try:
            response = requests.post(
                self.config.api_url,
                json=payload,
                headers=self.config.headers
            )
            logging.debug("Perplexity response status: %s", response.status_code)
            logging.debug("Perplexity response text: %s", response.text)
            response.raise_for_status()
            data = response.json()
            if "choices" in data and len(data["choices"]) > 0:
                # Adjust extraction if data structure differs
                content = data["choices"][0]["message"]["content"]
                logging.debug("Extracted content from Perplexity: %s", content)
                return content
            else:
                logging.error("Unexpected data format from Perplexity: %s", data)
        except Exception as e:
            logging.error("Error querying Perplexity API: %s", e)
        return ""

# -------------------------------
# CLAUDE API INTERACTION WITH CITATIONS
# -------------------------------
class ClaudeClient:
    def __init__(self, config: Config):
        self.config = config
        self.client = anthropic.Anthropic(api_key=config.api_key)

    @staticmethod
    def _extract_json(text: str) -> str:
        """
        Extract JSON contained in triple-backtick code blocks (with optional language tag).
        If no code block is found, return the entire text.
        """
        match = re.search(r"```(?:json)?\s*(.*?)\s*```", text, re.DOTALL)
        return match.group(1).strip() if match else text.strip()

    @staticmethod
    def _fix_json(json_str: str) -> str:
        """
        Fix common JSON formatting issues, such as trailing commas.
        """
        json_start = json_str.find('[')
        json_end = json_str.rfind(']')
        if json_start != -1 and json_end != -1:
            json_str = json_str[json_start:json_end+1]
        json_str = re.sub(r",\s*(\]|\})", r"\1", json_str)
        if not json_str.strip().endswith(']'):
            json_str += ']'
        open_braces = json_str.count('{')
        close_braces = json_str.count('}')
        if open_braces > close_braces:
            json_str += '}' * (open_braces - close_braces)
        return json_str

    def transform_data(self, perplexity_text: str) -> List[Dict]:
        """
        Use Claude (with citations enabled) to transform the Perplexity API output into our JSON schema.
        """
        transformation_prompt = (
            "Please transform the following Perplexity API output into a JSON array where each element is a JSON object with the following fields:\n\n"
            "1. Basic Details:\n"
            "   - publishedAt (date in YYYY-MM-DD format or empty string if not available)\n"
            "   - source (news source name or empty string)\n"
            "   - sourceUrl (URL or empty string). IMPORTANT: If the Perplexity output contains a URL (which will always start with http), copy it exactly as-is without any modifications.\n"
            "   - incidentLocation (an object with fields: country, city, specific_location)\n\n"
            "2. Victim Information:\n"
            "   - nationalities (list)\n"
            "   - approximateNumberOfVictims (number, use 0 if not available)\n"
            "   - demographicDetails (string, or empty string)\n"
            "   - laborConditions (string describing forced/deceptive/voluntary recruitment or empty string)\n\n"
            "3. Perpetrator Details:\n"
            "   - nationalityOfOperators (string or empty string)\n"
            "   - organizationNames (list or empty list)\n"
            "   - numberOfPerpetrators (number, use 0 if not available)\n\n"
            "4. Operation Details:\n"
            "   - typesOfScams (list)\n"
            "   - duration (string, or empty string)\n"
            "   - scale (string, or empty string)\n\n"
            "5. Law Enforcement Response:\n"
            "   - raidDetails (string or empty string)\n"
            "   - arrestsMade (string or empty string)\n"
            "   - victimRescueOperations (string or empty string)\n\n"
            "6. Additional Information:\n"
            "   - incidentId (a unique identifier for the incident, e.g. a short hash or ID string)\n"
            "   - incidentDescription (a short text summary of the incident, or empty string)\n"
            "   - investigationStatus (string indicating status such as 'open', 'closed', or 'ongoing')\n"
            "   - dateScraped (the date when the data was scraped in YYYY-MM-DD format)\n"
            "   - geolocation (an object with fields: latitude and longitude; use null or empty string if not available)\n\n"
            "Return the entire response as a valid JSON array enclosed in triple backticks with the language tag (i.e. ```json )."
        )
        combined_content = transformation_prompt + "\n\n" + "Perplexity API Output:\n" + perplexity_text
        logging.debug("Combined content sent to Claude:\n%s", combined_content)
        try:
            response = self.client.messages.create(
                model=self.config.model,
                max_tokens=8192,
                messages=[{"role": "user", "content": combined_content}],
            )
            logging.debug("Claude API response (raw content): %s", response.content)
            # Adjust extraction based on how the response content is structured:
            content = response.content[0].text if response.content else ""
            logging.debug("Content extracted from Claude API: %s", content)
            json_str = self._extract_json(content)
            logging.debug("Extracted JSON string: %s", json_str)
            fixed_json = self._fix_json(json_str)
            logging.debug("Fixed JSON string: %s", fixed_json)
            data = json.loads(fixed_json)
            return data
        except Exception as e:
            logging.error("Error transforming data using Claude: %s", e)
        return []

# -------------------------------
# DATA PROCESSING & FLATTENING
# -------------------------------
class ScamCenterAnalyzer:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def process_data(self) -> pd.DataFrame:
        """
        Process the raw JSON data.
        """
        if self.df.empty:
            logging.debug("Received an empty DataFrame in process_data.")
            return self.df

        if "publishedAt" in self.df.columns:
            self.df["publishedAt"] = pd.to_datetime(self.df["publishedAt"], errors="coerce")
        if "approximateNumberOfVictims" in self.df.columns:
            self.df["approximateNumberOfVictims"] = pd.to_numeric(
                self.df["approximateNumberOfVictims"], errors="coerce"
            ).fillna(0)
        if "laborConditions" in self.df.columns:
            self.df["labor_type"] = self.df["laborConditions"].apply(
                lambda cond: cond.lower() if cond else "unknown"
            )
        else:
            self.df["labor_type"] = "unknown"
        return self.df

    def flatten_data(self) -> pd.DataFrame:
        """
        Flatten nested dictionary fields into individual columns.
        """
        flattened_rows = []
        for _, row in self.df.iterrows():
            # 1) Basic details
            basic = row.get("basicDetails", {}) if isinstance(row.get("basicDetails"), dict) else {}
            publishedAt = basic.get("publishedAt", "")
            source = basic.get("source", "")
            sourceUrl = basic.get("sourceUrl", "")
            incidentLocation = basic.get("incidentLocation", {})
            incident_country = incidentLocation.get("country", "")
            incident_city = incidentLocation.get("city", "")
            incident_specific_location = incidentLocation.get("specific_location", "")

            # 2) Victim information
            victim = row.get("victimInformation", {}) if isinstance(row.get("victimInformation"), dict) else {}
            victim_nationalities = victim.get("nationalities", [])
            approximateNumberOfVictims = victim.get("approximateNumberOfVictims", 0)
            demographicDetails = victim.get("demographicDetails", "")
            laborConditions = victim.get("laborConditions", "")

            # 3) Perpetrator details
            perp = row.get("perpetratorDetails", {}) if isinstance(row.get("perpetratorDetails"), dict) else {}
            operator_nationality = perp.get("nationalityOfOperators", "")
            organizationNames = perp.get("organizationNames", [])
            numberOfPerpetrators = perp.get("numberOfPerpetrators", 0)

            # 4) Operation details
            oper = row.get("operationDetails", {}) if isinstance(row.get("operationDetails"), dict) else {}
            scam_types = oper.get("typesOfScams", [])
            duration = oper.get("duration", "")
            scale = oper.get("scale", "")

            # 5) Law enforcement response
            law = row.get("lawEnforcementResponse", {}) if isinstance(row.get("lawEnforcementResponse"), dict) else {}
            raidDetails = law.get("raidDetails", "")
            arrestsMade = law.get("arrestsMade", "")
            victimRescueOperations = law.get("victimRescueOperations", "")

            # 6) Additional information
            additional = row.get("additionalInformation", {}) if isinstance(row.get("additionalInformation"), dict) else {}
            incidentId = additional.get("incidentId", "")
            incidentDescription = additional.get("incidentDescription", "")
            investigationStatus = additional.get("investigationStatus", "")
            dateScraped = additional.get("dateScraped", "")
            geolocation = additional.get("geolocation", {})
            latitude = geolocation.get("latitude", "")
            longitude = geolocation.get("longitude", "")

            # Attempt geocoding if latitude/longitude is missing
            if not latitude or not longitude:
                generated_geo = get_geolocation(incident_country, incident_city, incident_specific_location)
                latitude = generated_geo.get("latitude", "")
                longitude = generated_geo.get("longitude", "")

            labor_type = str(row.get("labor_type", "unknown"))
            if "LaborType" in labor_type:
                labor_type = labor_type.replace("LaborType.", "").lower()

            flattened_rows.append({
                "incidentId": incidentId,
                "publishedAt": publishedAt,
                "source": source,
                "sourceUrl": sourceUrl,
                "incident_country": incident_country,
                "incident_city": incident_city,
                "incident_specific_location": incident_specific_location,
                "victim_nationalities": victim_nationalities,
                "approximateNumberOfVictims": approximateNumberOfVictims,
                "demographicDetails": demographicDetails,
                "laborConditions": laborConditions,
                "labor_type": labor_type,
                "operator_nationality": operator_nationality,
                "organizationNames": organizationNames,
                "numberOfPerpetrators": numberOfPerpetrators,
                "scam_types": scam_types,
                "duration": duration,
                "scale": scale,
                "raidDetails": raidDetails,
                "arrestsMade": arrestsMade,
                "victimRescueOperations": victimRescueOperations,
                "incidentDescription": incidentDescription,
                "investigationStatus": investigationStatus,
                "dateScraped": dateScraped,
                "latitude": latitude,
                "longitude": longitude
            })

        return pd.DataFrame(flattened_rows)

    def generate_summary_stats(self) -> Dict:
        if self.df.empty:
            return {}
        total_incidents = len(self.df)
        total_victims = int(self.df["approximateNumberOfVictims"].sum()) if "approximateNumberOfVictims" in self.df.columns else 0
        return {"total_incidents": total_incidents, "total_victims": total_victims}

def convert_np_ints(obj):
    if isinstance(obj, dict):
        return {k: convert_np_ints(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_np_ints(item) for item in obj]
    elif isinstance(obj, np.integer):
        return int(obj)
    else:
        return obj

# -------------------------------
# MAIN EXECUTION
# -------------------------------
def main():
    """
    Main flow:
      1. For each month of 2024, query data from Perplexity.
      2. Transform the raw output using Claude.
      3. Append to a master DataFrame.
      4. After all months are processed, finalize and save the cleaned data and summary statistics.
    """
    # Replace these tokens with your actual API keys.
    perplexity_api_key = ""
    claude_api_key = ""

    # Create configs
    perplexity_config = Config(
        api_key=perplexity_api_key,
        model="sonar-reasoning-pro",
        api_url="https://api.perplexity.ai/chat/completions"
    )
    claude_config = Config(
        api_key=claude_api_key,
        model="claude-3-5-sonnet-20241022",
        api_url="https://api.anthropic.com/v1/messages"
    )

    perplexity_client = PerplexityClient(perplexity_config)
    claude_client = ClaudeClient(claude_config)

    # List of months for 2024
    months_2024 = [
        "January", "February", "March", "April",
        "May", "June", "July", "August",
        "September", "October", "November", "December"
    ]

    # This will hold all data from each month
    master_data = []

    # 1) Query + transform for each month
    for month in months_2024:
        logging.info(f"Processing data for {month} 2024...")

        # Step 1: Query Perplexity API for this month.
        perplexity_raw = perplexity_client.query_articles(month + " 2024")
        logging.debug("Number of tokens in perplexity output for %s 2024: %d", month, len(perplexity_raw.split()))

        if not perplexity_raw:
            logging.warning(f"No data retrieved for {month} 2024.")
            continue

        # Step 2: Transform data using Claude.
        transformed_data = claude_client.transform_data(perplexity_raw)
        if not transformed_data:
            logging.warning(f"No transformable data for {month} 2024.")
            continue

        # Append to our master list
        master_data.extend(transformed_data)

    # Convert master data into a DataFrame
    if not master_data:
        logging.error("No data retrieved from any month. Exiting.")
        return

    df = pd.DataFrame(master_data)
    logging.debug("Master DataFrame shape after all months: %s", df.shape)

    # 3) Process and flatten the data
    analyzer = ScamCenterAnalyzer(df)
    processed_df = analyzer.process_data()
    dashboard_df = analyzer.flatten_data()

    # If dateScraped is empty, fill with current date
    if "dateScraped" in dashboard_df.columns:
        dashboard_df["dateScraped"] = dashboard_df["dateScraped"].replace("", datetime.now().strftime("%Y-%m-%d"))

    # 4) Generate summary stats and save results
    summary_stats = analyzer.generate_summary_stats()
    summary_stats_modified = convert_np_ints(summary_stats)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_filename = f"clean_scam_center_data_{timestamp}.csv"
    dashboard_df.to_csv(csv_filename, index=False, encoding="utf-8")

    json_filename = f"dashboard_summary_{timestamp}.json"
    with open(json_filename, "w", encoding="utf-8") as f:
        json.dump(summary_stats_modified, f, indent=2, ensure_ascii=False)
    
    logging.info("Dashboard data saved to: %s", csv_filename)
    logging.info("Summary stats saved to: %s", json_filename)

if __name__ == "__main__":
    print('hi')
    main()
