#!/usr/bin/env python

import json
import requests
import os
import logging
import csv
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple
import argparse
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=os.environ.get('LOG_LEVEL', 'INFO'),
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Constants
TRANSPORT_API_ENDPOINT = "https://transport.data.gouv.fr/api/datasets"
TRANSITLAND_API_BASE = "https://transit.land/api/v2/rest"
TRANSITLAND_GRAPHQL_ENDPOINT = "https://transit.land/api/v2/query"
CSV_FILENAME = f'feed_processing_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'

FRENCH_FEEDS_QUERY = """
query {
  agencies(where: {adm0_iso: "FR"}) {
    feed_version {
      sha1
      feed {
        onestop_id
        spec
        urls {
          static_current
        }
      }
    }
  }
}
"""

class TransitlandAPI:
    def __init__(self):
        self.session = requests.Session()
        self.feed_cache = {}  # Cache feed details to avoid duplicate requests
        # Get API key from environment
        self.api_key = os.environ.get('TLV2')
        if not self.api_key:
            logging.warning("No Transitland API key found in TLV2 environment variable")
    
    def _make_request(self, endpoint: str, params: Dict = None) -> Dict:
        """Make an authenticated request to the Transitland API."""
        if params is None:
            params = {}
        if self.api_key:
            params['apikey'] = self.api_key
            
        url = f"{TRANSITLAND_API_BASE}/{endpoint}"
        response = self.session.get(url, params=params)
        response.raise_for_status()
        return response.json()
    
    def _make_graphql_request(self, query: str) -> Dict:
        """Make an authenticated GraphQL request to the Transitland API."""
        headers = {'Content-Type': 'application/json'}
        if self.api_key:
            headers['apikey'] = self.api_key
        
        response = self.session.post(
            TRANSITLAND_GRAPHQL_ENDPOINT,
            json={'query': query},
            headers=headers
        )
        response.raise_for_status()
        return response.json()
    
    def get_french_agencies(self) -> List[Dict]:
        """Get all transit agencies in France."""
        agencies = []
        after = None
        
        while True:
            params = {
                'adm0_iso': 'FR',
                'limit': 100  # Max allowed by API
            }
            if after:
                params['after'] = after
            
            try:
                data = self._make_request('agencies', params)
                
                if not data.get('agencies'):
                    break
                    
                agencies.extend(data['agencies'])
                logging.debug(f"Fetched {len(data['agencies'])} agencies")
                
                # Check if there are more pages
                meta = data.get('meta', {})
                after = meta.get('after')
                if not after:
                    break
                    
            except requests.exceptions.RequestException as e:
                logging.error(f"Error fetching agencies: {e}")
                break
        
        logging.info(f"Found {len(agencies)} French agencies in Transitland")
        return agencies
    
    def get_feed_details(self, feed_onestop_id: str) -> Optional[Dict]:
        """Get details for a specific feed."""
        # Check cache first
        if feed_onestop_id in self.feed_cache:
            return self.feed_cache[feed_onestop_id]
        
        try:
            data = self._make_request(f'feeds/{feed_onestop_id}')
            feed = data.get('feeds', [{}])[0]
            self.feed_cache[feed_onestop_id] = feed
            return feed
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logging.debug(f"Feed {feed_onestop_id} not found")
                return None
            raise

    def get_existing_french_feeds(self) -> Dict[str, Dict]:
        """Get all existing French feeds using GraphQL API."""
        try:
            data = self._make_graphql_request(FRENCH_FEEDS_QUERY)
            feeds = {}
            
            # Process the response to match the exact structure from the API
            for agency in data.get('data', {}).get('agencies', []):
                feed_version = agency.get('feed_version', {})
                if feed_version and feed_version.get('feed'):
                    feed = feed_version['feed']
                    feed_id = feed.get('onestop_id')
                    if feed_id and feed_id not in feeds:
                        # Store both feed info and version info
                        feeds[feed_id] = {
                            'feed': feed,
                            'feed_version': feed_version
                        }
                        logging.debug(f"Found feed {feed_id} with SHA1: {feed_version.get('sha1')}")
            
            logging.info(f"Found {len(feeds)} unique French feeds in Transitland")
            return feeds
            
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching French feeds: {e}")
            return {}

def fetch_datasets() -> List[Dict]:
    """Fetch all datasets from transport.data.gouv.fr API."""
    response = requests.get(TRANSPORT_API_ENDPOINT)
    response.raise_for_status()
    return response.json()

def filter_gtfs_datasets(datasets: List[Dict]) -> List[Dict]:
    """Filter datasets to only include static GTFS format."""
    filtered = []
    for dataset in datasets:
        # Find static GTFS resource (exclude GTFS-RT)
        static_gtfs = next(
            (r for r in dataset.get("resources", [])
             if r.get("format", "").upper() == "GTFS" 
             and not r.get("format", "").upper().endswith("-RT")),
            None
        )
        if static_gtfs:
            filtered.append(dataset)
        else:
            logging.debug(f"Skipping dataset {dataset.get('slug', 'unknown')}: no static GTFS resource")
    return filtered

def export_feed_details(new_feeds: List[Dict], existing_feeds: Dict[str, Dict]):
    """Export feed processing details to CSV for debugging."""
    headers = [
        'feed_id', 
        'status',  # new/existing/existing_only
        'name',
        'current_url',
        'previous_url',
        'url_changed',
        'feed_versions',
        'latest_version_date',
        'license_type',
        'languages',
        'fr_nap_dataset_id',  # Updated from source_dataset_id
        'transitland_url'
    ]
    
    rows = []
    
    # Process new feeds
    for feed in new_feeds:
        feed_id = feed['id']
        existing_feed = existing_feeds.get(feed_id)
        
        current_url = feed['urls'].get('static_current', '')
        previous_url = existing_feed.get('urls', {}).get('static_current', '') if existing_feed else None
        url_changed = current_url != previous_url if previous_url and current_url else 'N/A'
        
        # Get feed version info
        feed_versions = feed.get('feed_versions', [])
        latest_version_date = ''
        if feed_versions:
            latest_version = max(feed_versions, key=lambda v: v.get('fetched_at', ''))
            latest_version_date = latest_version.get('latest_calendar_date', '')
        
        rows.append({
            'feed_id': feed_id,
            'status': 'existing' if feed_id in existing_feeds else 'new',
            'name': feed.get('name', ''),
            'current_url': current_url,
            'previous_url': previous_url or '',
            'url_changed': url_changed,
            'feed_versions': len(feed_versions),
            'latest_version_date': latest_version_date,
            'license_type': feed.get('license', {}).get('spdx_identifier', 'Unknown'),
            'languages': ','.join(feed.get('languages', [])) if feed.get('languages') else '',
            'fr_nap_dataset_id': feed.get('tags', {}).get('fr_nap_dataset_id', ''),  # Updated from source_dataset_id
            'transitland_url': f"https://transit.land/feeds/{feed_id}" if feed_id in existing_feeds else ''
        })
    
    # Add existing feeds that weren't in new feeds
    for feed_id, feed_info in existing_feeds.items():
        if not any(feed['id'] == feed_id for feed in new_feeds):
            static_url = feed_info.get('urls', {}).get('static_current', '')
            # Only include feeds that have a static GTFS URL
            if static_url:
                feed_versions = feed_info.get('feed_versions', [])
                latest_version_date = ''
                if feed_versions:
                    latest_version = max(feed_versions, key=lambda v: v.get('fetched_at', ''))
                    latest_version_date = latest_version.get('latest_calendar_date', '')
                
                rows.append({
                    'feed_id': feed_id,
                    'status': 'existing_only',
                    'name': feed_info.get('name', ''),
                    'current_url': static_url,
                    'previous_url': '',
                    'url_changed': 'N/A',
                    'feed_versions': len(feed_versions),
                    'latest_version_date': latest_version_date,
                    'license_type': feed_info.get('license', {}).get('spdx_identifier', 'Unknown'),
                    'languages': ','.join(feed_info.get('languages', [])) if feed_info.get('languages') else '',
                    'fr_nap_dataset_id': feed_info.get('tags', {}).get('fr_nap_dataset_id', ''),  # Updated from source_dataset_id
                    'transitland_url': f"https://transit.land/feeds/{feed_id}"
                })
            else:
                logging.debug(f"Skipping existing feed {feed_id}: no static GTFS URL")
    
    # Sort rows by feed_id for consistency
    rows.sort(key=lambda x: x['feed_id'])
    
    # Write to CSV
    with open(CSV_FILENAME, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)
    
    logging.info(f"Exported feed details to {CSV_FILENAME}")
    logging.debug(f"CSV contains {len(rows)} rows:")
    logging.debug(f"  New feeds: {sum(1 for r in rows if r['status'] == 'new')}")
    logging.debug(f"  Updated feeds: {sum(1 for r in rows if r['status'] == 'existing')}")
    logging.debug(f"  Existing only: {sum(1 for r in rows if r['status'] == 'existing_only')}")

def clean_empty_values(obj):
    """Recursively remove empty values from a dictionary or list."""
    if isinstance(obj, dict):
        return {
            k: clean_empty_values(v)
            for k, v in obj.items()
            if v not in (None, "", [], {}, "unknown")
        }
    elif isinstance(obj, list):
        return [
            clean_empty_values(item)
            for item in obj
            if item not in (None, "", [], {})
        ]
    return obj

def create_feed_record(dataset: Dict, existing_feeds: Dict[str, Dict]) -> Optional[Dict]:
    """Create a DMFR feed record from a dataset."""
    # Find the static GTFS resource (exclude GTFS-RT)
    gtfs_resource = next(
        (r for r in dataset.get("resources", [])
         if r.get("format", "").upper() == "GTFS"
         and not r.get("format", "").upper().endswith("-RT")),
        None
    )
    
    if not gtfs_resource:
        logging.warning(f"Dataset {dataset['slug']} has no static GTFS resource")
        return None

    # Create feed ID from slug using consistent format
    original_slug = dataset['slug'].lower()
    
    # Handle special characters and create name component:
    # 1. Replace hyphens and underscores with tildes
    # 2. Keep alphanumeric and accented chars
    # 3. Add ~fr suffix for French feeds if not present
    name_component = original_slug.replace('-', '~').replace('_', '~')
    
    # Remove duplicate tildes and any leading/trailing tildes
    while '~~' in name_component:
        name_component = name_component.replace('~~', '~')
    name_component = name_component.strip('~')
    
    # Add ~fr suffix if not already present and not a special case
    if not any(name_component.endswith(suffix) for suffix in ['~fr', '~france', '~reunion']):
        name_component = f"{name_component}~fr"
    
    # Create the feed ID
    feed_id = f"f-{name_component}"
    
    logging.debug(f"Processing dataset:")
    logging.debug(f"  Original slug: {original_slug}")
    logging.debug(f"  Name component: {name_component}")
    logging.debug(f"  Feed ID: {feed_id}")
    logging.debug(f"  GTFS URL: {gtfs_resource['url']}")
    
    # Create basic feed record matching Transitland structure
    feed_record = {
        "id": feed_id,
        "spec": "gtfs",  # Transitland uses lowercase gtfs
        "urls": {
            "static_current": gtfs_resource["url"]
        },
        "license": {},
        "name": dataset.get("title"),
        "tags": {
            "fr_nap_dataset_id": dataset["id"]
        }
    }

    # Add optional URL fields only if they have values
    realtime_urls = {
        "static_planned": [],
        "static_historic": [],
        "realtime_alerts": "",
        "realtime_trip_updates": "",
        "realtime_vehicle_positions": "",
        "gbfs_auto_discovery": "",
        "mds_provider": ""
    }
    feed_record["urls"].update({k: v for k, v in realtime_urls.items() if v})

    # Add license URL if available
    if dataset.get("page_url"):
        feed_record["license"]["url"] = dataset["page_url"]

    # Add license identifier if available
    if dataset.get("licence"):
        logging.debug(f"  License: {dataset['licence']}")
        # Map transport.data.gouv.fr license keys to SPDX identifiers
        # See https://spdx.org/licenses/ for the full list
        license_map = {
            # Open Data Commons licenses
            "odc-odbl": "ODbL-1.0",  # Open Data Commons Open Database License v1.0
            
            # French government licenses
            "lov2": "LO-2.0",        # French Open License 2.0 (Licence Ouverte)
            "fr-lo": "LO-2.0",       # Alternate name for French Open License
            "licence-ouverte": "LO-2.0",  # Another alternate name
            
            # Public domain and other open licenses
            "other-pd": "CC0-1.0",   # Public Domain dedication maps to CC0
            "mobility-licence": "LO-2.0",  # Mobility license is typically French Open License
            
            # Fallback for unspecified
            "notspecified": None     # No SPDX identifier for unspecified
        }
        
        license_key = dataset["licence"].lower().strip()
        if license_key in license_map:
            spdx_id = license_map[license_key]
            if spdx_id:  # Only set if we have a valid SPDX identifier
                feed_record["license"]["spdx_identifier"] = spdx_id
                
                # Set additional license properties based on the type
                if license_key == "odc-odbl":
                    feed_record["license"].update({
                        "share_alike_optional": "no",
                        "attribution_text": "© OpenStreetMap contributors",
                        "redistribution_allowed": "yes",
                        "commercial_use_allowed": "yes",
                        "create_derived_product": "yes"
                    })
                elif license_key in ["lov2", "fr-lo", "licence-ouverte", "mobility-licence"]:
                    feed_record["license"].update({
                        "attribution_text": "Licence Ouverte / Open License",
                        "redistribution_allowed": "yes",
                        "commercial_use_allowed": "yes",
                        "create_derived_product": "yes",
                        "share_alike_optional": "yes"
                    })
                elif license_key == "other-pd":
                    feed_record["license"].update({
                        "attribution_text": "",  # No attribution needed for public domain
                        "redistribution_allowed": "yes",
                        "commercial_use_allowed": "yes",
                        "create_derived_product": "yes",
                        "share_alike_optional": "yes"
                    })
                
                logging.debug(f"Added license info for {feed_id}: {spdx_id}")
            else:
                logging.warning(f"No SPDX identifier available for license: {license_key}")
                # Set reasonable defaults for unknown/unspecified licenses
                feed_record["license"].update({
                    "attribution_text": dataset.get("title", ""),
                    "redistribution_allowed": "unknown",
                    "commercial_use_allowed": "unknown",
                    "create_derived_product": "unknown",
                    "share_alike_optional": "unknown"
                })
        else:
            logging.warning(f"Unknown license type for {feed_id}: {license_key}")
            # Set reasonable defaults for unknown licenses
            feed_record["license"].update({
                "attribution_text": dataset.get("title", ""),
                "redistribution_allowed": "unknown",
                "commercial_use_allowed": "unknown",
                "create_derived_product": "unknown",
                "share_alike_optional": "unknown"
            })

    # Check if there's any existing feed info we should preserve
    existing_feed = existing_feeds.get(feed_id)
    if existing_feed:
        logging.info(f"Found existing feed {feed_id} in Transitland - will preserve metadata")
        logging.debug("Existing feed details:")
        logging.debug(json.dumps(existing_feed, indent=2, ensure_ascii=False))
        
        # Compare URLs to detect changes
        existing_url = existing_feed.get('urls', {}).get('static_current')
        if existing_url and existing_url != gtfs_resource["url"]:
            logging.info(f"URL changed for {feed_id}:")
            logging.info(f"  Old URL: {existing_url}")
            logging.info(f"  New URL: {gtfs_resource['url']}")
            
            # Check if new URL is a data.gouv.fr URL
            if not any(domain in gtfs_resource["url"] for domain in ['data.gouv.fr', '.fr/gtfs']):
                logging.warning(f"New URL for {feed_id} is not from a French domain: {gtfs_resource['url']}")
        
        # Preserve feed versions if any
        if existing_feed.get('feed_versions'):
            feed_record["feed_versions"] = existing_feed["feed_versions"]
            logging.debug(f"Preserved {len(existing_feed['feed_versions'])} feed versions for {feed_id}")
        
        # Preserve feed state if any
        if existing_feed.get('feed_state'):
            feed_record["feed_state"] = existing_feed["feed_state"]
            logging.debug(f"Preserved feed state for {feed_id}")
        
        # Preserve existing name if we don't have a new one
        if not feed_record.get("name") and existing_feed.get("name"):
            feed_record["name"] = existing_feed["name"]
            logging.debug(f"Preserved existing name for {feed_id}: {existing_feed['name']}")
        
        # Preserve existing license if it's more specific
        existing_license = existing_feed.get('license', {})
        if existing_license.get('spdx_identifier') and not feed_record['license'].get('spdx_identifier'):
            feed_record['license'] = existing_license
            logging.debug(f"Preserved existing license for {feed_id}: {existing_license.get('spdx_identifier')}")
        
        # Only add languages if they exist
        if existing_feed and existing_feed.get('languages'):
            feed_record["languages"] = existing_feed["languages"]
            logging.debug(f"Preserved languages for {feed_id}: {existing_feed['languages']}")

    # Preserve existing tags if any
    existing_feed = existing_feeds.get(feed_id)
    if existing_feed and existing_feed.get('tags'):
        # Update tags dict with existing tags, preserving fr_nap_dataset_id
        existing_tags = existing_feed['tags'].copy()
        existing_tags.update(feed_record['tags'])
        feed_record['tags'] = existing_tags
        logging.debug(f"Preserved existing tags for {feed_id}")

    # Clean empty values before returning
    return clean_empty_values(feed_record)

def write_dmfr_file(new_feeds: List[Dict], existing_feeds: Dict[str, Dict]):
    """Write feed records to transport.data.gouv.fr.dmfr.json."""
    dmfr_path = Path('./feeds/transport.data.gouv.fr.dmfr.json')
    
    # Create feeds directory if it doesn't exist
    dmfr_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Filter out feeds that have matching SHA1 hashes in Transitland
    feeds_to_write = []
    skipped_feeds = []
    
    for feed in new_feeds:
        feed_id = feed['id']
        existing_feed_data = existing_feeds.get(feed_id)
        
        if existing_feed_data:
            feed_version = existing_feed_data.get('feed_version', {})
            feed_info = existing_feed_data.get('feed', {})
            
            if feed_version and feed_version.get('sha1'):
                # We have a SHA1 to compare against
                try:
                    # Download the current feed content and compute SHA1
                    current_url = feed['urls'].get('static_current')
                    if current_url:
                        response = requests.head(current_url, allow_redirects=True)
                        
                        # Skip if URL is not accessible
                        if response.status_code != 200:
                            logging.warning(f"Could not access URL for {feed_id}: {current_url}")
                            feeds_to_write.append(feed)
                            continue
                        
                        # Compare content length as quick check before downloading
                        content_length = response.headers.get('content-length')
                        if content_length and int(content_length) == feed_version.get('size_bytes'):
                            # Sizes match, likely same content - skip this feed
                            skipped_feeds.append({
                                'feed_id': feed_id,
                                'reason': 'size_match',
                                'url': current_url,
                                'existing_sha1': feed_version.get('sha1')
                            })
                            continue
                        
                        # If sizes don't match or no size info, keep the feed
                        feeds_to_write.append(feed)
                    else:
                        # No URL to check, keep the feed
                        feeds_to_write.append(feed)
                        
                except Exception as e:
                    logging.error(f"Error checking feed {feed_id}: {e}")
                    # On error, keep the feed to be safe
                    feeds_to_write.append(feed)
            else:
                # No SHA1 to compare against, keep the feed
                feeds_to_write.append(feed)
        else:
            # No existing feed data, keep the feed
            feeds_to_write.append(feed)
    
    # Log skipped feeds
    if skipped_feeds:
        logging.info(f"Skipped {len(skipped_feeds)} feeds with matching content:")
        for skip in skipped_feeds:
            logging.debug(f"  {skip['feed_id']}: {skip['reason']}")
        
        # Export skipped feeds to CSV
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        with open(f'skipped_feeds_{timestamp}.csv', 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['feed_id', 'reason', 'url', 'existing_sha1'])
            writer.writeheader()
            writer.writerows(skipped_feeds)
    
    # Read existing file if it exists
    if dmfr_path.exists():
        try:
            with open(dmfr_path, 'r', encoding='utf-8') as f:
                dmfr_data = json.load(f)
                
            # Get feed IDs we're going to write
            new_feed_ids = {feed['id'] for feed in feeds_to_write}
            
            # Preserve feeds that aren't in new feeds
            preserved_feeds = [
                feed for feed in dmfr_data.get('feeds', [])
                if feed.get('id') not in new_feed_ids
            ]
            
            # Combine preserved feeds with new feeds
            feeds = preserved_feeds + feeds_to_write
            
        except (json.JSONDecodeError, FileNotFoundError) as e:
            logging.error(f"Error reading {dmfr_path}: {e}")
            feeds = feeds_to_write
    else:
        feeds = feeds_to_write
    
    # Write updated DMFR file
    try:
        # Clean empty values from all feeds
        cleaned_feeds = [clean_empty_values(feed) for feed in feeds]
        dmfr_data = {"feeds": cleaned_feeds}
        
        with open(dmfr_path, 'w', encoding='utf-8') as f:
            json.dump(dmfr_data, f, indent=2, ensure_ascii=False)
        
        logging.info(f"Updated {dmfr_path} with {len(feeds)} feeds")
        logging.debug(f"  New/updated feeds: {len(feeds_to_write)}")
        logging.debug(f"  Preserved feeds: {len(feeds) - len(feeds_to_write)}")
        logging.debug(f"  Skipped feeds: {len(skipped_feeds)}")
    except IOError as e:
        logging.error(f"Error writing {dmfr_path}: {e}")

def sync_and_fetch_feeds():
    """Sync DMFR file to SQLite and fetch feeds."""
    # Use absolute paths for everything
    workspace_root = Path('./').absolute()
    dmfr_path = workspace_root / 'feeds' / 'transport.data.gouv.fr.dmfr.json'
    db_path = workspace_root / 'data' / 'transitland.db'
    gtfs_path = workspace_root / 'data' / 'gtfs'
    
    # Create all necessary directories
    for path in [db_path.parent, gtfs_path]:
        path.mkdir(parents=True, exist_ok=True)
    
    # Log paths for debugging
    logging.info("Using the following paths:")
    logging.info(f"  Workspace: {workspace_root}")
    logging.info(f"  Database: {db_path}")
    logging.info(f"  GTFS files: {gtfs_path}")
    logging.info(f"  DMFR file: {dmfr_path}")
    
    # Verify DMFR file exists and has valid content
    if not dmfr_path.exists():
        logging.error(f"DMFR file not found: {dmfr_path}")
        return None
        
    try:
        with open(dmfr_path, 'r') as f:
            dmfr_data = json.load(f)
            feed_count = len(dmfr_data.get('feeds', []))
            logging.info(f"Found {feed_count} feeds in DMFR file")
            if feed_count == 0:
                logging.error("No feeds found in DMFR file")
                return None
    except Exception as e:
        logging.error(f"Error reading DMFR file: {e}")
        return None
    
    # First, sync the DMFR file to create/update the database
    logging.info("Syncing DMFR file to database...")
    sync_cmd = f"transitland sync --dburl=sqlite3://{db_path} {dmfr_path}"
    
    try:
        logging.debug(f"Running: {sync_cmd}")
        result = os.system(sync_cmd)
        if result != 0:
            logging.error(f"Error syncing DMFR file, exit code: {result}")
            # Don't return None - let's check if we can still use the database
    except Exception as e:
        logging.error(f"Error syncing DMFR file: {e}")
        # Don't return None - let's check if we can still use the database

    # Verify the database state
    try:
        import sqlite3
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Check what tables we actually have
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {row[0] for row in cursor.fetchall()}
        logging.info("Found tables in database:")
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            logging.info(f"  {table}: {count} rows")
        
        # Check if we have the minimum required tables
        required_tables = {'current_feeds', 'feed_versions'}
        missing_tables = required_tables - tables
        if missing_tables:
            logging.error(f"Missing required tables: {missing_tables}")
            conn.close()
            return None
            
        # Check if we have feeds
        if 'current_feeds' in tables:
            cursor.execute("SELECT COUNT(*) FROM current_feeds")
            feed_count = cursor.fetchone()[0]
            logging.info(f"Found {feed_count} feeds in current_feeds table")
            
            if feed_count == 0:
                logging.error("No feeds found in database")
                conn.close()
                return None
        
        conn.close()
    except Exception as e:
        logging.error(f"Error verifying database: {e}")
        return None

    # Now fetch the feeds
    logging.info("Fetching feeds...")
    fetch_cmd = (
        f"transitland fetch "
        f"--dburl=sqlite3://{db_path} "    # SQLite database URL
        f"--storage {gtfs_path} "         # Store GTFS files here
        f"--workers 1 "                   # Single worker to avoid database locks
        f"--create-feed "                 # Create feed record if not found
        f"--allow-local-fetch "           # Allow fetching from local files
        f"--allow-ftp-fetch "            # Allow fetching from FTP
        f"--allow-s3-fetch "             # Allow fetching from S3
    )
    
    try:
        logging.debug(f"Running: {fetch_cmd}")
        result = os.system(fetch_cmd)
        if result != 0:
            logging.error(f"Error fetching feeds, exit code: {result}")
            # Don't return None - let's check if we got any feed versions
    except Exception as e:
        logging.error(f"Error fetching feeds: {e}")
        # Don't return None - let's check if we got any feed versions

    # Check if we got any feed versions
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get summary of feeds and their versions
        cursor.execute("""
            SELECT 
                cf.onestop_id,
                COUNT(fv.id) as version_count,
                MAX(fv.fetched_at) as latest_fetch,
                MAX(fv.sha1) as latest_sha1
            FROM current_feeds cf
            LEFT JOIN feed_versions fv ON cf.id = fv.feed_id
            GROUP BY cf.onestop_id
            HAVING version_count > 0
        """)
        
        feed_summary = cursor.fetchall()
        total_versions = sum(row[1] for row in feed_summary)
        
        logging.info("\nFeed version summary:")
        logging.info(f"Total feed versions: {total_versions}")
        
        if total_versions > 0:
            # Log some example feeds with versions
            logging.info("Example feeds with versions:")
            for onestop_id, version_count, latest_fetch, latest_sha1 in feed_summary[:5]:
                if version_count > 0:
                    logging.info(f"  {onestop_id}:")
                    logging.info(f"    Versions: {version_count}")
                    logging.info(f"    Latest fetch: {latest_fetch}")
                    logging.info(f"    Latest SHA1: {latest_sha1}")
            
        conn.close()
        
        # Print helpful debug message
        logging.info("\nDatabase is ready for debugging!")
        logging.info(f"To query the database directly, run:")
        logging.info(f"  sqlite3 {db_path}")
        logging.info("Available tables:")
        for table in tables:
            logging.info(f"  - {table}")
        logging.info("Example queries:")
        logging.info("  SELECT onestop_id, name FROM current_feeds;")
        logging.info("  SELECT cf.onestop_id, fv.sha1, fv.fetched_at FROM current_feeds cf JOIN feed_versions fv ON cf.id = fv.feed_id;")
        
        # Only return the database path if we have some feed versions
        if total_versions > 0:
            return db_path
        else:
            logging.error("No feed versions found in database")
            return None
        
    except Exception as e:
        logging.error(f"Error checking feed versions: {e}")
        return None

def check_feed_versions(db_path: Path, existing_feeds: Dict[str, Dict]):
    """Check feed versions against existing SHA1 hashes."""
    import sqlite3
    
    duplicates = []
    new_versions = []
    
    # Connect to the database
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get all feed versions and their SHA1 hashes from the database
        cursor.execute("""
            SELECT 
                cf.onestop_id,
                fv.sha1,
                fv.url,
                fv.size_bytes
            FROM feed_versions fv
            JOIN current_feeds cf ON cf.id = fv.feed_id
            ORDER BY fv.fetched_at DESC
        """)
        
        db_results = cursor.fetchall()
        
        # Compare database SHA1s with Transitland API SHA1s
        for onestop_id, db_sha1, url, size_bytes in db_results:
            feed_data = existing_feeds.get(onestop_id)
            if feed_data:
                feed_version = feed_data.get('feed_version', {})
                api_sha1 = feed_version.get('sha1')
                
                if api_sha1:
                    if api_sha1 == db_sha1:
                        # Found a match - this feed hasn't changed
                        duplicates.append({
                            'feed_id': onestop_id,
                            'sha1': db_sha1,
                            'url': url,
                            'size_bytes': size_bytes,
                            'status': 'match'
                        })
                    else:
                        # SHA1s don't match - feed has changed
                        new_versions.append({
                            'feed_id': onestop_id,
                            'old_sha1': api_sha1,
                            'new_sha1': db_sha1,
                            'url': url,
                            'size_bytes': size_bytes,
                            'status': 'changed'
                        })
                else:
                    # No SHA1 in API - treat as new
                    new_versions.append({
                        'feed_id': onestop_id,
                        'sha1': db_sha1,
                        'url': url,
                        'size_bytes': size_bytes,
                        'status': 'new'
                    })
            else:
                # Feed not in API - treat as new
                new_versions.append({
                    'feed_id': onestop_id,
                    'sha1': db_sha1,
                    'url': url,
                    'size_bytes': size_bytes,
                    'status': 'new'
                })
        
        conn.close()
        
        # Log results
        if duplicates:
            logging.info(f"Found {len(duplicates)} unchanged feeds:")
            for dup in duplicates[:5]:  # Show first 5 as examples
                logging.info(f"  {dup['feed_id']}: {dup['sha1']}")
                
        if new_versions:
            logging.info(f"Found {len(new_versions)} new/changed feeds:")
            for new in new_versions[:5]:  # Show first 5 as examples
                if new['status'] == 'changed':
                    logging.info(f"  {new['feed_id']}: {new['old_sha1']} -> {new['new_sha1']}")
                else:
                    logging.info(f"  {new['feed_id']}: {new['sha1']} (new)")
        
        # Export results to CSV
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Export duplicates
        if duplicates:
            dup_file = f'unchanged_feeds_{timestamp}.csv'
            with open(dup_file, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=['feed_id', 'sha1', 'url', 'size_bytes', 'status'])
                writer.writeheader()
                writer.writerows(duplicates)
            logging.info(f"Exported {len(duplicates)} unchanged feeds to {dup_file}")
        
        # Export new versions
        if new_versions:
            new_file = f'changed_feeds_{timestamp}.csv'
            with open(new_file, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=['feed_id', 'old_sha1', 'new_sha1', 'url', 'size_bytes', 'status'])
                writer.writeheader()
                writer.writerows(new_versions)
            logging.info(f"Exported {len(new_versions)} new/changed feeds to {new_file}")
        
        return duplicates, new_versions
        
    except sqlite3.Error as e:
        logging.error(f"Database error: {e}")
        logging.error("SQL query that failed:")
        logging.error("""
            SELECT 
                cf.onestop_id,
                fv.sha1,
                fv.url,
                fv.size_bytes
            FROM feed_versions fv
            JOIN current_feeds cf ON cf.id = fv.feed_id
            ORDER BY fv.fetched_at DESC
        """)
        return [], []
    except Exception as e:
        logging.error(f"Error checking feed versions: {e}")
        return [], []

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Scrape French NAP datasets and compare with Transitland.')
    args = parser.parse_args()

    # Initialize Transitland API client
    transitland = TransitlandAPI()
    
    # Get existing French feeds from Transitland
    existing_feeds = transitland.get_existing_french_feeds()
    
    # Fetch and process datasets from transport.data.gouv.fr
    logging.info("Fetching datasets from transport.data.gouv.fr...")
    datasets = fetch_datasets()
    
    logging.info(f"Found {len(datasets)} total datasets")
    gtfs_datasets = filter_gtfs_datasets(datasets)
    logging.info(f"Filtered to {len(gtfs_datasets)} GTFS datasets")
    
    # Create feed records
    new_feeds = []
    for dataset in gtfs_datasets:
        feed_record = create_feed_record(dataset, existing_feeds)
        if feed_record:
            new_feeds.append(feed_record)
    
    logging.info(f"Created {len(new_feeds)} feed records")
    
    # Export debug info to CSV
    export_feed_details(new_feeds, existing_feeds)
    
    # Write feed records to DMFR file
    write_dmfr_file(new_feeds, existing_feeds)
    
    # Only proceed with sync and fetch if we have feeds to process
    if new_feeds:
        # Sync DMFR file and fetch feeds
        db_path = sync_and_fetch_feeds()
        if db_path:
            # Check feed versions against existing hashes
            duplicates, new_versions = check_feed_versions(db_path, existing_feeds)
            
            # Log summary
            logging.info("\nFeed Version Summary:")
            logging.info(f"  Total feeds processed: {len(new_feeds)}")
            logging.info(f"  Unchanged feeds: {len(duplicates)}")
            logging.info(f"  New/changed feeds: {len(new_versions)}")
            
            # Remove unchanged feeds from DMFR file
            if duplicates:
                logging.info("Removing unchanged feeds from DMFR file...")
                unchanged_ids = {d['feed_id'] for d in duplicates}
                write_dmfr_file([f for f in new_feeds if f['id'] not in unchanged_ids], existing_feeds)
        else:
            logging.error("Failed to sync/fetch feeds - skipping version check")
    else:
        logging.info("No new feeds to process - skipping sync/fetch")

if __name__ == "__main__":
    main()
