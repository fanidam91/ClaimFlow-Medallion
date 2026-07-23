import os
import csv

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
POLICIES_CSV = os.path.join(DATA_DIR, "reference_policies.csv")

DEFAULT_POLICIES = [
    {
        "reference_no": "POL-101",
        "policyholder": "John Smith",
        "policy_type": "Auto Insurance",
        "policy_status": "Active",
        "coverage_limit": 5000.0,
        "effective_date": "2025-01-01"
    },
    {
        "reference_no": "POL-102",
        "policyholder": "Sarah Connor",
        "policy_type": "Home Insurance",
        "policy_status": "Active",
        "coverage_limit": 10000.0,
        "effective_date": "2024-06-15"
    },
    {
        "reference_no": "POL-103",
        "policyholder": "Alice Johnson",
        "policy_type": "Health Insurance",
        "policy_status": "Expired",
        "coverage_limit": 25000.0,
        "effective_date": "2023-01-01"
    },
    {
        "reference_no": "POL-104",
        "policyholder": "Tony Stark",
        "policy_type": "Commercial Liability",
        "policy_status": "Active",
        "coverage_limit": 500000.0,
        "effective_date": "2025-01-01"
    }
]

def init_db():
    """Ensure data directory and default policies CSV file exist."""
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
    
    # Create bronze, silver, gold, review, raw directories
    for zone in ["bronze", "silver", "gold", "raw", "review"]:
        zone_dir = os.path.join(DATA_DIR, zone)
        if not os.path.exists(zone_dir):
            os.makedirs(zone_dir)

    if not os.path.exists(POLICIES_CSV):
        save_policies(DEFAULT_POLICIES)

def load_policies():
    """Load the policies from the CSV file."""
    init_db()
    if not os.path.exists(POLICIES_CSV):
        return DEFAULT_POLICIES
        
    policies = []
    try:
        with open(POLICIES_CSV, mode="r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Cast numeric fields
                row["coverage_limit"] = float(row.get("coverage_limit", 0.0) or 0.0)
                policies.append(row)
        return policies
    except Exception as e:
        print(f"Error loading policies CSV: {e}")
        return DEFAULT_POLICIES

def save_policies(policies):
    """Save the policies list to the CSV file."""
    os.makedirs(DATA_DIR, exist_ok=True)
    fieldnames = ["reference_no", "policyholder", "policy_type", "policy_status", "coverage_limit", "effective_date"]
    try:
        with open(POLICIES_CSV, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for p in policies:
                writer.writerow(p)
    except Exception as e:
        print(f"Error saving policies CSV: {e}")

def get_policy(reference_no):
    """Fetch a policy by its reference number (case-insensitive)."""
    policies = load_policies()
    for policy in policies:
        if policy["reference_no"].strip().upper() == reference_no.strip().upper():
            return policy
    return None

def upsert_policy(policy_data):
    """Insert or update a policy."""
    policies = load_policies()
    ref_no = policy_data.get("reference_no", "").strip().upper()
    if not ref_no:
        return False
    
    new_policy = {
        "reference_no": ref_no,
        "policyholder": policy_data.get("policyholder", "Unknown").strip(),
        "policy_type": policy_data.get("policy_type", "General").strip(),
        "policy_status": policy_data.get("policy_status", "Active").strip(),
        "coverage_limit": float(policy_data.get("coverage_limit", 0.0)),
        "effective_date": policy_data.get("effective_date", "2026-01-01").strip()
    }
    
    updated = False
    for i, p in enumerate(policies):
        if p["reference_no"].strip().upper() == ref_no:
            policies[i] = new_policy
            updated = True
            break
            
    if not updated:
        policies.append(new_policy)
        
    save_policies(policies)
    return True
