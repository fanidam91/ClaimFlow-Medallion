import os
import json
import shutil
import hashlib
from datetime import datetime
from app.parser import parse_pdf_claim
from app.database import get_policy, load_policies

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(PROJECT_DIR, "data")
BRONZE_DIR = os.path.join(DATA_DIR, "bronze")
SILVER_DIR = os.path.join(DATA_DIR, "silver")
GOLD_DIR = os.path.join(DATA_DIR, "gold")
REVIEW_DIR = os.path.join(DATA_DIR, "review")

def calculate_surrogate_key(parsed_fields):
    """Calculates a unique SHA-256 surrogate key from claim details."""
    ref = str(parsed_fields.get("reference_no") or "").strip().upper()
    name = str(parsed_fields.get("claimant_name") or "").strip().lower()
    date = str(parsed_fields.get("claim_date") or "").strip()
    amount = str(parsed_fields.get("claim_amount") or "").strip()
    
    hash_input = f"{ref}||{name}||{date}||{amount}"
    return hashlib.sha256(hash_input.encode("utf-8")).hexdigest()

def is_duplicate_claim(surrogate_key):
    """Checks if a claim with the same surrogate key has already been processed."""
    if not os.path.exists(SILVER_DIR):
        return False
        
    for filename in os.listdir(SILVER_DIR):
        if filename.endswith(".json"):
            filepath = os.path.join(SILVER_DIR, filename)
            try:
                with open(filepath, "r") as f:
                    data = json.load(f)
                    if data.get("surrogate_key") == surrogate_key:
                        return True
            except Exception:
                continue
    return False

def run_medallion_pipeline(pdf_path, original_filename=None):
    """
    Simulates the Medallion Architecture locally:
    1. Bronze: Raw PDF parsing and text storage.
    2. Silver: Cleaning, field validation, and surrogate key calculation.
    3. Gold: Policy verification, mapping, and duplicate quarantine.
    """
    # Ensure dirs exist
    for d in [BRONZE_DIR, SILVER_DIR, GOLD_DIR, REVIEW_DIR]:
        os.makedirs(d, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    if not original_filename:
        original_filename = os.path.basename(pdf_path)

    # 1. BRONZE LAYER: Extract and ingest raw data
    try:
        raw_extraction = parse_pdf_claim(pdf_path)
        raw_text = raw_extraction["raw_text"]
        parsed_fields = raw_extraction["parsed_fields"]
    except Exception as e:
        return {
            "success": False,
            "error": f"Bronze layer extraction failed: {str(e)}"
        }

    bronze_data = {
        "filename": original_filename,
        "ingested_time": datetime.now().isoformat(),
        "raw_text": raw_text
    }
    bronze_filename = f"claim_raw_{timestamp}.json"
    bronze_filepath = os.path.join(BRONZE_DIR, bronze_filename)
    with open(bronze_filepath, "w") as f:
        json.dump(bronze_data, f, indent=4)

    # 2. SILVER LAYER: Cleanse, structure, and calculate surrogate key
    # Standardize claimant name (Title Case)
    claimant_name = parsed_fields.get("claimant_name")
    if claimant_name:
        claimant_name = claimant_name.strip().title()
        
    # Standardize reference number
    ref_no = parsed_fields.get("reference_no")
    if ref_no:
        ref_no = ref_no.strip().upper()
        
    # Calculate surrogate key based on parsed details
    surrogate_key = calculate_surrogate_key({
        "reference_no": ref_no,
        "claimant_name": claimant_name,
        "claim_date": parsed_fields.get("claim_date"),
        "claim_amount": parsed_fields.get("claim_amount")
    })

    # Check for duplicates before moving forward
    duplicate_flag = is_duplicate_claim(surrogate_key)

    silver_data = {
        "surrogate_key": surrogate_key,
        "reference_no": ref_no,
        "claimant_name": claimant_name,
        "claim_date": parsed_fields.get("claim_date"),
        "claim_amount": parsed_fields.get("claim_amount"),
        "claim_reason": parsed_fields.get("claim_reason"),
        "ingested_time": bronze_data["ingested_time"],
        "bronze_source_file": bronze_filename
    }

    silver_filename = f"claim_clean_{timestamp}.json"
    silver_filepath = os.path.join(SILVER_DIR, silver_filename)
    with open(silver_filepath, "w") as f:
        json.dump(silver_data, f, indent=4)

    # 3. GOLD LAYER: Match, map side-by-side, and check validation rules
    policy = None
    if ref_no:
        policy = get_policy(ref_no)

    # Initial checks
    is_policy_found = policy is not None
    is_name_matched = False
    is_policy_active = False
    is_limit_exceeded = False
    amount_difference = 0.0

    if is_policy_found:
        policyholder = policy["policyholder"]
        is_name_matched = claimant_name.lower().strip() == policyholder.lower().strip()
        is_policy_active = policy["policy_status"].lower().strip() == "active"
        
        limit = float(policy["coverage_limit"])
        claim_amt = float(silver_data["claim_amount"] or 0.0)
        is_limit_exceeded = claim_amt > limit
        amount_difference = claim_amt - limit

    # Reconciled status determination
    if duplicate_flag:
        claim_status = "FLAGGED - DUPLICATE (UNDER REVIEW)"
        rejection_reason = "This claim has identical details to an already processed claim."
        # Quarantine PDF to the review folder
        review_pdf_path = os.path.join(REVIEW_DIR, f"DUPLICATE_{timestamp}_{original_filename}")
        shutil.copy2(pdf_path, review_pdf_path)
    elif not is_policy_found:
        claim_status = "REJECTED"
        rejection_reason = f"Policy reference '{ref_no}' was not found in the reference policies database."
    elif not is_policy_active:
        claim_status = "REJECTED"
        rejection_reason = f"Policy '{ref_no}' is expired or inactive (Status: {policy['policy_status']})."
    elif not is_name_matched:
        claim_status = "REJECTED"
        rejection_reason = f"Claimant name '{claimant_name}' does not match policyholder name '{policy['policyholder']}'."
    elif is_limit_exceeded:
        claim_status = "REJECTED"
        rejection_reason = f"Claim amount ${silver_data['claim_amount']:,.2f} exceeds policy limit of ${policy['coverage_limit']:,.2f} (Diff: ${amount_difference:,.2f})."
    else:
        claim_status = "APPROVED"
        rejection_reason = "Verification check successful. All criteria matched."

    gold_data = {
        "surrogate_key": surrogate_key,
        "reference_no": ref_no,
        
        # Mapped Side-by-Side Fields
        "claimant_name": claimant_name,
        "claim_amount": silver_data["claim_amount"],
        "claim_date": silver_data["claim_date"],
        "claim_reason": silver_data["claim_reason"],
        
        "policyholder_db": policy["policyholder"] if is_policy_found else None,
        "policy_type_db": policy["policy_type"] if is_policy_found else None,
        "policy_status_db": policy["policy_status"] if is_policy_found else None,
        "coverage_limit_db": policy["coverage_limit"] if is_policy_found else None,
        
        # Reconciliation Calculations
        "is_policy_found": is_policy_found,
        "is_name_matched": is_name_matched,
        "is_policy_active": is_policy_active,
        "is_limit_exceeded": is_limit_exceeded,
        "amount_difference": amount_difference,
        
        # Final Outcome
        "claim_status": claim_status,
        "rejection_reason": rejection_reason,
        "processed_time": datetime.now().isoformat(),
        
        "silver_source_file": silver_filename,
        "original_pdf_source": original_filename,
        "is_quarantined": duplicate_flag
    }

    gold_filename = f"claim_report_{timestamp}.json"
    gold_filepath = os.path.join(GOLD_DIR, gold_filename)
    with open(gold_filepath, "w") as f:
        json.dump(gold_data, f, indent=4)

    return {
        "success": True,
        "bronze_file": bronze_filename,
        "silver_file": silver_filename,
        "gold_file": gold_filename,
        "claim_status": claim_status,
        "rejection_reason": rejection_reason,
        "is_duplicate": duplicate_flag,
        "surrogate_key": surrogate_key
    }

def get_all_processed_claims():
    """Retrieve all final Gold reports to present in the dashboard."""
    if not os.path.exists(GOLD_DIR):
        return []
        
    claims = []
    for filename in os.listdir(GOLD_DIR):
        if filename.endswith(".json"):
            filepath = os.path.join(GOLD_DIR, filename)
            try:
                with open(filepath, "r") as f:
                    claims.append(json.load(f))
            except Exception:
                continue
    # Sort claims by processed time descending
    claims.sort(key=lambda x: x.get("processed_time", ""), reverse=True)
    return claims

def override_duplicate_claim(surrogate_key, new_status="APPROVED", auditor_comment="Manual Auditor Override"):
    """Overrides a quarantined duplicate claim, updating its Gold report and moving its PDF to archive."""
    archive_dir = os.path.join(DATA_DIR, "archive")
    
    # 1. Update the Gold JSON report file
    target_data = None
    target_filepath = None
    
    if not os.path.exists(GOLD_DIR):
        return False
        
    for filename in os.listdir(GOLD_DIR):
        if filename.endswith(".json"):
            filepath = os.path.join(GOLD_DIR, filename)
            try:
                with open(filepath, "r") as f:
                    data = json.load(f)
                if data.get("surrogate_key") == surrogate_key:
                    target_data = data
                    target_filepath = filepath
                    break
            except Exception:
                continue
                
    if not target_data:
        return False
        
    # Update status
    target_data["claim_status"] = new_status
    target_data["rejection_reason"] = f"Auditor Override: {auditor_comment}"
    target_data["is_quarantined"] = False
    target_data["is_override"] = True
    target_data["audited_by"] = "Auditor-1"
    target_data["audited_time"] = datetime.now().isoformat()
    
    # Save back
    with open(target_filepath, "w") as f:
        json.dump(target_data, f, indent=4)
        
    # 2. Find and move PDF from review/ to archive/
    os.makedirs(archive_dir, exist_ok=True)
    if os.path.exists(REVIEW_DIR):
        for filename in os.listdir(REVIEW_DIR):
            # Check if file name belongs to this claim
            if surrogate_key[:12] in filename or target_data.get("original_pdf_source") in filename:
                src_path = os.path.join(REVIEW_DIR, filename)
                dest_path = os.path.join(archive_dir, filename.replace("DUPLICATE_", "OVERRIDE_"))
                try:
                    shutil.move(src_path, dest_path)
                except Exception:
                    pass
    return True

