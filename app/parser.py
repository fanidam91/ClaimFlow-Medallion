import re
import os
from pypdf import PdfReader

def extract_text_from_pdf(pdf_path):
    """Extract raw text from a PDF file using pypdf."""
    try:
        reader = PdfReader(pdf_path)
        text = ""
        for page in reader.pages:
            text += page.extract_text() or ""
        return text
    except Exception as e:
        raise Exception(f"Failed to read PDF file: {str(e)}")

def parse_claim_text(text):
    """Parse key fields from extracted PDF text using regular expressions."""
    # Initialize default structure
    data = {
        "reference_no": None,
        "claimant_name": None,
        "claim_date": None,
        "claim_amount": None,
        "claim_reason": None
    }
    
    # Regex Patterns
    # Matches: Reference Number: POL-101, Reference No: POL-101, Ref: POL-101
    ref_match = re.search(r"(?i)Reference\s*(?:Number|No)?\s*:\s*([A-Z0-9-]+)", text)
    if ref_match:
        data["reference_no"] = ref_match.group(1).strip()
        
    # Matches: Claimant Name: John Smith, Claimant: John Smith, Name: John Smith
    name_match = re.search(r"(?i)(?:Claimant\s*(?:Name)?|Name)\s*:\s*([A-Za-z \t\.\'-]+)", text)
    if name_match:
        data["claimant_name"] = name_match.group(1).strip()
        
    # Matches: Claim Date: 2026-05-12, Date: 2026-05-12, Submitted Date: 2026-05-12
    date_match = re.search(r"(?i)(?:Claim\s*)?Date\s*:\s*([\d]{4}-[\d]{2}-[\d]{2})", text)
    if not date_match:
        # Fallback to other common date formats e.g. DD/MM/YYYY
        date_match = re.search(r"(?i)(?:Claim\s*)?Date\s*:\s*([\d]{2}/[\d]{2}/[\d]{4})", text)
    if date_match:
        data["claim_date"] = date_match.group(1).strip()
        
    # Matches: Claim Amount: $1,200.00, Amount: $1200, Claim Value: 1200.00
    amount_match = re.search(r"(?i)(?:Claim\s*)?Amount\s*:\s*\$?([\d,]+(?:\.[\d]{2})?)", text)
    if amount_match:
        # Clean amount (remove dollar sign and commas if any)
        raw_amount = amount_match.group(1).replace(",", "")
        try:
            data["claim_amount"] = float(raw_amount)
        except ValueError:
            data["claim_amount"] = raw_amount
            
    # Matches: Reason for Claim: Windshield damage, Reason: Flat tire
    reason_match = re.search(r"(?i)Reason\s*(?:for\s*Claim)?\s*:\s*([^\n]+)", text)
    if reason_match:
        data["claim_reason"] = reason_match.group(1).strip()
        
    return data

def parse_pdf_claim(pdf_path):
    """Helper to extract text and parse in one function call."""
    raw_text = extract_text_from_pdf(pdf_path)
    parsed = parse_claim_text(raw_text)
    return {
        "raw_text": raw_text,
        "parsed_fields": parsed
    }
