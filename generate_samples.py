import os
import sys

def check_and_install_reportlab():
    """Ensure reportlab is installed before generating samples."""
    try:
        from reportlab.pdfgen import canvas
    except ImportError:
        print("reportlab not found. Installing it...")
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", "reportlab"])

# Ensure reportlab is available
check_and_install_reportlab()

from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

def create_claim_pdf(filepath, ref_no, claimant, date, amount, reason):
    """Generates a structured PDF claim form using reportlab."""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    c = canvas.Canvas(filepath, pagesize=letter)
    width, height = letter
    
    # Design Header Banner
    c.setFillColorRGB(0.1, 0.2, 0.45) # Dark Blue Banner
    c.rect(0, height - 100, width, 100, fill=True, stroke=False)
    
    # Title Text
    c.setFillColorRGB(1.0, 1.0, 1.0)
    c.setFont("Helvetica-Bold", 24)
    c.drawString(50, height - 60, "SECURE GUARD INSURANCE")
    c.setFont("Helvetica", 12)
    c.drawString(50, height - 80, "Official Claim Request Document")
    
    # Body Styling
    c.setFillColorRGB(0, 0, 0)
    c.setFont("Helvetica-Bold", 14)
    c.drawString(50, height - 140, "Claim Submission Details")
    
    # Draw decorative line
    c.setStrokeColorRGB(0.8, 0.8, 0.8)
    c.setLineWidth(1)
    c.line(50, height - 150, width - 50, height - 150)
    
    # Form Fields Layout
    y = height - 190
    line_height = 30
    
    fields = [
        ("Reference Number:", ref_no),
        ("Claimant Name:", claimant),
        ("Claim Date:", date),
        ("Claim Amount:", amount),
        ("Reason for Claim:", reason)
    ]
    
    for label, val in fields:
        c.setFont("Helvetica-Bold", 11)
        c.drawString(50, y, label)
        c.setFont("Helvetica", 11)
        c.drawString(220, y, str(val))
        y -= line_height
        
    c.line(50, y - 10, width - 50, y - 10)
    
    # Footer Section
    c.setFillColorRGB(0.5, 0.5, 0.5)
    c.setFont("Helvetica-Oblique", 9)
    c.drawString(50, y - 35, "Disclaimer: Fraudulent claims are subject to immediate termination and legal action.")
    c.drawString(50, y - 50, f"Generated for verification. Document ID: CLM-{ref_no}-{date.replace('-', '')}")
    c.drawString(50, y - 65, "Secure Guard Co. (Australia East - Sydney)")
    
    c.save()
    print(f"Successfully generated sample claim PDF: {filepath}")

def generate_all_samples():
    project_dir = os.path.dirname(os.path.abspath(__file__))
    samples_dir = os.path.join(project_dir, "samples")
    raw_dir = os.path.join(project_dir, "data", "raw")
    
    # List of samples to generate in both 'samples' folder and the DBFS-simulated local 'raw' folder
    claim_specs = [
        ("claim_john_smith_valid.pdf", "POL-101", "John Smith", "2026-05-12", "$1,200.00", "Windshield rock strike damage and glass replacement"),
        ("claim_sarah_connor_over_limit.pdf", "POL-102", "Sarah Connor", "2026-06-01", "$15,000.00", "Total engine replacement due to critical mechanical failure"),
        ("claim_invalid_reference.pdf", "POL-999", "Bruce Wayne", "2026-06-10", "$5,000.00", "Bumper replacement and minor bodywork scratching"),
        ("claim_name_mismatch.pdf", "POL-101", "Emma Watson", "2026-06-15", "$450.00", "Flat tire replacement and wheel alignment"),
        ("claim_alice_johnson_expired.pdf", "POL-103", "Alice Johnson", "2026-06-20", "$3,500.00", "Dental treatment under medical checkup policy")
    ]
    
    for filename, ref, claimant, date, amt, reason in claim_specs:
        # Write to samples folder
        create_claim_pdf(os.path.join(samples_dir, filename), ref, claimant, date, amt, reason)
        # Write to raw ingestion folder
        create_claim_pdf(os.path.join(raw_dir, filename), ref, claimant, date, amt, reason)

if __name__ == "__main__":
    generate_all_samples()
