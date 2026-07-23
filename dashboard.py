import os
import json
import streamlit as st
import pandas as pd
from datetime import datetime

# Import local modules
from app.database import load_policies, upsert_policy, init_db
from app.pipeline import run_medallion_pipeline, get_all_processed_claims, calculate_surrogate_key, override_duplicate_claim
from generate_samples import generate_all_samples

# Initialize folders and default policies file
init_db()

# Set page config
st.set_page_config(
    page_title="ClaimFlow-Medallion | PDF Parser & Reconciler",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom premium styling (Dark Mode glassmorphic aesthetics)
st.markdown("""
<style>
    /* Global styling */
    .stApp {
        background-color: #0E1117;
        color: #E0E6ED;
    }
    
    /* Headers styling */
    h1, h2, h3 {
        font-family: 'Inter', sans-serif;
        font-weight: 700 !important;
        letter-spacing: -0.5px;
    }
    
    /* Metrics */
    div[data-testid="metric-container"] {
        background: rgba(255, 255, 255, 0.03);
        border: 1px solid rgba(255, 255, 255, 0.1);
        padding: 15px 20px;
        border-radius: 12px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    
    /* Badges */
    .status-badge {
        padding: 4px 8px;
        border-radius: 6px;
        font-weight: bold;
        font-size: 0.85em;
        text-align: center;
        display: inline-block;
    }
    .status-approved {
        background-color: rgba(46, 204, 113, 0.15);
        color: #2ecc71;
        border: 1px solid rgba(46, 204, 113, 0.3);
    }
    .status-rejected {
        background-color: rgba(231, 76, 60, 0.15);
        color: #e74c3c;
        border: 1px solid rgba(231, 76, 60, 0.3);
    }
    .status-duplicate {
        background-color: rgba(241, 196, 15, 0.15);
        color: #f1c40f;
        border: 1px solid rgba(241, 196, 15, 0.3);
    }

    /* Flow diagram styles */
    .flow-container {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin: 20px 0;
        background: rgba(255, 255, 255, 0.02);
        padding: 20px;
        border-radius: 12px;
        border: 1px solid rgba(255, 255, 255, 0.05);
    }
    .flow-node {
        flex: 1;
        text-align: center;
        padding: 15px;
        margin: 0 10px;
        border-radius: 10px;
        font-weight: bold;
        box-shadow: 0 4px 10px rgba(0,0,0,0.2);
    }
    .node-raw {
        background: linear-gradient(135deg, #34495e, #2c3e50);
        border: 1px solid #7f8c8d;
        color: #ecf0f1;
    }
    .node-bronze {
        background: linear-gradient(135deg, #d35400, #a04000);
        border: 1px solid #e67e22;
        color: #fdf2e9;
    }
    .node-silver {
        background: linear-gradient(135deg, #7f8c8d, #95a5a6);
        border: 1px solid #bdc3c7;
        color: #f8f9f9;
    }
    .node-gold {
        background: linear-gradient(135deg, #d4af37, #b8860b);
        border: 1px solid #f1c40f;
        color: #fef9e7;
    }
    .flow-arrow {
        font-size: 24px;
        color: rgba(255, 255, 255, 0.3);
    }
</style>
""", unsafe_allow_html=True)

# Application Title
st.title("🌟 ClaimFlow-Medallion Pipeline")
st.markdown("### Document Parsing & Policy Reconciliation (Databricks Medallion Simulation)")

# Sidebar panel
st.sidebar.image("https://img.icons8.com/nolan/128/blockchain.png", width=64)
st.sidebar.title("Claim Ingestion Center")

# Create directories and generate samples if missing
project_dir = os.path.dirname(os.path.abspath(__file__))
samples_dir = os.path.join(project_dir, "samples")
if not os.path.exists(samples_dir) or len(os.listdir(samples_dir)) == 0:
    with st.spinner("Generating sample PDF files..."):
        generate_all_samples()

# Upload File Section
st.sidebar.markdown("---")
st.sidebar.subheader("📤 Upload Claim Form")
uploaded_file = st.sidebar.file_uploader("Select an Insurance Claim PDF", type="pdf")

if uploaded_file is not None:
    # Save the file temporarily
    temp_dir = os.path.join(project_dir, "data", "temp")
    os.makedirs(temp_dir, exist_ok=True)
    temp_path = os.path.join(temp_dir, uploaded_file.name)
    with open(temp_path, "wb") as f:
        f.write(uploaded_file.getbuffer())
        
    if st.sidebar.button("⚙️ Run Ingestion Pipeline", use_container_width=True):
        with st.spinner("Processing document through Medallion stages..."):
            res = run_medallion_pipeline(temp_path, original_filename=uploaded_file.name)
            if res["success"]:
                st.sidebar.success(f"Success! Status: {res['claim_status']}")
                # Clean temp file
                if os.path.exists(temp_path):
                    os.remove(temp_path)
                st.rerun()
            else:
                st.sidebar.error(res["error"])

# Run Samples section
st.sidebar.markdown("---")
st.sidebar.subheader("📁 Test with Sample Claims")
sample_files = [f for f in os.listdir(samples_dir) if f.endswith(".pdf")]
selected_sample = st.sidebar.selectbox("Choose a pre-generated scenario:", sample_files)

if st.sidebar.button("🚀 Process Selected Sample", use_container_width=True):
    sample_path = os.path.join(samples_dir, selected_sample)
    with st.spinner("Processing sample through Medallion stages..."):
        res = run_medallion_pipeline(sample_path, original_filename=selected_sample)
        if res["success"]:
            st.sidebar.success(f"Ingested! Status: {res['claim_status']}")
            st.rerun()
        else:
            st.sidebar.error(res["error"])

# Reset all claims
st.sidebar.markdown("---")
if st.sidebar.button("🗑️ Reset Ingestion Pipeline Data", use_container_width=True):
    for folder in ["bronze", "silver", "gold", "review", "raw"]:
        f_dir = os.path.join(project_dir, "data", folder)
        if os.path.exists(f_dir):
            for file in os.listdir(f_dir):
                if file.endswith(".json") or file.endswith(".pdf"):
                    try:
                        os.remove(os.path.join(f_dir, file))
                    except Exception:
                        pass
    st.sidebar.warning("All processed data cleared.")
    st.rerun()

# Load processed claims
processed_claims = get_all_processed_claims()

# Main Metrics Calculation
total_cnt = len(processed_claims)
approved_cnt = sum(1 for c in processed_claims if c.get("claim_status") == "APPROVED")
rejected_cnt = sum(1 for c in processed_claims if c.get("claim_status") == "REJECTED")
dup_cnt = sum(1 for c in processed_claims if "DUPLICATE" in str(c.get("claim_status")))

# Layout Metrics
m1, m2, m3, m4 = st.columns(4)
m1.metric("Claims Processed", total_cnt)
m2.metric("Approved", approved_cnt, delta=f"{int(approved_cnt/total_cnt*100)}%" if total_cnt > 0 else None)
m3.metric("Rejected / Mismatched", rejected_cnt)
m4.metric("Quarantined (Duplicates)", dup_cnt, delta="Review Required" if dup_cnt > 0 else None, delta_color="inverse")

# Main Content Tabs
tab1, tab2, tab3, tab4 = st.tabs([
    "📋 Reconciliation Dashboard", 
    "🔄 Medallion Pipeline Explorer", 
    "🛡️ Quarantine Folder",
    "⚙️ Reference Policy Manager"
])

# Tab 1: Dashboard
with tab1:
    st.subheader("Final Mapped Reports (Gold Layer)")
    if total_cnt == 0:
        st.info("No claims have been ingested yet. Use the sidebar to upload a PDF or process a sample claim!")
    else:
        # Convert claims list to DataFrame for displaying
        df_rows = []
        for c in processed_claims:
            # Map statuses
            status = c.get("claim_status")
            status_html = ""
            if status == "APPROVED":
                status_html = f'<span class="status-badge status-approved">{status}</span>'
            elif "DUPLICATE" in status:
                status_html = f'<span class="status-badge status-duplicate">DUPLICATE</span>'
            else:
                status_html = f'<span class="status-badge status-rejected">{status}</span>'
                
            df_rows.append({
                "Surrogate Key": c.get("surrogate_key", "")[:12] + "...",
                "Ref No": c.get("reference_no", "N/A"),
                "Claimant (PDF)": c.get("claimant_name", "N/A"),
                "Claim Amount": f"${c.get('claim_amount', 0.0):,.2f}",
                "Date": c.get("claim_date", "N/A"),
                "Policy Owner (DB)": c.get("policyholder_db") or "NOT FOUND",
                "Policy Status": c.get("policy_status_db") or "N/A",
                "Limit": f"${c.get('coverage_limit_db', 0.0):,.2f}" if c.get('coverage_limit_db') else "N/A",
                "Outcome": status_html,
                "Audit Reason": c.get("rejection_reason", ""),
                "original_filename": c.get("original_pdf_source")
            })
            
        df = pd.DataFrame(df_rows)
        # Render HTML to allow the status badges to display properly
        st.write(df.to_html(escape=False, index=False), unsafe_allow_html=True)

# Tab 2: Medallion Pipeline Explorer
with tab2:
    st.subheader("Medallion Architecture Lifecycle Explorer")
    if total_cnt == 0:
        st.info("Ingest a claim to view its Medallion data flow.")
    else:
        claim_options = {
            f"{c.get('original_pdf_source')} ({c.get('processed_time')[:19].replace('T', ' ')})": c
            for c in processed_claims
        }
        selected_key = st.selectbox("Select a processed claim to trace:", list(claim_options.keys()))
        selected_claim = claim_options[selected_key]
        
        # Display Flow Diagram
        st.markdown(f"""
        <div class="flow-container">
            <div class="flow-node node-raw">
                📄 PDF Submitted<br/>
                <span style="font-size: 0.8em; font-weight: normal;">{selected_claim.get('original_pdf_source')}</span>
            </div>
            <div class="flow-arrow">➔</div>
            <div class="flow-node node-bronze">
                🥉 Bronze Layer<br/>
                <span style="font-size: 0.8em; font-weight: normal;">Raw Extracted Text</span>
            </div>
            <div class="flow-arrow">➔</div>
            <div class="flow-node node-silver">
                🥈 Silver Layer<br/>
                <span style="font-size: 0.8em; font-weight: normal;">Cleaned & Structured</span>
            </div>
            <div class="flow-arrow">➔</div>
            <div class="flow-node node-gold">
                🥇 Gold Layer<br/>
                <span style="font-size: 0.8em; font-weight: normal;">Policy Reconciled</span>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Display Auditing Details
        col_aud1, col_aud2 = st.columns(2)
        with col_aud1:
            st.info(f"**Surrogate Key (Unique Hash):**  \n`{selected_claim.get('surrogate_key')}`")
        with col_aud2:
            st.warning(f"**Blob Store Simulated Path:**  \n`abfss://claims-raw@stcoredatadevaue.dfs.core.windows.net/raw/{selected_claim.get('original_pdf_source')}`")
            
        # Tabs for actual files
        sub_tab_bronze, sub_tab_silver, sub_tab_gold = st.tabs([
            "🥉 Bronze File (Raw Data)", 
            "🥈 Silver File (Cleaned Data)", 
            "🥇 Gold File (Reconciled Report)"
        ])
        
        # Load Bronze file
        with sub_tab_bronze:
            bronze_file = os.path.join(PROJECT_DIR, "data", "bronze", selected_claim.get("silver_source_file").replace("claim_clean_", "claim_raw_"))
            if os.path.exists(bronze_file):
                with open(bronze_file, "r") as f:
                    st.json(json.load(f))
            else:
                st.error("Bronze file not found on disk.")
                
        # Load Silver file
        with sub_tab_silver:
            silver_file = os.path.join(PROJECT_DIR, "data", "silver", selected_claim.get("silver_source_file"))
            if os.path.exists(silver_file):
                with open(silver_file, "r") as f:
                    st.json(json.load(f))
            else:
                st.error("Silver file not found on disk.")
                
        # Display Gold data directly (loaded in memory)
        with sub_tab_gold:
            st.json(selected_claim)

# Tab 3: Quarantine Directory & Audit Override
with tab3:
    st.subheader("Quarantined Duplicate Claims")
    st.markdown("If a claim matches an already processed claim's surrogate key (same policy, claimant, date, and amount), the system flags the file and routes the PDF to the review directory.")
    
    review_dir = os.path.join(project_dir, "data", "review")
    
    # 1. Audit Override Action Console
    flagged_claims = [c for c in processed_claims if "DUPLICATE" in str(c.get("claim_status"))]
    
    if len(flagged_claims) > 0:
        st.markdown("---")
        st.write("##### ⚖️ Auditor Decision Override Console")
        st.info("If the audit confirms the claim is a legitimate separate request (not a duplicate submission), you can override the status and approve it here.")
        
        claim_map = {
            f"Ref: {c.get('reference_no')} | {c.get('claimant_name')} | ${c.get('claim_amount'):,.2f} ({c.get('processed_time')[:16].replace('T', ' ')})": c
            for c in flagged_claims
        }
        
        selected_flagged_key = st.selectbox("Select a flagged claim to review:", list(claim_map.keys()))
        selected_flagged = claim_map[selected_flagged_key]
        
        col_aud_c1, col_aud_c2 = st.columns(2)
        with col_aud_c1:
            st.write("**Claim Details (from PDF):**")
            st.write(f"- Claimant Name: `{selected_flagged.get('claimant_name')}`")
            st.write(f"- Claim Date: `{selected_flagged.get('claim_date')}`")
            st.write(f"- Claim Amount: `${selected_flagged.get('claim_amount'):,.2f}`")
            st.write(f"- Reason: `\"{selected_flagged.get('claim_reason')}\"`")
        with col_aud_c2:
            st.write("**Policy Details (from DB):**")
            st.write(f"- Policyholder Name: `{selected_flagged.get('policyholder_db')}`")
            st.write(f"- Policy Type: `{selected_flagged.get('policy_type_db')}`")
            st.write(f"- Status: `{selected_flagged.get('policy_status_db')}`")
            st.write(f"- Coverage Limit: `${selected_flagged.get('coverage_limit_db', 0.0):,.2f}`")
            
        audit_note = st.text_input("Auditor Verification Notes / Override Reason", value="Verified as a separate legitimate claim request.")
        
        col_btn1, col_btn2, _ = st.columns([1, 1, 2])
        with col_btn1:
            if st.button("✅ Approve Override", use_container_width=True):
                if override_duplicate_claim(selected_flagged.get("surrogate_key"), "APPROVED", audit_note):
                    st.success("Claim successfully approved and moved out of quarantine!")
                    st.rerun()
                else:
                    st.error("Failed to apply override.")
        with col_btn2:
            if st.button("❌ Dismiss / Reject Claim", use_container_width=True):
                if override_duplicate_claim(selected_flagged.get("surrogate_key"), "REJECTED", f"Auditor Confirmed Duplicate: {audit_note}"):
                    st.warning("Claim status updated to Rejected.")
                    st.rerun()
                else:
                    st.error("Failed to update status.")
                    
        st.markdown("---")
        
    # 2. File list
    st.write("##### Quarantined Files in Review Directory")
    if not os.path.exists(review_dir) or len(os.listdir(review_dir)) == 0:
        st.success("✅ Clean Audit: No duplicate claim PDF files currently in quarantine.")
    else:
        st.warning(f"⚠️ Action Required: {len(os.listdir(review_dir))} files flagged as potential duplicate submissions on DBFS/Disk.")
        
        files_in_review = os.listdir(review_dir)
        df_review = []
        for file in files_in_review:
            stats = os.stat(os.path.join(review_dir, file))
            df_review.append({
                "Filename": file,
                "Quarantine Location": f"dbfs:/tmp/claims_streaming/review/{file}",
                "Size (Bytes)": stats.st_size,
                "Flagged Date": datetime.fromtimestamp(stats.st_mtime).strftime("%Y-%m-%d %H:%M:%S")
            })
            
        st.dataframe(pd.DataFrame(df_review), use_container_width=True)

# Tab 4: Policy Database Manager
with tab4:
    st.subheader("Reference Policy Database")
    st.markdown("This database simulates the static master policy list stored in `` `adb-core-data-dev-aue`.insurance_claims.reference_policies ``.")
    
    # Load policies
    policies = load_policies()
    
    # Edit / Insert Policy
    st.markdown("---")
    st.write("##### Add or Edit Reference Policy")
    col1, col2, col3 = st.columns(3)
    with col1:
        ref_no = st.text_input("Policy Reference Number", value="POL-", placeholder="e.g. POL-105")
    with col2:
        policyholder = st.text_input("Policyholder Claimant Name", placeholder="e.g. John Doe")
    with col3:
        policy_type = st.selectbox("Policy Type", ["Auto Insurance", "Home Insurance", "Health Insurance", "Commercial Liability"])
        
    col4, col5, col6 = st.columns(3)
    with col4:
        coverage_limit = st.number_input("Coverage Limit ($)", min_value=0.0, value=10000.0, step=500.0)
    with col5:
        policy_status = st.selectbox("Policy Status", ["Active", "Expired", "Suspended"])
    with col6:
        eff_date = st.date_input("Effective Date", value=datetime.today())
        
    if st.button("💾 Upsert Policy to Database"):
        if ref_no == "POL-" or not policyholder:
            st.error("Error: Please provide a valid Reference Number and Claimant Name.")
        else:
            policy_data = {
                "reference_no": ref_no.upper().strip(),
                "policyholder": policyholder.strip(),
                "policy_type": policy_type,
                "coverage_limit": coverage_limit,
                "policy_status": policy_status,
                "effective_date": eff_date.strftime("%Y-%m-%d")
            }
            if upsert_policy(policy_data):
                st.success(f"Policy '{ref_no}' successfully saved/updated!")
                st.rerun()
            else:
                st.error("Failed to save policy.")
                
    st.markdown("---")
    st.write("##### Current Active Policies")
    st.dataframe(pd.DataFrame(policies), use_container_width=True)
