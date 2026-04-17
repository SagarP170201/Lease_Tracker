import streamlit as st
import pandas as pd
import datetime
from datetime import timedelta
import json
from snowflake.snowpark.context import get_active_session

st.set_page_config(
    page_title="Lease Intelligence - Apparel Group",
    page_icon=":office:",
    layout="wide",
    initial_sidebar_state="expanded",
)

DB = "SI_EVENTS_HOL"
SCHEMA = "LEASE_INTELLIGENCE"
STAGE = f"@{DB}.{SCHEMA}.LEASE_DOCS"

CITY_REGION = {
    "Mumbai": "West", "Pune": "West", "Navi Mumbai": "West", "Ahmedabad": "West", "Surat": "West",
    "Delhi": "North", "New Delhi": "North", "Gurgaon": "North", "Gurugram": "North",
    "Noida": "North", "Chandigarh": "North", "Faridabad": "North",
    "Bangalore": "South", "Bengaluru": "South", "Chennai": "South",
    "Hyderabad": "South", "Kochi": "South", "Cochin": "South", "Coimbatore": "South",
    "Kolkata": "East", "Kanpur": "East", "Lucknow": "East", "Patna": "East",
}

session = get_active_session()


def run_query(sql):
    return session.sql(sql).to_pandas()


def run_dml(sql):
    session.sql(sql).collect()


@st.cache_data(ttl=60)
def load_leases():
    return run_query(f"SELECT * FROM {DB}.{SCHEMA}.LEASE_TRACKER ORDER BY LEASE_ID")


@st.cache_data(ttl=60)
def load_staging():
    return run_query(f"SELECT * FROM {DB}.{SCHEMA}.LEASE_STAGING ORDER BY STAGING_ID DESC")


@st.cache_data(ttl=60)
def load_parsed():
    return run_query(f"SELECT DOC_ID, FILE_NAME, PAGE_COUNT, PARSED_AT, STATUS FROM {DB}.{SCHEMA}.PARSED_DOCS ORDER BY DOC_ID DESC")


def _safe_num(v, default=None):
    if v is None or v == "":
        return default
    try:
        return float(str(v).replace(",", "").replace("\u20b9", "").replace("INR", "").strip())
    except Exception:
        return default


def _safe_int(v, default=None):
    n = _safe_num(v)
    return int(n) if n is not None else default


def _safe_date(v, default=None):
    if v is None or v == "":
        return default
    try:
        return datetime.date.fromisoformat(str(v)[:10])
    except Exception:
        return default


def _add_months(d, months):
    if d is None or months is None:
        return None
    try:
        m = d.month - 1 + int(months)
        return datetime.date(d.year + m // 12, m % 12 + 1, 1)
    except Exception:
        return None


def _sql_val(v):
    if v is None:
        return "NULL"
    if isinstance(v, (datetime.date, datetime.datetime)):
        return f"'{v}'"
    if isinstance(v, (int, float)):
        return str(v)
    escaped = str(v).replace("'", "''")
    return f"'{escaped}'"


def approve_record(staging_id, file_name, extract):
    city = extract.get("city") or ""
    lease_start = _safe_date(extract.get("lease_start_date"))
    lease_end = _safe_date(extract.get("lease_end_date"))
    lock_in_months = _safe_int(extract.get("lock_in_period_months"))
    esc_freq = _safe_int(extract.get("escalation_frequency_months")) or 12
    renewal_days = _safe_int(extract.get("renewal_notice_days")) or 90
    fixed_rent = _safe_num(extract.get("fixed_rent_monthly")) or 0
    min_guarantee = _safe_num(extract.get("minimum_guarantee_monthly")) or 0
    cam = _safe_num(extract.get("cam_monthly")) or 0
    hvac = _safe_num(extract.get("hvac_monthly")) or 0
    marketing = _safe_num(extract.get("marketing_contribution_monthly")) or 0
    base_rent = fixed_rent if fixed_rent > 0 else min_guarantee
    total_monthly = base_rent + cam + hvac + marketing
    renewal_date = (lease_end - timedelta(days=renewal_days)) if lease_end and renewal_days else None

    vals = {
        "FILE_NAME": _sql_val(file_name),
        "STORE_NAME": _sql_val(extract.get("store_name") or ""),
        "MALL_NAME": _sql_val(extract.get("mall_name") or ""),
        "CITY": _sql_val(city),
        "STATE": _sql_val(extract.get("state") or ""),
        "REGION": _sql_val(CITY_REGION.get(city, "Other")),
        "CARPET_AREA_SQFT": _sql_val(_safe_num(extract.get("carpet_area_sqft"))),
        "LESSOR_NAME": _sql_val(extract.get("lessor_name") or ""),
        "LESSEE_NAME": _sql_val(extract.get("lessee_name") or ""),
        "BRAND": _sql_val(extract.get("brand") or ""),
        "LEASE_START_DATE": _sql_val(lease_start),
        "LEASE_END_DATE": _sql_val(lease_end),
        "LEASE_TENURE_MONTHS": _sql_val(_safe_int(extract.get("lease_tenure_months"))),
        "LOCK_IN_PERIOD_MONTHS": _sql_val(lock_in_months),
        "LOCK_IN_EXPIRY_DATE": _sql_val(_add_months(lease_start, lock_in_months)),
        "NOTICE_PERIOD_DAYS": _sql_val(_safe_int(extract.get("notice_period_days"))),
        "RENT_FREE_PERIOD_MONTHS": _sql_val(_safe_int(extract.get("rent_free_period_months"))),
        "RENT_MODEL": _sql_val(extract.get("rent_model") or "FIXED"),
        "FIXED_RENT_MONTHLY": _sql_val(fixed_rent or None),
        "REVENUE_SHARE_PCT": _sql_val(_safe_num(extract.get("revenue_share_pct"))),
        "MINIMUM_GUARANTEE_MONTHLY": _sql_val(min_guarantee or None),
        "RATE_PER_SQFT": _sql_val(_safe_num(extract.get("rate_per_sqft"))),
        "ESCALATION_PCT": _sql_val(_safe_num(extract.get("escalation_pct"))),
        "ESCALATION_FREQUENCY_MONTHS": _sql_val(esc_freq),
        "NEXT_ESCALATION_DATE": _sql_val(_add_months(lease_start, esc_freq)),
        "CAM_MONTHLY": _sql_val(cam or None),
        "HVAC_MONTHLY": _sql_val(hvac or None),
        "MARKETING_CONTRIBUTION_MONTHLY": _sql_val(marketing or None),
        "SECURITY_DEPOSIT": _sql_val(_safe_num(extract.get("security_deposit"))),
        "STAMP_DUTY": _sql_val(_safe_num(extract.get("stamp_duty"))),
        "REGISTRATION_COST": _sql_val(_safe_num(extract.get("registration_cost"))),
        "CAPEX_REIMBURSEMENT": _sql_val(_safe_num(extract.get("capex_reimbursement"))),
        "PAYMENT_DUE_DAY": _sql_val(_safe_int(extract.get("payment_due_day"))),
        "RENEWAL_OPTION": _sql_val(extract.get("renewal_option") or "MUTUAL"),
        "RENEWAL_NOTICE_DAYS": _sql_val(renewal_days),
        "RENEWAL_DATE": _sql_val(renewal_date),
        "TOTAL_MONTHLY_OUTGO": _sql_val(total_monthly or None),
        "STATUS": "'ACTIVE'",
        "APPROVED_BY": "'spawar'",
        "APPROVED_AT": "CURRENT_TIMESTAMP()",
    }

    col_list = ", ".join(vals.keys())
    val_list = ", ".join(vals.values())

    run_dml(f"""
        INSERT INTO {DB}.{SCHEMA}.LEASE_TRACKER ({col_list})
        VALUES ({val_list})
    """)

    run_dml(f"UPDATE {DB}.{SCHEMA}.LEASE_STAGING SET STATUS = 'APPROVED' WHERE STAGING_ID = {staging_id}")

    load_leases.clear()
    load_staging.clear()


with st.sidebar:
    st.markdown("### Apparel Group")
    st.markdown("**Lease Intelligence Platform**")
    st.markdown("---")
    page = st.radio(
        "Navigation",
        [
            "Portfolio Overview",
            "Upload & Extract",
            "Review & Approve",
            "Cash Flow",
            "Alerts & Renewals",
            "Regional Analytics",
        ],
    )

df = load_leases()
today = datetime.date.today()

if page == "Portfolio Overview":
    st.title("Lease Portfolio Overview")

    active = df[df["STATUS"] == "ACTIVE"]
    expiring = df[df["STATUS"] == "EXPIRING"]
    expired = df[df["STATUS"] == "EXPIRED"]
    total_monthly = df["TOTAL_MONTHLY_OUTGO"].sum()
    total_area = df["CARPET_AREA_SQFT"].sum()
    total_deposit = df["SECURITY_DEPOSIT"].sum()
    avg_rate = df["RATE_PER_SQFT"].mean()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Leases", len(df))
    c2.metric("Active", len(active))
    c3.metric("Expiring Soon", len(expiring), "Need attention")
    c4.metric("Expired", len(expired))

    c5, c6, c7, c8 = st.columns(4)
    c5.metric("Monthly Outgo", f"INR {total_monthly:,.0f}")
    c6.metric("Total Area", f"{total_area:,.0f} sq ft")
    c7.metric("Security Deposits", f"INR {total_deposit:,.0f}")
    c8.metric("Avg Rate/sq ft", f"INR {avg_rate:,.0f}")

    st.markdown("---")
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Leases by Brand")
        brand_data = df.groupby("BRAND")["LEASE_ID"].count().reset_index()
        brand_data.columns = ["BRAND", "Stores"]
        st.bar_chart(brand_data.set_index("BRAND"))

    with col2:
        st.subheader("Rent Model Mix")
        rent_mix = df["RENT_MODEL"].value_counts()
        st.bar_chart(rent_mix)

    col3, col4 = st.columns(2)

    with col3:
        st.subheader("Monthly Outgo by Region")
        region_data = df.groupby("REGION")["TOTAL_MONTHLY_OUTGO"].sum()
        st.bar_chart(region_data)

    with col4:
        st.subheader("Lease Status Distribution")
        status_data = df["STATUS"].value_counts()
        st.bar_chart(status_data)

    st.markdown("---")
    st.subheader("All Leases")
    display_cols = [
        "STORE_NAME", "BRAND", "MALL_NAME", "CITY", "STATE",
        "RENT_MODEL", "TOTAL_MONTHLY_OUTGO", "LEASE_START_DATE",
        "LEASE_END_DATE", "STATUS",
    ]
    st.dataframe(df[display_cols], hide_index=True, use_container_width=True)

elif page == "Upload & Extract":
    st.title("Upload Lease Agreements")

    st.info(
        "Upload lease agreement PDFs here. The AI pipeline will automatically:\n"
        "1. **Parse** the document (AI_PARSE_DOCUMENT - LAYOUT mode)\n"
        "2. **Extract** 30+ lease fields (AI_EXTRACT)\n"
        "3. Place results in **Review & Approve** for human validation"
    )

    uploaded_files = st.file_uploader(
        "Upload Lease Agreement PDFs", type=["pdf"], accept_multiple_files=True
    )

    if uploaded_files:
        st.markdown(f"**{len(uploaded_files)} file(s) selected**")
        if st.button("Upload to Snowflake Stage", type="primary"):
            progress = st.progress(0)
            for i, file in enumerate(uploaded_files):
                try:
                    session.file.put_stream(file, f"{STAGE}/{file.name}", auto_compress=False, overwrite=True)
                    st.success(f"Uploaded: {file.name}")
                except Exception as e:
                    st.error(f"Failed to upload {file.name}: {e}")
                progress.progress((i + 1) / len(uploaded_files))
            st.balloons()
            st.success(
                "Files uploaded! The automated pipeline will process them within 5 minutes. "
                "Check **Review & Approve** for extracted results."
            )

    st.markdown("---")
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Pipeline Status")
        try:
            task_status = run_query(
                f"""SELECT NAME, STATE, LAST_SUCCESSFUL_RUN_TIME
                FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
                    SCHEDULED_TIME_RANGE_START => DATEADD('hour', -24, CURRENT_TIMESTAMP())
                ))
                WHERE DATABASE_NAME = '{DB}' AND SCHEMA_NAME = '{SCHEMA}'
                ORDER BY SCHEDULED_TIME DESC LIMIT 10"""
            )
            if len(task_status) > 0:
                st.dataframe(task_status, hide_index=True)
            else:
                st.caption("No task runs in the last 24 hours")
        except Exception:
            st.caption("Pipeline tasks are configured and waiting for new uploads")

    with col2:
        st.subheader("Stage Contents")
        try:
            stage_files = run_query(f"SELECT RELATIVE_PATH, SIZE, LAST_MODIFIED FROM DIRECTORY({STAGE})")
            if len(stage_files) > 0:
                st.dataframe(stage_files, hide_index=True)
            else:
                st.caption("No files in stage yet")
        except Exception:
            st.caption("Stage is empty - upload files above")

    st.subheader("Parsed Documents")
    try:
        parsed = load_parsed()
        if len(parsed) > 0:
            st.dataframe(parsed, hide_index=True)
        else:
            st.caption("No documents parsed yet")
    except Exception:
        st.caption("Waiting for first document to be parsed")

    st.markdown("---")
    st.subheader("How it works")
    st.code("""
PDF Upload --> Snowflake Stage
     | (Stream detects new file)
Task 1: AI_PARSE_DOCUMENT (LAYOUT mode)
     --> Extracts full text with table structure preserved
     --> Stores in PARSED_DOCS table
     | (Stream on PARSED_DOCS)
Task 2: AI_EXTRACT on parsed text
     --> Extracts 30+ structured lease fields
     --> Stores in LEASE_STAGING table
     |
Human Review (this app)
     --> Validate, correct, approve
     --> Moves to LEASE_TRACKER (golden record)
""", language=None)

elif page == "Review & Approve":
    st.title("Review & Correct Extracted Lease Data")

    FIELD_SECTIONS = {
        "Property Details": [
            ("store_name", "Store / Outlet Name", "text"),
            ("brand", "Brand", "text"),
            ("mall_name", "Mall / Shopping Center", "text"),
            ("city", "City", "text"),
            ("state", "State", "text"),
            ("carpet_area_sqft", "Carpet Area (sq ft)", "number"),
            ("lessor_name", "Lessor / Landlord", "text"),
            ("lessee_name", "Lessee / Tenant", "text"),
        ],
        "Lease Tenure": [
            ("lease_start_date", "Lease Start Date (YYYY-MM-DD)", "text"),
            ("lease_end_date", "Lease End Date (YYYY-MM-DD)", "text"),
            ("lease_tenure_months", "Tenure (months)", "number"),
            ("lock_in_period_months", "Lock-in (months)", "number"),
            ("notice_period_days", "Notice Period (days)", "number"),
            ("rent_free_period_months", "Rent-Free / Fit-out (months)", "number"),
        ],
        "Rent Structure": [
            ("rent_model", "Rent Model (FIXED / HYBRID / REVENUE_SHARE)", "text"),
            ("fixed_rent_monthly", "Fixed Rent Monthly (INR)", "number"),
            ("revenue_share_pct", "Revenue Share %", "number"),
            ("minimum_guarantee_monthly", "Minimum Guarantee Monthly (INR)", "number"),
            ("rate_per_sqft", "Rate per Sq Ft (INR)", "number"),
            ("escalation_pct", "Escalation %", "number"),
            ("escalation_frequency_months", "Escalation Frequency (months)", "number"),
        ],
        "Charges & Deposits": [
            ("cam_monthly", "CAM Monthly (INR)", "number"),
            ("hvac_monthly", "HVAC Monthly (INR)", "number"),
            ("marketing_contribution_monthly", "Marketing Monthly (INR)", "number"),
            ("security_deposit", "Security Deposit (INR)", "number"),
            ("stamp_duty", "Stamp Duty (INR)", "number"),
            ("registration_cost", "Registration Cost (INR)", "number"),
            ("capex_reimbursement", "Capex Reimbursement (INR)", "number"),
        ],
        "Payment & Renewal": [
            ("payment_due_day", "Payment Due Day of Month", "number"),
            ("renewal_option", "Renewal (AUTO / MUTUAL / NONE)", "text"),
            ("renewal_notice_days", "Renewal Notice (days)", "number"),
        ],
    }

    try:
        staging = load_staging()
        if len(staging) > 0:
            pending = staging[staging["STATUS"] == "PENDING_REVIEW"]
            approved = staging[staging["STATUS"] == "APPROVED"]
            rejected = staging[staging["STATUS"] == "REJECTED"]

            c1, c2, c3 = st.columns(3)
            c1.metric("Pending Review", len(pending))
            c2.metric("Approved", len(approved))
            c3.metric("Rejected", len(rejected))

            if len(pending) == 0:
                st.info("All records reviewed. Upload more lease PDFs in **Upload & Extract**.")

            for idx, row in pending.iterrows():
                extract = row["RAW_EXTRACT"]
                if isinstance(extract, str):
                    try:
                        extract = json.loads(extract)
                    except Exception:
                        extract = {}
                if not isinstance(extract, dict):
                    extract = {}

                sid = row["STAGING_ID"]
                fname = row["FILE_NAME"]

                with st.expander(f"FILE: {fname} -- Extracted {row['EXTRACTED_AT']}", expanded=(len(pending) <= 3)):
                    st.caption("Edit any field below before approving. AI-extracted values are pre-filled.")

                    edited = {}
                    for section_name, fields in FIELD_SECTIONS.items():
                        st.markdown(f"**{section_name}**")
                        cols = st.columns(3)
                        for i, (key, label, ftype) in enumerate(fields):
                            raw_val = extract.get(key, "")
                            if raw_val is None:
                                raw_val = ""
                            raw_val = str(raw_val).replace("Rs. ", "").replace("Rs.", "").replace(",", "").strip()
                            with cols[i % 3]:
                                edited[key] = st.text_input(label, value=raw_val, key=f"{sid}_{key}")
                        st.markdown("---")

                    st.markdown("**Reviewer Notes**")
                    notes = st.text_area("Add corrections or comments", key=f"{sid}_notes", height=68)

                    bcol1, bcol2, bcol3 = st.columns([2, 1, 1])
                    with bcol1:
                        if st.button("Approve & Move to Tracker", key=f"approve_{sid}", type="primary"):
                            try:
                                approve_record(sid, fname, edited)
                                st.success(f"Approved! {fname} moved to Lease Tracker.")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Approval failed: {e}")
                    with bcol2:
                        if st.button("Reject", key=f"reject_{sid}"):
                            try:
                                run_dml(
                                    f"UPDATE {DB}.{SCHEMA}.LEASE_STAGING SET STATUS = 'REJECTED' WHERE STAGING_ID = {sid}"
                                )
                                load_staging.clear()
                                st.warning(f"Rejected: {fname}")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Reject failed: {e}")
                    with bcol3:
                        if st.button("Re-extract", key=f"reextract_{sid}"):
                            try:
                                run_dml(f"""
                                    UPDATE {DB}.{SCHEMA}.LEASE_STAGING
                                    SET RAW_EXTRACT = (
                                        SELECT AI_EXTRACT(
                                            text => p.CONTENT,
                                            responseFormat => {{
                                                'store_name': 'Name of the retail store or outlet',
                                                'mall_name': 'Name of the mall or shopping center',
                                                'city': 'City where property is located',
                                                'state': 'State where property is located',
                                                'carpet_area_sqft': 'Carpet area in square feet. Number only.',
                                                'lessor_name': 'Lessor / landlord name',
                                                'lessee_name': 'Lessee / tenant name',
                                                'brand': 'Brand name operated in premises',
                                                'lease_start_date': 'Commencement date YYYY-MM-DD',
                                                'lease_end_date': 'End date YYYY-MM-DD',
                                                'lease_tenure_months': 'Total tenure months. Number only.',
                                                'lock_in_period_months': 'Lock-in months. Number only.',
                                                'notice_period_days': 'Notice period days. Number only.',
                                                'rent_free_period_months': 'Rent-free or fit-out period months. Number only.',
                                                'rent_model': 'FIXED, REVENUE_SHARE, or HYBRID',
                                                'fixed_rent_monthly': 'Monthly fixed rent. Number only.',
                                                'revenue_share_pct': 'Revenue share percentage. Number only.',
                                                'minimum_guarantee_monthly': 'Minimum guarantee per month. Number only.',
                                                'rate_per_sqft': 'Rent rate per sqft per month. Number only.',
                                                'escalation_pct': 'Escalation percentage. Number only.',
                                                'escalation_frequency_months': 'Escalation frequency months. Number only.',
                                                'cam_monthly': 'CAM per month total. Number only.',
                                                'hvac_monthly': 'HVAC per month. Number only.',
                                                'marketing_contribution_monthly': 'Marketing contribution monthly. Number only.',
                                                'security_deposit': 'Total security deposit. Number only.',
                                                'stamp_duty': 'Stamp duty amount. Number only.',
                                                'registration_cost': 'Registration cost. Number only.',
                                                'capex_reimbursement': 'Capex reimbursement. Number only.',
                                                'payment_due_day': 'Day of month payment due. Number only.',
                                                'renewal_option': 'AUTO, MUTUAL, or NONE',
                                                'renewal_notice_days': 'Renewal notice days. Number only.'
                                            }}
                                        ):response
                                        FROM {DB}.{SCHEMA}.PARSED_DOCS p
                                        WHERE p.FILE_NAME = '{fname}'
                                        LIMIT 1
                                    ),
                                    STATUS = 'PENDING_REVIEW',
                                    EXTRACTED_AT = CURRENT_TIMESTAMP()
                                    WHERE STAGING_ID = {sid}
                                """)
                                load_staging.clear()
                                st.success(f"Re-extracted: {fname}. Refresh to see updated values.")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Re-extract failed: {e}")

            if len(approved) > 0:
                st.markdown("---")
                st.subheader("Recently Approved")
                st.dataframe(
                    approved[["STAGING_ID", "FILE_NAME", "STATUS", "EXTRACTED_AT"]],
                    hide_index=True, use_container_width=True,
                )
        else:
            st.info("No staged records yet. Upload lease PDFs in the **Upload & Extract** tab.")
    except Exception as e:
        st.error(f"Error loading staging data: {e}")

elif page == "Cash Flow":
    st.title("Cash Flow Projections")

    with st.sidebar:
        st.markdown("---")
        projection_months = st.slider("Projection Horizon (months)", 6, 36, 12)
        selected_brands = st.multiselect(
            "Filter by Brand",
            df["BRAND"].unique().tolist(),
            default=df["BRAND"].unique().tolist(),
        )

    filtered = df[df["BRAND"].isin(selected_brands)]
    active_leases = filtered[filtered["STATUS"].isin(["ACTIVE", "EXPIRING"])]

    monthly_data = []
    for month_offset in range(projection_months):
        proj_date = today + timedelta(days=30 * month_offset)
        month_label = proj_date.strftime("%Y-%m")
        month_rent = 0
        month_cam = 0
        month_other = 0

        for _, lease in active_leases.iterrows():
            start = lease["LEASE_START_DATE"]
            end = lease["LEASE_END_DATE"]
            if start is None or end is None or pd.isna(start) or pd.isna(end):
                continue
            if hasattr(start, "date"):
                start = start.date()
            if hasattr(end, "date"):
                end = end.date()

            try:
                if start <= proj_date <= end:
                    base_rent = float(lease["TOTAL_MONTHLY_OUTGO"] or 0)
                    cam = float(lease["CAM_MONTHLY"] or 0) + float(lease["HVAC_MONTHLY"] or 0)
                    esc_pct = float(lease["ESCALATION_PCT"] or 0)
                    esc_freq = _safe_int(lease["ESCALATION_FREQUENCY_MONTHS"]) or 12

                    months_since_start = (proj_date.year - start.year) * 12 + proj_date.month - start.month
                    escalations = months_since_start // esc_freq if esc_freq > 0 else 0
                    escalated_rent = base_rent * ((1 + esc_pct / 100) ** escalations)

                    month_rent += escalated_rent
                    month_cam += cam
                    month_other += float(lease["MARKETING_CONTRIBUTION_MONTHLY"] or 0) + float(lease["OTHER_CHARGES_MONTHLY"] or 0)
            except (TypeError, ValueError):
                continue

        monthly_data.append({
            "Month": month_label,
            "Rent": month_rent,
            "CAM_HVAC": month_cam,
            "Other": month_other,
            "Total": month_rent + month_cam + month_other,
        })

    proj_df = pd.DataFrame(monthly_data)

    c1, c2, c3 = st.columns(3)
    c1.metric("Current Month", f"INR {proj_df.iloc[0]['Total']:,.0f}")
    c2.metric(f"Month {projection_months}", f"INR {proj_df.iloc[-1]['Total']:,.0f}",
              f"{((proj_df.iloc[-1]['Total'] / max(proj_df.iloc[0]['Total'], 1)) - 1) * 100:+.1f}%")
    c3.metric(f"Total {projection_months}mo Outgo", f"INR {proj_df['Total'].sum():,.0f}")

    st.markdown("---")
    st.subheader("Monthly Cash Flow Projection")
    chart_df = proj_df.set_index("Month")[["Rent", "CAM_HVAC", "Other"]]
    st.area_chart(chart_df)

    st.subheader("Brand-wise Monthly Projection")
    brand_proj = []
    for brand in selected_brands:
        brand_leases = active_leases[active_leases["BRAND"] == brand]
        brand_total = brand_leases["TOTAL_MONTHLY_OUTGO"].sum()
        brand_proj.append({
            "Brand": brand,
            "Active Stores": len(brand_leases),
            "Current Monthly (INR)": float(brand_total),
            "Annual Projection (INR)": float(brand_total * 12),
        })
    st.dataframe(pd.DataFrame(brand_proj), hide_index=True, use_container_width=True)

    st.subheader("Detailed Projection Table")
    st.dataframe(proj_df, hide_index=True, use_container_width=True)

elif page == "Alerts & Renewals":
    st.title("Alerts & Renewal Tracker")

    alert_data = []
    for _, lease in df.iterrows():
        end_date = lease["LEASE_END_DATE"]
        if end_date is None or pd.isna(end_date):
            continue
        if hasattr(end_date, "date"):
            end_date = end_date.date()
        try:
            days_to_expiry = (end_date - today).days
        except (TypeError, ValueError):
            continue

        lockin_date = lease["LOCK_IN_EXPIRY_DATE"]
        if lockin_date is not None and not pd.isna(lockin_date):
            if hasattr(lockin_date, "date"):
                lockin_date = lockin_date.date()
            days_to_lockin = (lockin_date - today).days
        else:
            lockin_date = None
            days_to_lockin = None

        esc_date = lease["NEXT_ESCALATION_DATE"]
        if esc_date is not None and not pd.isna(esc_date):
            if hasattr(esc_date, "date"):
                esc_date = esc_date.date()
            days_to_esc = (esc_date - today).days
        else:
            esc_date = None
            days_to_esc = None

        alert_data.append({
            "Store": lease["STORE_NAME"],
            "Brand": lease["BRAND"],
            "Mall": lease["MALL_NAME"],
            "City": lease["CITY"],
            "Lease End": end_date,
            "Days to Expiry": days_to_expiry,
            "Lock-in Expiry": lockin_date,
            "Days to Lock-in": days_to_lockin,
            "Next Escalation": esc_date,
            "Days to Escalation": days_to_esc,
            "Renewal Option": lease["RENEWAL_OPTION"],
            "Status": lease["STATUS"],
        })

    alert_df = pd.DataFrame(alert_data)

    lease_expiry_90 = alert_df[(alert_df["Days to Expiry"] <= 90) & (alert_df["Days to Expiry"] > 0)]
    lease_expiry_180 = alert_df[(alert_df["Days to Expiry"] <= 180) & (alert_df["Days to Expiry"] > 90)]
    lockin_expiring = alert_df[alert_df["Days to Lock-in"].notna() & (alert_df["Days to Lock-in"] <= 90) & (alert_df["Days to Lock-in"] > 0)]
    esc_upcoming = alert_df[alert_df["Days to Escalation"].notna() & (alert_df["Days to Escalation"] <= 60) & (alert_df["Days to Escalation"] > 0)]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Expiring <90 days", len(lease_expiry_90))
    c2.metric("Expiring <180 days", len(lease_expiry_180))
    c3.metric("Lock-in Expiring <90d", len(lockin_expiring))
    c4.metric("Escalation <60d", len(esc_upcoming))

    if len(lease_expiry_90) > 0:
        st.markdown("---")
        st.subheader("CRITICAL: Leases Expiring in <90 Days")
        st.dataframe(
            lease_expiry_90[["Store", "Brand", "Mall", "City", "Lease End", "Days to Expiry", "Renewal Option"]].sort_values("Days to Expiry"),
            hide_index=True, use_container_width=True,
        )

    if len(lease_expiry_180) > 0:
        st.markdown("---")
        st.subheader("WARNING: Leases Expiring in 90-180 Days")
        st.dataframe(
            lease_expiry_180[["Store", "Brand", "Mall", "City", "Lease End", "Days to Expiry", "Renewal Option"]].sort_values("Days to Expiry"),
            hide_index=True, use_container_width=True,
        )

    if len(lockin_expiring) > 0:
        st.markdown("---")
        st.subheader("Lock-in Period Expiring in <90 Days")
        st.dataframe(
            lockin_expiring[["Store", "Brand", "Mall", "City", "Lock-in Expiry", "Days to Lock-in"]].sort_values("Days to Lock-in"),
            hide_index=True, use_container_width=True,
        )

    if len(esc_upcoming) > 0:
        st.markdown("---")
        st.subheader("Upcoming Escalations (<60 Days)")
        st.dataframe(
            esc_upcoming[["Store", "Brand", "Mall", "City", "Next Escalation", "Days to Escalation"]].sort_values("Days to Escalation"),
            hide_index=True, use_container_width=True,
        )

    st.markdown("---")
    st.subheader("Full Renewal Calendar")
    st.dataframe(
        alert_df.sort_values("Days to Expiry")[["Store", "Brand", "Mall", "City", "Lease End", "Days to Expiry", "Renewal Option", "Status"]],
        hide_index=True, use_container_width=True,
    )

elif page == "Regional Analytics":
    st.title("Regional Analytics")

    with st.sidebar:
        st.markdown("---")
        region_filter = st.multiselect("Filter Regions", df["REGION"].unique().tolist(), default=df["REGION"].unique().tolist())

    filtered = df[df["REGION"].isin(region_filter)]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Stores", len(filtered))
    c2.metric("Total Monthly Outgo", f"INR {filtered['TOTAL_MONTHLY_OUTGO'].sum():,.0f}")
    c3.metric("Total Area", f"{filtered['CARPET_AREA_SQFT'].sum():,.0f} sq ft")
    c4.metric("Avg Rate/sq ft", f"INR {filtered['RATE_PER_SQFT'].mean():,.0f}")

    st.markdown("---")
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Monthly Outgo by City")
        city_rent = filtered.groupby("CITY")["TOTAL_MONTHLY_OUTGO"].sum().sort_values(ascending=False)
        st.bar_chart(city_rent)

    with col2:
        st.subheader("Stores by City")
        city_count = filtered.groupby("CITY")["LEASE_ID"].count().sort_values(ascending=False)
        st.bar_chart(city_count)

    col3, col4 = st.columns(2)

    with col3:
        st.subheader("Rate per Sq Ft by City")
        rate_data = filtered.groupby("CITY")["RATE_PER_SQFT"].mean().sort_values(ascending=False)
        st.bar_chart(rate_data)

    with col4:
        st.subheader("Security Deposits by Region")
        deposit_data = filtered.groupby("REGION")["SECURITY_DEPOSIT"].sum()
        st.bar_chart(deposit_data)

    st.markdown("---")
    st.subheader("State-wise Lease Summary")
    state_summary = (
        filtered.groupby(["STATE", "REGION"])
        .agg(Stores=("LEASE_ID", "count"), Total_Area=("CARPET_AREA_SQFT", "sum"),
             Monthly_Outgo=("TOTAL_MONTHLY_OUTGO", "sum"), Avg_Rate=("RATE_PER_SQFT", "mean"),
             Total_Deposits=("SECURITY_DEPOSIT", "sum"))
        .reset_index().sort_values("Monthly_Outgo", ascending=False)
    )
    st.dataframe(state_summary, hide_index=True, use_container_width=True)

    st.subheader("Mall-wise Details")
    mall_data = (
        filtered.groupby(["MALL_NAME", "CITY"])
        .agg(Brands=("BRAND", lambda x: ", ".join(sorted(x.unique()))),
             Stores=("LEASE_ID", "count"), Total_Area=("CARPET_AREA_SQFT", "sum"),
             Monthly_Outgo=("TOTAL_MONTHLY_OUTGO", "sum"))
        .reset_index().sort_values("Monthly_Outgo", ascending=False)
    )
    st.dataframe(mall_data, hide_index=True, use_container_width=True)
