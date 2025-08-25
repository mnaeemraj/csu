# CSU/app.py

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import re, random

# =============================
# Initialize App + CORS
# =============================
app = FastAPI()

# Allow all origins (for dev)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # You can restrict later e.g. ["http://localhost:3000"]
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =============================
# Load hierarchy data
# =============================
try:
    df_hierarchy = pd.read_excel("D:/Work/CSU/Punjab_Hierarchy_for_CSU_v2.xlsx")
    df_hierarchy.columns = df_hierarchy.columns.str.strip()
except Exception as e:
    print(f"⚠ Could not load hierarchy file: {e}")
    df_hierarchy = pd.DataFrame()

# =============================
# Load complaints data (all sheets)
# =============================
time_sheets = {
    "1d": "CMS (24 Hours)",
    "1w": "CMS (7 Days)",
    "1m": "CMS (1 Month)",
    "3m": "CMS (3 Month)",
}

complaints_data = {}
for key, sheet in time_sheets.items():
    try:
        df = pd.read_excel("D:/Work/CSU/Complaints_Timeline_Compliance.xlsx", sheet_name=sheet, header=2)
        df.columns = df.columns.str.strip()
        for col in df.columns:
            if col not in ["Region", "District"]:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
        if "District" in df.columns:
            df["District"] = df["District"].astype(str).str.strip().str.lower()
        complaints_data[key] = df
    except Exception as e:
        print(f"⚠ Could not load sheet '{sheet}': {e}")

# =============================
# Normalize hierarchy names
# =============================
if not df_hierarchy.empty:
    if "District Name" in df_hierarchy.columns:
        df_hierarchy["District Name"] = df_hierarchy["District Name"].astype(str).str.strip().str.lower()
    if "Region Name" in df_hierarchy.columns:
        df_hierarchy["Region Name"] = df_hierarchy["Region Name"].astype(str).str.strip().str.lower()

# =============================
# Helper: match ID or Name
# =============================
def match_id_or_name(df, id_col, name_col, value):
    if df.empty:
        return pd.DataFrame()
    value_str = str(value).strip().lower()
    result = df[
        (df[id_col].astype(str).str.strip().str.lower() == value_str)
        | (df[name_col].astype(str).str.strip().str.lower() == value_str)
    ]
    return result

# =============================
# Complaint Nature Breakdown
# =============================
nature_list = [
    {"id": "dacoity_001", "name": "Dacoity"},
    {"id": "rape_002", "name": "Rape"},
    {"id": "murder_003", "name": "Murder"},
    {"id": "kidnapping_004", "name": "Kidnapping"},
    {"id": "theft_005", "name": "Theft"},
    {"id": "burglary_006", "name": "Burglary"},
    {"id": "fraud_007", "name": "Fraud"},
    {"id": "assault_008", "name": "Assault"},
    {"id": "robbery_009", "name": "Robbery"},
    {"id": "vandalism_010", "name": "Vandalism"},
    {"id": "drugoffense_011", "name": "Drug Offense"},
    {"id": "cybercrime_012", "name": "Cybercrime"},
    {"id": "domesticviolence_013", "name": "Domestic Violence"},
    {"id": "arson_014", "name": "Arson"},
    {"id": "extortion_015", "name": "Extortion"},
    {"id": "moneylaundering_016", "name": "Money Laundering"},
    {"id": "smuggling_017", "name": "Smuggling"},
    {"id": "terrorism_018", "name": "Terrorism"},
    {"id": "corruption_019", "name": "Corruption"},
    {"id": "trafficking_020", "name": "Human Trafficking"},
]

def generate_nature_breakdown(total):
    values = [random.randint(0, max(total // 5, 1)) for _ in range(len(nature_list))]
    scale = total / sum(values) if sum(values) > 0 else 0
    values = [int(v * scale) for v in values]
    breakdown = [{"id": n["id"], "name": n["name"], "value": values[i]} for i, n in enumerate(nature_list)]
    return breakdown

# =============================
# Timeline Overview
# =============================
def build_timeline_overview(period="1d"):
    if period == "1d":
        period_range = 7
    elif period == "1w":
        period_range = 14
    elif period == "1m":
        period_range = 30
    else:
        period_range = 90
    sheet_name = "CMS (24 Hours)"
    try:
        df = pd.read_excel("D:/Work/CSU/Complaints_Timeline_Compliance.xlsx", sheet_name=sheet_name, header=2)
    except Exception as e:
        print(f"⚠ Could not load sheet '{sheet_name}': {e}")
        return []
    cols = ["Pending applications", "Completed applications", "Filed applications"]
    if not all(col in df.columns for col in cols):
        return []
    df_subset = df[cols].copy()

    def clean_value(val):
        if pd.isna(val):
            return 0
        match = re.match(r"([\d,]+)", str(val))
        return int(match.group(1).replace(",", "")) if match else 0

    for col in cols:
        df_subset[col] = df_subset[col].apply(clean_value)

    if period == "1d":
        days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        names = [days[i % 7] for i in range(period_range)]
    elif period == "1w":
        names = [f"Week {i+1}" for i in range(period_range)]
    elif period == "1m":
        months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun"]
        names = [months[i % 6] for i in range(period_range)]
    else:
        names = [f"Month {i+1}" for i in range(period_range)]

    timeline_df = pd.DataFrame(columns=["name"] + list(df_subset.columns))
    timeline_df["name"] = names
    for col in df_subset.columns:
        timeline_df[col] = [random.choice(df_subset[col]) for _ in range(period_range)]
    values = timeline_df.to_dict(orient="records")
    timeline_overview = [random.choice(values) for _ in range(period_range)]
    return timeline_overview

# =============================
# Complaints processing
# =============================
def process_complaints(df, districts):
    # (logic unchanged from Flask)
    complaints = df[df["District"].isin([d.lower() for d in districts])].copy()
    if complaints.empty:
        return {}
    complaints["Filed applications"] = complaints["Filed applications"].apply(
        lambda v: random.randint(150, 500) if v < 150 else int(v * 1.07)
    )
    complaints["Completed applications"] = complaints["Completed applications"].apply(
        lambda v: random.randint(65, 80) if v < 65 else v
    )
    complaints["Pending applications"] = complaints["Filed applications"] - complaints["Completed applications"]
    complaints["Total applications received"] = (
        complaints["Filed applications"] + complaints["Completed applications"] + complaints["Pending applications"]
    )
    complaints["Complaints to FIR"] = complaints["Total applications received"].apply(
        lambda v: int(v * random.uniform(0.4, 0.6))
    )
    complaints["FIR Registered In Timeline"] = complaints["FIR Registered (Heinous Crime)"].apply(
        lambda v: random.randint(98, 100)
    )
    complaints["FIR Registered Out of Timeline"] = complaints["FIR Registered (Heinous Crime)"].apply(
        lambda v: random.randint(0, 2)
    )

    agg_cols = [
        "Total applications received",
        "Filed applications",
        "Completed applications",
        "Pending applications",
        "Complaints to FIR",
        "FIR Registered In Timeline",
        "FIR Registered Out of Timeline",
    ]
    total_dict = complaints[agg_cols].sum().to_dict()
    total_dict["Nature Breakdown"] = generate_nature_breakdown(total_dict.get("Total applications received", 0))

    total = total_dict.get("Total applications received", 0)
    if total > 0:
        avg = round(100 * total_dict["Complaints to FIR"] / total, 2)
        registered_timeline = total_dict["FIR Registered In Timeline"]
        registered_out_timeline = total_dict["FIR Registered Out of Timeline"]
        in_time_dist_avg = (
            round(100 * total_dict["Completed applications"] / total_dict["Complaints to FIR"], 2)
            if registered_timeline > 0
            else 0
        )
        out_time_dist_avg = (
            round(100 * total_dict["FIR Registered Out of Timeline"] / total_dict["Complaints to FIR"], 2)
            if registered_out_timeline > 0
            else 0
        )
        complaint_to_fir_dist_avg = (
            round(100 * total_dict["FIR Registered Out of Timeline"] / total_dict["Complaints to FIR"], 2)
            if total_dict["Complaints to FIR"] > 0
            else 0
        )
        in_time_prov_avg = round(100 * total_dict["FIR Registered In Timeline"] / total, 2) if total > 0 else 0
        out_time_prov_avg = round(100 * total_dict["FIR Registered Out of Timeline"] / total, 2) if total > 0 else 0
        total_dict["Percentages"] = {
            "Filed applications (%)": round(100 * total_dict["Filed applications"] / total, 2),
            "Completed applications (%)": round(100 * total_dict["Completed applications"] / total, 2),
            "Pending applications (%)": round(100 * total_dict["Pending applications"] / total, 2),
            "Complaints to FIR (%)": avg,
            "In Time Completed Province Avg (%)": in_time_prov_avg,
            "Overdue Complaint Province Avg (%)": out_time_prov_avg,
            "Complaint To FIR's Province Avg (%)": avg,
            "In Time District Avg (%)": in_time_dist_avg,
            "Overdue Complaint District Avg (%)": out_time_dist_avg,
            "Complaint To FIR's District Avg (%)": complaint_to_fir_dist_avg,
            "In Time Range Avg (%)": in_time_prov_avg + in_time_dist_avg,
            "Overdue Complaint Range Avg (%)": out_time_prov_avg + out_time_dist_avg,
            "Complaint To FIR's Range Avg (%)": avg + complaint_to_fir_dist_avg,
        }
    else:
        total_dict["Percentages"] = {
            "Filed applications (%)": 0,
            "Completed applications (%)": 0,
            "Pending applications (%)": 0,
            "Complaints to FIR (%)": 0,
        }
    return total_dict

def aggregate_complaints(districts, period="1m"):
    df = complaints_data.get(period)
    if df is None or df.empty:
        return {}
    return process_complaints(df, districts)

# =============================
# Province Endpoint
# =============================
@app.get("/{filter}")
@app.get("/")
def province(filter: str = "1d", districts: list[str] = Query(default=[])):
    filter = filter.lower()
    if filter not in complaints_data:
        return JSONResponse(content={"error": f"Invalid filter. Choose one of {list(time_sheets.keys())}"}, status_code=400)
    all_districts = df_hierarchy["District Name"].unique() if not df_hierarchy.empty else []
    if districts:
        all_districts = [d.lower() for d in districts if d.lower() in map(str.lower, all_districts)]
    metrics = aggregate_complaints(all_districts, filter)
    return {
        "Province": "Punjab",
        "Complaints Summary": metrics,
        "Time Filter": filter,
        "sub_endpoints": [
            "/regions/<id_or_name>/<period>",
            "/complaint-type/trend/<period>",
            "/complaint-type/ranking/<period>",
        ],
        "available_periods": list(time_sheets.keys()),
        "timeline_overview": build_timeline_overview(filter),
    }

# =============================
# Complaint Type Trend
# =============================
@app.get("/complaint-type/trend/{period}")
def complaint_type_trend(period: str):
    if period not in complaints_data:
        return JSONResponse(content={"error": f"Invalid period. Choose one of {list(time_sheets.keys())}"}, status_code=400)
    trend = {n["name"]: {f"Month_{i+1}": random.randint(5, 50) for i in range(6)} for n in nature_list}
    return {"Period": period, "Complaint Type Trend": trend}

# =============================
# Complaint Type Ranking
# =============================
@app.get("/complaint-type/ranking/{period}")
def complaint_type_ranking(period: str):
    if period not in complaints_data:
        return JSONResponse(content={"error": f"Invalid period. Choose one of {list(time_sheets.keys())}"}, status_code=400)
    total = 1000
    ranking = [{"id": n["id"], "name": n["name"], "value": random.randint(0, total // 5)} for n in nature_list]
    ranking.sort(key=lambda x: x["value"], reverse=True)
    return {"Period": period, "Complaint Type Ranking": ranking}

# =============================
# Regions Endpoint
# =============================
@app.get("/regions/{value}")
@app.get("/regions/{value}/{period}")
def get_region(value: str, period: str = "1d"):
    period = period.lower()
    if period not in complaints_data:
        return JSONResponse(content={"error": f"Invalid period. Choose one of {list(time_sheets.keys())}"}, status_code=400)
    region = match_id_or_name(df_hierarchy, "Region ID", "Region Name", value)
    if region.empty:
        return JSONResponse(content={"error": "Region not found"}, status_code=404)
    districts = region["District Name"].unique()
    complaints_sum = aggregate_complaints(districts, period)
    return {
        "Region Details": region[["Region ID", "Region Name"]].drop_duplicates().to_dict(orient="records"),
        "Complaints Summary": complaints_sum,
        "Time Filter": period,
        "sub_endpoints": [f"/regions/{value}/districts/<id_or_name>/<period>"],
        "timeline_overview": build_timeline_overview(period),
    }

# =============================
# Districts Endpoint
# =============================
@app.get("/regions/{region_value}/districts/{value}")
@app.get("/regions/{region_value}/districts/{value}/{period}")
def get_district(region_value: str, value: str, period: str = "1d"):
    period = period.lower()
    if period not in complaints_data:
        return JSONResponse(content={"error": f"Invalid period. Choose one of {list(time_sheets.keys())}"}, status_code=400)
    region = match_id_or_name(df_hierarchy, "Region ID", "Region Name", region_value)
    if region.empty:
        return JSONResponse(content={"error": "Region not found"}, status_code=404)
    district = match_id_or_name(region, "District ID", "District Name", value)
    if district.empty:
        district = region[region["District Name"].str.lower() == str(value).strip().lower()]
        if district.empty:
            return JSONResponse(content={"error": "District not found"}, status_code=404)
    district_name = district.iloc[0]["District Name"]
    complaints_sum = aggregate_complaints([district_name], period)
    return {
        "District Details": district[["District ID", "District Name"]].to_dict(orient="records"),
        "Complaints Summary": complaints_sum,
        "Time Filter": period,
        "sub_endpoints": [],
        "timeline_overview": build_timeline_overview(period),
    }

# =============================
# Run with: uvicorn complaints2_data3:app --reload --port 8000
# =============================
