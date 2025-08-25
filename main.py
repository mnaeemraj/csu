from flask import Flask, jsonify, request
import pandas as pd

app = Flask(__name__)

# ======================
# CONFIG
# ======================
henious_file = "./data/Henious.xlsx"
hierarchy_file = "./data/Punjab_Hierarchy_v2.xlsx"

# Columns to keep (normalized)
VALID_COLUMNS = [
    "region",
    "district",
    "total applications received",
    "pending applications",
    "completed applications",
    "filed applications",
    "fir registered (heinous crime)",
    "fir registered (in timeline)",
    "fir registered (out of timeline)",
    "fir not registered",
    "disposed within timelines",
    "disposed out of timelines"
]
VALID_COLUMNS = [c.strip().lower() for c in VALID_COLUMNS]

# Time filter → sheet map
TIME_SHEETS = {
    "1d": "1 Day (Henious)",
    "1w": "1 Week (Henious)",
    "1m": "1 Month (Henious)",
    "3m": "3 Month (Henious)"
}

# ======================
# LOAD HENIOUS DATA
# ======================
henious_sheets = pd.read_excel(henious_file, sheet_name=None)
henious_data = {}

for key, sheet in TIME_SHEETS.items():
    if sheet not in henious_sheets:
        continue
    data = henious_sheets[sheet]

    # normalize columns
    data.columns = (
        data.columns.str.strip()
        .str.lower()
        .str.replace("\n", " ")
        .str.replace(" +", " ", regex=True)
    )

    # keep valid cols
    cols_to_use = [col for col in data.columns if col in VALID_COLUMNS]
    data = data[cols_to_use].copy()

    # normalize text
    if "region" in data.columns:
        data["region"] = data["region"].astype(str).str.strip().str.lower()
    if "district" in data.columns:
        data["district"] = data["district"].astype(str).str.strip().str.lower()

    henious_data[key] = data

# ======================
# LOAD HIERARCHY
# ======================
hierarchy = pd.read_excel(hierarchy_file, sheet_name="PStations")
hierarchy.columns = hierarchy.columns.str.strip().str.lower()

hierarchy["region_name"] = hierarchy["region name"].astype(str).str.strip()
hierarchy["district_name"] = hierarchy["district name"].astype(str).str.strip()
hierarchy["region_norm"] = hierarchy["region name"].astype(str).str.strip().str.lower()
hierarchy["district_norm"] = hierarchy["district name"].astype(str).str.strip().str.lower()

# ======================
# HELPERS
# ======================
def to_number(val):
    """Convert value safely to float."""
    try:
        if pd.isna(val):
            return 0
        return float(str(val).replace(",", "").strip())
    except Exception:
        return 0

def safe_pct(num, den):
    num, den = to_number(num), to_number(den)
    return round((num / den) * 100, 2) if den else 0

def recalc_percentages(summary):
    """Recalculate all % values correctly from raw numbers."""
    total_apps = to_number(summary.get("total applications received", 0))

    summary["pending applications %"] = safe_pct(summary.get("pending applications", 0), total_apps)
    summary["completed applications %"] = safe_pct(summary.get("completed applications", 0), total_apps)
    summary["filed applications %"] = safe_pct(summary.get("filed applications", 0), total_apps)

    fir_total = to_number(summary.get("fir registered (heinous crime)", 0)) + to_number(summary.get("fir not registered", 0))
    summary["fir registered (heinous crime) %"] = safe_pct(summary.get("fir registered (heinous crime)", 0), fir_total)
    summary["fir not registered %"] = safe_pct(summary.get("fir not registered", 0), fir_total)

    fir_reg_total = to_number(summary.get("fir registered (heinous crime)", 0))
    summary["fir registered (in timeline) %"] = safe_pct(summary.get("fir registered (in timeline)", 0), fir_reg_total)
    summary["fir registered (out of timeline) %"] = safe_pct(summary.get("fir registered (out of timeline)", 0), fir_reg_total)

    disposed_total = to_number(summary.get("disposed within timelines", 0)) + to_number(summary.get("disposed out of timelines", 0))
    summary["disposed within timelines %"] = safe_pct(summary.get("disposed within timelines", 0), disposed_total)
    summary["disposed out of timelines %"] = safe_pct(summary.get("disposed out of timelines", 0), disposed_total)

    return summary

def convert_types(summary_dict):
    """Make JSON serializable."""
    clean_dict = {}
    for k, v in summary_dict.items():
        if pd.isna(v):
            clean_dict[k] = None
        elif isinstance(v, (int, float, str)):
            clean_dict[k] = v
        else:
            try:
                clean_dict[k] = v.item()
            except Exception:
                clean_dict[k] = str(v)
    return clean_dict

# ======================
# LIST HELPERS
# ======================
def get_region_list():
    regions = hierarchy[["region id", "region_name"]].drop_duplicates()
    return [{"region_id": int(r["region id"]), "region_name": r["region_name"]} for _, r in regions.iterrows()]

def get_district_list():
    districts = hierarchy[["district id", "district_name", "region id", "region_name"]].drop_duplicates()
    return [
        {
            "district_id": int(r["district id"]),
            "district_name": r["district_name"],
            "region_id": int(r["region id"]),
            "region_name": r["region_name"]
        }
        for _, r in districts.iterrows()
    ]

# ======================
# DETAIL HELPERS
# ======================
def get_region_details(region_key, time_key):
    if time_key not in henious_data:
        return {"error": "Invalid time filter"}

    region_row = hierarchy[
        (hierarchy["region id"].astype(str) == str(region_key)) |
        (hierarchy["region_norm"] == str(region_key).lower())
    ]
    if region_row.empty:
        return None

    region_id = int(region_row.iloc[0]["region id"])
    region_name = region_row.iloc[0]["region_name"]

    districts = region_row["district_norm"].unique().tolist()
    region_data = henious_data[time_key][henious_data[time_key]["district"].isin(districts)]

    if region_data.empty:
        return {"message": "No details found for this region"}

    summary = {}
    for col in VALID_COLUMNS:
        if col in region_data.columns:
            summary[col] = region_data[col].apply(to_number).sum()

    summary = recalc_percentages(summary)

    return {
        "region_id": region_id,
        "region_name": region_name,
        "time_key": time_key,
        "summary": convert_types(summary)
    }

def get_district_details(district_key, time_key):
    if time_key not in henious_data:
        return {"error": "Invalid time filter"}

    district_row = hierarchy[
        (hierarchy["district id"].astype(str) == str(district_key)) |
        (hierarchy["district_norm"] == str(district_key).lower())
    ]
    if district_row.empty:
        return None

    district_id = int(district_row.iloc[0]["district id"])
    district_name = district_row.iloc[0]["district_name"]
    region_id = int(district_row.iloc[0]["region id"])
    region_name = district_row.iloc[0]["region_name"]

    district_data = henious_data[time_key][henious_data[time_key]["district"] == district_name.lower()]

    if district_data.empty:
        return {"message": "No details found for this district"}

    summary = {}
    for col in VALID_COLUMNS:
        if col in district_data.columns:
            summary[col] = district_data[col].apply(to_number).sum()

    summary = recalc_percentages(summary)

    return {
        "district_id": district_id,
        "district_name": district_name,
        "region_id": region_id,
        "region_name": region_name,
        "time_key": time_key,
        "summary": convert_types(summary)
    }

# ======================
# API ENDPOINTS
# ======================
@app.route("/")
def home():
    return "Welcome to Punjab"

@app.route("/regions")
def regions():
    return jsonify(get_region_list())

@app.route("/regions/<region_key>")
def region_details(region_key):
    time_key = request.args.get("time", "1d").lower()
    details = get_region_details(region_key, time_key)
    if details:
        return jsonify(details)
    return jsonify({"error": "Region not found"}), 404

@app.route("/districts")
def districts():
    return jsonify(get_district_list())

@app.route("/districts/<district_key>")
def district_details(district_key):
    time_key = request.args.get("time", "1d").lower()
    details = get_district_details(district_key, time_key)
    if details:
        return jsonify(details)
    return jsonify({"error": "District not found"}), 404

# ======================
# RUN SERVER
# ======================
if __name__ == "__main__":
    app.run(debug=True, port=8000)
