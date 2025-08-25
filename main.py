from flask import Flask, jsonify, request
import pandas as pd

app = Flask(__name__)

# ======================
# CONFIG
# ======================
henious_file = "./data/Henious.xlsx"
hierarchy_file = "./data/Punjab_Hierarchy_v2.xlsx"

# Columns to keep from Henious file (normalized)
VALID_COLUMNS = [
    "region",
    "district",
    "total applications received",
    "pending applications",
    "pending applications %",
    "completed applications",
    "completed applications %",
    "filed applications",
    "filed applications %",
    "fir registered (heinous crime)",
    "fir registered (heinous crime) %",
    "fir registered (in timeline)",
    "fir registered (in timeline) %",
    "fir registered (out of timeline)",
    "fir registered (out of timeline) %",
    "fir not registered",
    "fir not registered %",
    "disposed within timelines",
    "disposed within timelines %",
    "disposed out of timelines",
    "disposed out of timelines %"
]
VALID_COLUMNS = [c.strip().lower() for c in VALID_COLUMNS]

# ======================
# TIME SHEET MAPPING
# ======================
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

    # normalize all column names
    data.columns = data.columns.str.strip().str.lower()

    # keep only valid columns
    cols_to_use = [col for col in data.columns if col in VALID_COLUMNS]
    data = data[cols_to_use].copy()

    # normalize text columns
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

# Normalize names
hierarchy["region_name"] = hierarchy["region name"].astype(str).str.strip()
hierarchy["district_name"] = hierarchy["district name"].astype(str).str.strip()
hierarchy["region_norm"] = hierarchy["region name"].astype(str).str.strip().str.lower()
hierarchy["district_norm"] = hierarchy["district name"].astype(str).str.strip().str.lower()

# ======================
# HELPERS
# ======================
def convert_types(summary_dict):
    """Ensure numpy types are converted to JSON-serializable native types."""
    clean_dict = {}
    for k, v in summary_dict.items():
        if pd.isna(v):
            clean_dict[k] = None
        elif isinstance(v, (int, float, str)):
            clean_dict[k] = v
        else:
            try:
                clean_dict[k] = v.item()  # works for numpy.int64/float64
            except:
                clean_dict[k] = str(v)
    return clean_dict


def get_region_list():
    regions = hierarchy[["region id", "region_name"]].drop_duplicates()
    return [
        {"region_id": int(r["region id"]), "region_name": r["region_name"]}
        for _, r in regions.iterrows()
    ]


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
            if pd.api.types.is_numeric_dtype(region_data[col]):
                summary[col] = region_data[col].sum()
            else:
                summary[col] = ", ".join(region_data[col].dropna().unique().astype(str))

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
            if pd.api.types.is_numeric_dtype(district_data[col]):
                summary[col] = district_data[col].sum()
            else:
                summary[col] = ", ".join(district_data[col].dropna().unique().astype(str))

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
