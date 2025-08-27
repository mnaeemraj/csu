from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import JSONResponse
import pandas as pd
import random

# =============================
# Init FastAPI app
# =============================
app = FastAPI()

# =============================
# Load PStations sheet
# =============================
PSTATION_FILE = "./data/Punjab_Hierarchy_v2.xlsx"   # adjust path
try:
    df_pstations = pd.read_excel(PSTATION_FILE, sheet_name="PStations")
    df_pstations.columns = df_pstations.columns.str.strip().str.lower()
except Exception as e:
    print(f"⚠ Could not load PStations sheet: {e}")
    df_pstations = pd.DataFrame()


@app.get("/circle_lookup")
def circle_lookup(circle_id: int = Query(..., description="Circle ID to lookup")):
    if df_pstations.empty:
        raise HTTPException(status_code=500, detail="PStations sheet not loaded")

    # Search for Circle
    row = df_pstations[df_pstations["circle id"] == circle_id]
    if row.empty:
        raise HTTPException(status_code=404, detail=f"No record found for Circle ID {circle_id}")

    # Extract details
    circle_name = row.iloc[0].get("circle name", "Unknown")
    district_name = row.iloc[0].get("district name", "Unknown")
    region_name = row.iloc[0].get("region name", "Unknown")

    # Find all circles under the same Region (remove duplicates!)
    circles_in_region = (
        df_pstations[df_pstations["region name"] == region_name][
            ["circle id", "circle name", "district name"]
        ]
        .drop_duplicates()
        .to_dict(orient="records")
    )

    # 🔹 Add random FIR registered value (12,000 – 14,500) for each circle
    for c in circles_in_region:
        c["fir_registered"] = random.randint(12000, 14500)

    # 🔹 Rank circles by FIR registered (higher = better rank)
    circles_in_region = sorted(
        circles_in_region, key=lambda x: x["fir_registered"], reverse=True
    )
    for rank, c in enumerate(circles_in_region, start=1):
        c["rank"] = rank

    # Get selected circle (with updated FIR + rank)
    selected_circle = next((c for c in circles_in_region if c["circle id"] == circle_id), None)

    response = {
        "selected_circle": selected_circle,
        "hierarchy": f"{region_name} - {district_name}",
        "all_circles_in_region": circles_in_region
    }

    return JSONResponse(content=response)


# Run using: uvicorn filename:app --reload