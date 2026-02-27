from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import io

from app.parser import parse_hdfc_pdf
from app.analyzer import analyze_transactions

app = FastAPI(title="Bank Statement Analyzer V1")

MAX_FILE_SIZE = 10 * 1024 * 1024  # 10MB

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -------------------------------
# 📊 SUMMARY FUNCTIONS
# -------------------------------


def generate_summary(df):
    total_income = df["credit"].sum()
    total_expense = df["debit"].sum()
    net_flow = total_income - total_expense

    # Use daily closing balance average (more accurate)
    daily = df.groupby(df["date"].dt.date).last()
    avg_balance = daily["balance"].mean()

    return {
        "total_income": round(total_income, 2),
        "total_expense": round(total_expense, 2),
        "net_flow": round(net_flow, 2),
        "avg_balance": round(avg_balance, 2),
    }


def generate_monthly_summary(df):
    df["month"] = df["date"].dt.to_period("M")

    monthly = df.groupby("month").agg({"credit": "sum", "debit": "sum"}).reset_index()

    monthly["net"] = monthly["credit"] - monthly["debit"]
    monthly["month"] = monthly["month"].astype(str)

    return monthly


# -------------------------------
# 💰 LOAN READINESS ENGINE
# -------------------------------


def generate_loan_readiness(summary, monthly_summary):

    months = len(monthly_summary)

    avg_income = monthly_summary["credit"].mean() if months else 0
    avg_expense = monthly_summary["debit"].mean() if months else 0
    surplus = avg_income - avg_expense

    # Surplus Score (40)
    if surplus > avg_income * 0.25:
        surplus_score = 40
    elif surplus > 0:
        surplus_score = 25
    else:
        surplus_score = 5

    # Income Stability (30)
    variation = monthly_summary["credit"].std() / avg_income if avg_income else 1

    if variation < 0.25:
        stability_score = 30
    elif variation < 0.5:
        stability_score = 15
    else:
        stability_score = 5

    # Balance Health (30)
    closing_balance = summary["closing_balance"]

    if closing_balance > avg_expense:
        balance_score = 30
    elif closing_balance > avg_expense * 0.5:
        balance_score = 15
    else:
        balance_score = 5

    total_score = surplus_score + stability_score + balance_score

    if total_score >= 80:
        rating = "Strong"
    elif total_score >= 60:
        rating = "Moderate"
    elif total_score >= 40:
        rating = "Risky"
    else:
        rating = "High Risk"

    return {
        "loan_score": round(total_score, 2),
        "rating": rating,
        "avg_monthly_income": round(avg_income, 2),
        "avg_monthly_expense": round(avg_expense, 2),
        "monthly_surplus": round(surplus, 2),
    }


# -------------------------------
# 📂 CORE ANALYSIS FUNCTION
# -------------------------------


def process_statement(contents: bytes):
    file_stream = io.BytesIO(contents)

    df = parse_hdfc_pdf(file_stream)

    if df.empty:
        raise HTTPException(400, "No transactions detected.")

    df = df.sort_values(["date"]).reset_index(drop=True)

    # Opening Balance Reconstruction
    first = df.iloc[0]
    last = df.iloc[-1]

    credit = float(first.get("credit", 0) or 0)
    debit = float(first.get("debit", 0) or 0)
    balance = float(first["balance"])

    if credit > 0:
        opening_balance = balance - credit
    else:
        opening_balance = balance + debit

    closing_balance = float(last["balance"])

    summary = generate_summary(df)
    monthly_summary = generate_monthly_summary(df)

    summary["opening_balance"] = round(opening_balance, 2)
    summary["closing_balance"] = round(closing_balance, 2)

    loan_metrics = generate_loan_readiness(summary, monthly_summary)

    return df, summary, monthly_summary, loan_metrics


# -------------------------------
# 📊 ANALYZE API
# -------------------------------


@app.post("/analyze")
async def analyze(file: UploadFile = File(...)):

    if file.content_type != "application/pdf":
        raise HTTPException(400, "Only PDF allowed.")

    contents = await file.read()

    if len(contents) > MAX_FILE_SIZE:
        raise HTTPException(400, "File too large.")

    df, summary, monthly_summary, loan_metrics = process_statement(contents)

    return {
        "summary": summary,
        "loan_analysis": loan_metrics,
        "monthly_summary": monthly_summary.to_dict(orient="records"),
        "total_transactions": len(df),
    }


# -------------------------------
# 📥 DOWNLOAD EXCEL API
# -------------------------------


@app.post("/download-excel")
async def download_excel(file: UploadFile = File(...)):

    contents = await file.read()

    if len(contents) > MAX_FILE_SIZE:
        raise HTTPException(400, "File too large.")

    df, summary, monthly_summary, loan_metrics = process_statement(contents)

    df_export = df.copy()
    df_export["date"] = df_export["date"].dt.strftime("%d-%m-%Y")

    output = io.BytesIO()

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df_export.to_excel(writer, index=False, sheet_name="Transactions")

        pd.DataFrame(summary.items(), columns=["Metric", "Value"]).to_excel(
            writer, index=False, sheet_name="Summary"
        )

        monthly_summary.to_excel(writer, index=False, sheet_name="Monthly")

        pd.DataFrame(loan_metrics.items(), columns=["Metric", "Value"]).to_excel(
            writer, index=False, sheet_name="Loan Analysis"
        )

    output.seek(0)

    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=analysis.xlsx"},
    )
