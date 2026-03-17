from fastapi import FastAPI, UploadFile, File, HTTPException, Form
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import pdfplumber
import io
import re
from datetime import datetime

from app.parser import (
    parse_hdfc_pdf,
    parse_sbi_pdf,
    parse_axis_pdf,
    parse_pnb_pdf,
    parse_ubin_pdf,
    parse_idbi_pdf,
    parse_bob_pdf,          # ← NEW
)
from app.analyzer import analyze_emi
from app.categorizer import add_categories, generate_category_summary

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
# 🏦 ACCOUNT INFO EXTRACTOR
# -------------------------------


def extract_account_info(
    contents: bytes, bank: str, password: str | None = None
) -> dict:
    if bank == "pnb":
        from app.parser import _pnb_open

        pdf_obj = _pnb_open(io.BytesIO(contents), password)
        with pdf_obj as pdf:
            raw_text = pdf.pages[0].extract_text() or ""
    else:
        with pdfplumber.open(io.BytesIO(contents), password=password) as pdf:
            raw_text = pdf.pages[0].extract_text() or ""
            words = pdf.pages[0].extract_words()

    if bank not in ("pnb", "idbi", "bob"):
        rows = {}
        for w in words:
            rows.setdefault(round(w["top"], 0), []).append(w)

    if bank == "hdfc":
        meta = {}
        name = ""
        from_date = ""
        to_date = ""

        for top, rw in sorted(rows.items()):
            rw = sorted(rw, key=lambda w: w["x0"])
            texts = [w["text"] for w in rw]
            x0s = [w["x0"] for w in rw]

            if top < 100 and x0s[0] < 70:
                line = " ".join(texts)
                if any(p in line for p in ("MR.", "MRS.", "MS.")):
                    for p in ("MR.", "MRS.", "MS."):
                        line = line.replace(p, "")
                    name = line.strip()

            if "From" in texts and "To" in texts:
                dates = re.findall(r"\d{2}/\d{2}/\d{4}", " ".join(texts))
                if len(dates) >= 2:
                    from_date, to_date = dates[0], dates[1]

            right = [(t, x) for t, x in zip(texts, x0s) if x >= 340]
            if len(right) < 2:
                continue
            ci = next((i for i, (t, _) in enumerate(right) if t == ":"), None)
            if ci and ci > 0:
                key = " ".join(t for t, _ in right[:ci])
                val = " ".join(t for t, _ in right[ci + 1 :])
                if key and val:
                    meta[key] = val

        m = re.search(r"RTGS/NEFTIFSC:\s*(\w+)", raw_text)
        ifsc = m.group(1) if m else ""
        acc_no = re.sub(r"\s*OTHER\s*", "", meta.get("AccountNo", "")).strip()

        return {
            "account_holder": name,
            "bank_name": "HDFC",
            "account_number": acc_no,
            "masked_account_number": (
                "- • ••••" + acc_no[-4:] if len(acc_no) >= 4 else acc_no
            ),
            "account_type": meta.get("AccountStatus", ""),
            "ifsc_code": ifsc,
            "branch": meta.get("AccountBranch", ""),
            "email": meta.get("Email", ""),
            "phone": meta.get("Phoneno.", ""),
            "customer_id": meta.get("CustID", ""),
            "account_open_date": meta.get("A/COpenDate", ""),
            "od_limit": meta.get("ODLimit", ""),
            "currency": meta.get("Currency", "INR"),
            "from_date": from_date,
            "to_date": to_date,
            "period": _compute_period(from_date, to_date, "hdfc"),
        }

    elif bank == "axis":

        def _axis_field(label):
            m = re.search(label + r"\s*[:\t]+\s*(.+)", raw_text)
            return m.group(1).strip() if m else ""

        acc_m = re.search(r"Axis Account No\s*[:\s]+(\d+)", raw_text)
        acc_no = acc_m.group(1).strip() if acc_m else _axis_field("Account No")

        period_m = re.search(
            r"period\s*\(From\s*:\s*([\d\-]+)\s+To\s*:\s*([\d\-]+)\)",
            raw_text,
        )
        from_date = period_m.group(1) if period_m else ""
        to_date = period_m.group(2) if period_m else ""

        name = ""
        lines = [l.strip() for l in raw_text.splitlines() if l.strip()]
        for line in lines:
            if re.match(r"^[A-Z][A-Z\s]+$", line) and len(line) > 4:
                name = line.strip()
                break

        return {
            "account_holder": name,
            "bank_name": "Axis Bank",
            "account_number": acc_no,
            "masked_account_number": (
                "- • ••••" + acc_no[-4:] if len(acc_no) >= 4 else acc_no
            ),
            "account_type": _axis_field("Scheme"),
            "ifsc_code": "",
            "branch": "",
            "email": "",
            "phone": "",
            "customer_id": _axis_field("Customer No"),
            "account_open_date": "",
            "od_limit": "",
            "currency": _axis_field("Currency") or "INR",
            "from_date": from_date,
            "to_date": to_date,
            "period": _compute_period(from_date, to_date, "axis"),
        }

    elif bank == "pnb":
        acc_m = re.search(r"Account[:\s]+([\d]{10,20})", raw_text)
        acc_no = acc_m.group(1).strip() if acc_m else ""

        name_m = re.search(r"Customer Name[:\s]+(.+)", raw_text)
        name = name_m.group(1).strip() if name_m else ""

        branch_m = re.search(r"Branch Name[:\s]+(.+)", raw_text)
        branch = branch_m.group(1).strip() if branch_m else ""

        ifsc_m = re.search(r"IFSC Code[:\s]+([A-Z0-9]+)", raw_text)
        ifsc = ifsc_m.group(1).strip() if ifsc_m else ""

        period_m = re.search(
            r"Statement For[:\s]+(\d{4}/\d{2}/\d{2})\s+to\s+(\d{4}/\d{2}/\d{2})",
            raw_text,
        )
        from_date = period_m.group(1).replace("/", "-") if period_m else ""
        to_date = period_m.group(2).replace("/", "-") if period_m else ""

        return {
            "account_holder": name,
            "bank_name": "Punjab National Bank",
            "account_number": acc_no,
            "masked_account_number": (
                "- • ••••" + acc_no[-4:] if len(acc_no) >= 4 else acc_no
            ),
            "account_type": "",
            "ifsc_code": ifsc,
            "branch": branch,
            "email": "",
            "phone": "",
            "customer_id": "",
            "account_open_date": "",
            "od_limit": "",
            "currency": "INR",
            "from_date": from_date,
            "to_date": to_date,
            "period": _compute_period(from_date, to_date, "pnb"),
        }

    elif bank == "ubin":
        acc_m = re.search(r"Account No\s+(\d+)", raw_text)
        acc_no = acc_m.group(1).strip() if acc_m else ""

        name = ""
        lines = [l.strip() for l in raw_text.splitlines() if l.strip()]
        for line in lines:
            if re.match(r"^[A-Z][A-Z\s]+$", line) and len(line) > 3:
                name = line.strip()
                break

        branch_m = re.search(r"Branch\s+(.+)", raw_text)
        branch = branch_m.group(1).strip() if branch_m else ""

        ifsc_m = re.search(r"IFSC Code\s+([A-Z0-9]+)", raw_text)
        ifsc = ifsc_m.group(1).strip() if ifsc_m else ""

        atype_m = re.search(r"Account Type\s+(.+)", raw_text)
        atype = atype_m.group(1).strip() if atype_m else ""

        cust_m = re.search(r"Customer Id\s+(\d+)", raw_text)
        cust_id = cust_m.group(1).strip() if cust_m else ""

        period_m = re.search(
            r"Statement Period From\s*-?(\d{2}/\d{2}/\d{4})\s+To\s+(\d{2}/\d{2}/\d{4})",
            raw_text,
        )
        from_date = period_m.group(1) if period_m else ""
        to_date = period_m.group(2) if period_m else ""

        return {
            "account_holder": name,
            "bank_name": "Union Bank of India",
            "account_number": acc_no,
            "masked_account_number": (
                "- • ••••" + acc_no[-4:] if len(acc_no) >= 4 else acc_no
            ),
            "account_type": atype,
            "ifsc_code": ifsc,
            "branch": branch,
            "email": "",
            "phone": "",
            "customer_id": cust_id,
            "account_open_date": "",
            "od_limit": "",
            "currency": "INR",
            "from_date": from_date,
            "to_date": to_date,
            "period": _compute_period(from_date, to_date, "ubin"),
        }

    elif bank == "idbi":
        name_m = re.search(r"Primary Account Holder Name\s*[:\t]+\s*(.+)", raw_text)
        name = name_m.group(1).strip() if name_m else ""

        acc_m = re.search(r"Account No\s*[:\t]+\s*(\d+)", raw_text)
        acc_no = acc_m.group(1).strip() if acc_m else ""

        cust_m = re.search(r"Customer ID\s*[:\t]+\s*(\d+)", raw_text)
        cust_id = cust_m.group(1).strip() if cust_m else ""

        branch_m = re.search(r"Account Branch\s*[:\t]+\s*(.+)", raw_text)
        branch = branch_m.group(1).strip() if branch_m else ""

        period_m = re.search(
            r"Transaction Date From\s*[:\t]+\s*(\d{2}/\d{2}/\d{4})\s+to:\s*(\d{2}/\d{2}/\d{4})",
            raw_text,
        )
        from_date = period_m.group(1) if period_m else ""
        to_date = period_m.group(2) if period_m else ""

        return {
            "account_holder": name,
            "bank_name": "IDBI Bank",
            "account_number": acc_no,
            "masked_account_number": (
                "- • ••••" + acc_no[-4:] if len(acc_no) >= 4 else acc_no
            ),
            "account_type": "",
            "ifsc_code": "",
            "branch": branch,
            "email": "",
            "phone": "",
            "customer_id": cust_id,
            "account_open_date": "",
            "od_limit": "",
            "currency": "INR",
            "from_date": from_date,
            "to_date": to_date,
            "period": _compute_period(from_date, to_date, "idbi"),
        }

    # ── Bank of Baroda ───────────────────────────────────────────────────────
    elif bank == "bob":
        # Use pdfplumber word coordinates to cleanly separate name from branch
        with pdfplumber.open(io.BytesIO(contents), password=password) as pdf:
            pg_words = pdf.pages[0].extract_words()

        word_rows = {}
        for w in pg_words:
            word_rows.setdefault(round(w["top"], 1), []).append(w)

        name   = ""
        branch = ""
        atype  = ""

        for top, rw in sorted(word_rows.items()):
            rw_s = sorted(rw, key=lambda w: w["x0"])
            texts = [w["text"] for w in rw_s]
            x0s   = [w["x0"]   for w in rw_s]

            # Name row: y ≈ 226 — customer name on left, branch on right
            if 220 < top < 235:
                name   = " ".join(t for t, x in zip(texts, x0s) if x < 490)
                branch = " ".join(t for t, x in zip(texts, x0s) if x >= 490)

            # Account type row: y ≈ 296 — single token on the left
            if 290 < top < 302 and x0s[0] < 100:
                atype = texts[0]

        acc_m = re.search(r"\b(\d{14,17})\b", raw_text)
        acc_no = acc_m.group(1) if acc_m else ""

        ifsc_m = re.search(r"BARB\w+", raw_text)
        ifsc   = ifsc_m.group(0) if ifsc_m else ""

        period_m = re.search(
            r"Account Statement from\s+(\d{2}-\d{2}-\d{4})\s+to\s+(\d{2}-\d{2}-\d{4})",
            raw_text,
        )
        from_date = period_m.group(1) if period_m else ""
        to_date   = period_m.group(2) if period_m else ""

        return {
            "account_holder":        name,
            "bank_name":             "Bank of Baroda",
            "account_number":        acc_no,
            "masked_account_number": ("- • ••••" + acc_no[-4:] if len(acc_no) >= 4 else acc_no),
            "account_type":          atype,
            "ifsc_code":             ifsc,
            "branch":                branch,
            "email":                 "",
            "phone":                 "",
            "customer_id":           "",
            "account_open_date":     "",
            "od_limit":              "",
            "currency":              "INR",
            "from_date":             from_date,
            "to_date":               to_date,
            "period":                _compute_period(from_date, to_date, "bob"),
        }

    else:

        def field(label):
            m = re.search(label + r"\s*[:	]+\s*(.+)", raw_text)
            return re.sub(r"\(cid:\d+\)", "", m.group(1)).strip() if m else ""

        name = field("Account Name")
        for p in ("Mr.", "Mrs.", "Ms.", "MR.", "MRS.", "MS."):
            name = name.replace(p, "")
        name = re.sub(r"\(cid:\d+\)", "", name).strip()

        acc_no = re.sub(r"\(cid:\d+\)", "", field("Account Number")).strip()
        branch = re.sub(r"\(cid:\d+\)", "", field("Branch")).strip()

        period_m = re.search(
            r"Account Statement from\s+(\d+ \w+ \d+)\s+to\s+(\d+ \w+ \d+)",
            raw_text,
        )
        from_date = period_m.group(1) if period_m else ""
        to_date = period_m.group(2) if period_m else ""

        return {
            "account_holder": name,
            "bank_name": "SBI",
            "account_number": acc_no,
            "masked_account_number": (
                "- • ••••" + acc_no[-4:] if len(acc_no) >= 4 else acc_no
            ),
            "account_type": re.sub(
                r"\(cid:\d+\)", "", field("Account Description")
            ).strip(),
            "ifsc_code": "",
            "branch": branch,
            "email": "",
            "phone": "",
            "customer_id": "",
            "account_open_date": "",
            "od_limit": re.sub(r"\(cid:\d+\)", "", field("Drawing Power")).strip(),
            "currency": "INR",
            "from_date": from_date,
            "to_date": to_date,
            "period": _compute_period(from_date, to_date, "sbi"),
        }


def _compute_period(from_date: str, to_date: str, fmt: str) -> str:
    try:
        if fmt in ("hdfc", "idbi"):
            d1 = datetime.strptime(from_date, "%d/%m/%Y")
            d2 = datetime.strptime(to_date, "%d/%m/%Y")
        elif fmt == "axis":
            d1 = datetime.strptime(from_date, "%d-%m-%Y")
            d2 = datetime.strptime(to_date, "%d-%m-%Y")
        elif fmt == "pnb":
            d1 = datetime.strptime(from_date, "%Y-%m-%d")
            d2 = datetime.strptime(to_date, "%Y-%m-%d")
        elif fmt == "ubin":
            d1 = datetime.strptime(from_date, "%d/%m/%Y")
            d2 = datetime.strptime(to_date, "%d/%m/%Y")
        elif fmt == "bob":                          # ← NEW
            d1 = datetime.strptime(from_date, "%d-%m-%Y")
            d2 = datetime.strptime(to_date,   "%d-%m-%Y")
        else:
            d1 = datetime.strptime(from_date, "%d %b %Y")
            d2 = datetime.strptime(to_date, "%d %b %Y")
        diff = (d2 - d1).days
        months = diff // 30
        days = diff % 30
        return f"{months}mo ({days}d)" if months else f"{diff}d"
    except Exception:
        return ""


# -------------------------------
# 🧠 ACCOUNT TYPE CLASSIFIER
# -------------------------------

_SALARY_KW = [
    r"SAL",
    r"SALARY",
    r"NETSAL",
    r"PAYROLL",
    r"PAYSAL",
    r"EMPLOYEE",
    r"EMPLYR",
    r"SAL[/_-]\w+",
    r"SALARY.*CREDIT",
    r"CREDIT.*SALARY",
]

_BUSINESS_KW = [
    r"GST",
    r"TDS",
    r"INVOICE",
    r"VENDOR",
    r"MERCHANT",
    r"TRADE",
    r"COMMISSION",
    r"CONSULTANCY",
    r"BUSINESS",
    r"FIRM",
    r"CASH DEPOSIT",
    r"CASH DEP",
]


def classify_account_type(df: pd.DataFrame) -> dict:
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    narrations = df["narration"].str.upper().fillna("")
    credits = df[df["credit"] > 0].copy()

    total_txns = len(df)
    total_months = df["date"].dt.to_period("M").nunique() or 1
    avg_txns_pm = total_txns / total_months

    credit_narr = credits["narration"].str.upper().fillna("")
    credits["month"] = credits["date"].dt.to_period("M")
    credits["day"] = credits["date"].dt.day

    monthly_cr_count = credits.groupby("month").size()
    monthly_cr_max = credits.groupby("month")["credit"].max()
    avg_credits_pm = monthly_cr_count.mean() if len(monthly_cr_count) else 0
    total_cr_txns = len(credits)

    scores = {"Salaried": 0, "Business": 0, "Non-Salaried": 0, "Regular": 0}
    signals = []

    sal_hits = sum(1 for n in credit_narr for p in _SALARY_KW if re.search(p, n))
    if sal_hits:
        scores["Salaried"] += min(sal_hits * 5, 20)
        signals.append(f"Salary keyword detected in {sal_hits} transaction(s)")

    if len(monthly_cr_max) >= 2:
        mean = monthly_cr_max.mean()
        cv = monthly_cr_max.std() / mean if mean else 1.0
        if cv < 0.10:
            scores["Salaried"] += 20
            signals.append(
                f"Very consistent monthly credits (variation {cv:.1%}) — salary-like"
            )
        elif cv < 0.20:
            scores["Salaried"] += 12
            signals.append(f"Fairly consistent monthly credits (variation {cv:.1%})")
        elif cv < 0.40:
            scores["Non-Salaried"] += 6
        else:
            scores["Business"] += 4
            scores["Non-Salaried"] += 2

    in_window = credits[credits["day"].apply(lambda d: d >= 25 or d <= 5)]
    ratio = len(in_window) / len(credits) if len(credits) else 0
    if ratio >= 0.6 and len(credits) >= 2:
        scores["Salaried"] += 10
        signals.append(
            f"{ratio:.0%} of credits fall in salary date window (25th–5th of month)"
        )

    if avg_credits_pm <= 2:
        scores["Salaried"] += 10
        signals.append(
            f"Low credits/month ({avg_credits_pm:.1f}) — typical salaried pattern"
        )
    elif avg_credits_pm <= 5:
        scores["Non-Salaried"] += 8
        signals.append(
            f"Medium credits/month ({avg_credits_pm:.1f}) — freelance/non-salaried pattern"
        )
    else:
        scores["Business"] += 10
        signals.append(
            f"High credits/month ({avg_credits_pm:.1f}) — business collection pattern"
        )

    emi_count = narrations.str.contains(r"EMI|SIP|LOAN|ACH", regex=True).sum()
    if emi_count >= 3:
        scores["Salaried"] += 8
        signals.append(
            f"{emi_count} EMI/SIP/Loan deductions — consistent liabilities (salaried)"
        )
    elif emi_count >= 1:
        scores["Salaried"] += 3
        scores["Non-Salaried"] += 2

    if avg_txns_pm >= 20:
        scores["Business"] += 12
        signals.append(
            f"High transaction volume ({avg_txns_pm:.0f}/month) — business pattern"
        )
    elif avg_txns_pm >= 10:
        scores["Business"] += 5
        scores["Non-Salaried"] += 3
    else:
        scores["Salaried"] += 3
        scores["Regular"] += 3

    cr_per_day = credits.groupby(credits["date"].dt.date).size()
    days_multi_cr = (cr_per_day > 2).sum()
    if days_multi_cr >= 3:
        scores["Business"] += 10
        signals.append(
            f"{days_multi_cr} days with 3+ credits — business collections/round-tripping"
        )
    elif days_multi_cr >= 1:
        scores["Business"] += 4

    biz_hits = sum(1 for n in narrations for p in _BUSINESS_KW if re.search(p, n))
    if biz_hits:
        scores["Business"] += min(biz_hits * 4, 16)
        signals.append(
            f"Business keyword detected in {biz_hits} transaction(s) (GST/TDS/Vendor etc.)"
        )

    unique_cr_narr = credits["narration"].nunique()
    if 3 <= unique_cr_narr <= 10 and avg_credits_pm <= 5:
        scores["Non-Salaried"] += 6
        signals.append(
            f"{unique_cr_narr} different credit sources at low volume — project/freelance income"
        )

    if total_cr_txns <= 3 and avg_txns_pm < 5:
        scores["Regular"] += 12
        signals.append(
            f"Very low activity ({total_cr_txns} total credits) — regular/savings account"
        )

    winner = max(scores, key=scores.get)
    if scores[winner] < 5:
        winner = "Regular"
        signals.append("No strong income pattern detected — classified as Regular")

    return {"category": winner, "scores": scores, "signals": signals}


# -------------------------------
# 📊 SUMMARY FUNCTIONS
# -------------------------------


def generate_summary(df):
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    total_income = df["credit"].sum()
    total_expense = df["debit"].sum()
    net_flow = total_income - total_expense

    daily = df.sort_values("date").groupby(df["date"].dt.date)["balance"].last()
    avg_balance = daily.mean()

    credit_df = df[df["credit"] > 0]
    debit_df = df[df["debit"] > 0]

    max_balance = df["balance"].max()
    min_balance = df["balance"].min()
    max_credit = credit_df["credit"].max() if not credit_df.empty else 0
    min_credit = credit_df["credit"].min() if not credit_df.empty else 0
    max_debit = debit_df["debit"].max() if not debit_df.empty else 0
    min_debit = debit_df["debit"].min() if not debit_df.empty else 0

    return {
        "total_income": round(total_income, 2),
        "total_expense": round(total_expense, 2),
        "net_flow": round(net_flow, 2),
        "avg_balance": round(avg_balance, 2),
        "max_credit": round(max_credit, 2),
        "min_credit": round(min_credit, 2),
        "max_debit": round(max_debit, 2),
        "min_debit": round(min_debit, 2),
        "max_balance": round(max_balance, 2),
        "min_balance": round(min_balance, 2),
        "largest_transaction": round(max(max_credit, max_debit), 2),
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

    if surplus > avg_income * 0.25:
        surplus_score = 40
    elif surplus > 0:
        surplus_score = 25
    else:
        surplus_score = 5

    variation = monthly_summary["credit"].std() / avg_income if avg_income else 1
    if variation < 0.25:
        stability_score = 30
    elif variation < 0.5:
        stability_score = 15
    else:
        stability_score = 5

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
# 🏦 LOAN OFFER MATCHER
# -------------------------------

LENDER_CATALOG = [
    {
        "lender": "Bajaj Finserv",
        "type": "Personal Loan",
        "min_score": 40,
        "min_surplus": 8000,
        "max_loan_multiplier": 24,
        "interest_rate": "13% p.a.",
        "tag": "Instant approval",
        "affiliate_url": "https://www.bajajfinserv.in/personal-loan?ref=BANKIE001",
    },
    {
        "lender": "MoneyView",
        "type": "Personal Loan",
        "min_score": 35,
        "min_surplus": 6000,
        "max_loan_multiplier": 20,
        "interest_rate": "14% p.a.",
        "tag": "No branch visit",
        "affiliate_url": "https://moneyview.in/personal-loan?ref=BANKIE001",
    },
    {
        "lender": "KreditBee",
        "type": "Personal Loan",
        "min_score": 30,
        "min_surplus": 5000,
        "max_loan_multiplier": 18,
        "interest_rate": "15.5% p.a.",
        "tag": "Approval in 10 mins",
        "affiliate_url": "https://kreditbee.in?ref=BANKIE001",
    },
    {
        "lender": "HDFC Bank",
        "type": "Home Loan",
        "min_score": 70,
        "min_surplus": 20000,
        "max_loan_multiplier": 60,
        "interest_rate": "8.75% p.a.",
        "tag": "Lowest rate",
        "affiliate_url": "https://www.hdfcbank.com/home-loan?ref=BANKIE001",
    },
    {
        "lender": "Tata Capital",
        "type": "Business Loan",
        "min_score": 50,
        "min_surplus": 15000,
        "max_loan_multiplier": 36,
        "interest_rate": "12% p.a.",
        "tag": "For business owners",
        "affiliate_url": "https://www.tatacapital.com/business-loan?ref=BANKIE001",
    },
    {
        "lender": "IndiaLends",
        "type": "Personal Loan",
        "min_score": 25,
        "min_surplus": 4000,
        "max_loan_multiplier": 15,
        "interest_rate": "16% p.a.",
        "tag": "Low score accepted",
        "affiliate_url": "https://indialends.com?ref=BANKIE001",
    },
]


def generate_loan_offers(loan_metrics: dict, account_category: str) -> dict:
    score = loan_metrics.get("loan_score", 0)
    surplus = loan_metrics.get("monthly_surplus", 0)

    emi_capacity = surplus * 0.45
    max_personal_loan = int(emi_capacity * 36)
    max_home_loan = int(emi_capacity * 180)

    eligible = []
    for lender in LENDER_CATALOG:
        if score < lender["min_score"]:
            continue
        if surplus < lender["min_surplus"]:
            continue
        if (
            lender["type"] == "Home Loan"
            and account_category not in ("Salaried",)
            and score < 75
        ):
            continue
        if lender["type"] == "Business Loan" and account_category == "Salaried":
            continue

        max_loan = int(surplus * lender["max_loan_multiplier"])
        eligible.append(
            {
                "lender": lender["lender"],
                "type": lender["type"],
                "max_loan": max_loan,
                "interest_rate": lender["interest_rate"],
                "tag": lender["tag"],
                "affiliate_url": lender["affiliate_url"],
            }
        )

    eligible.sort(key=lambda x: float(x["interest_rate"].replace("% p.a.", "")))
    eligible = eligible[:3]

    avg_income = loan_metrics.get("avg_monthly_income", 0)
    avg_expense = loan_metrics.get("avg_monthly_expense", 0)
    expense_ratio = (avg_expense / avg_income * 100) if avg_income else 0

    if surplus <= 0:
        tip = (
            f"Your expenses exceed your income. Reducing monthly spending by "
            f"₹{abs(int(surplus)) + 5000:,} would make you eligible for loans up to "
            f"₹{int(5000 * 36):,}."
        )
    elif expense_ratio > 75:
        reduction = int(avg_expense * 0.10)
        new_surplus = surplus + reduction
        new_max = int(new_surplus * 0.45 * 36)
        tip = (
            f"Your expenses are {expense_ratio:.0f}% of income. "
            f"Reducing monthly expenses by ₹{reduction:,} would raise your "
            f"eligible loan amount to ₹{new_max:,}."
        )
    elif score < 60:
        tip = (
            "Your income is inconsistent month-to-month. "
            "3 months of stable income credits will push your score above 70 "
            "and unlock lower interest rate offers."
        )
    else:
        tip = (
            f"Your profile looks healthy. Maintaining this surplus of "
            f"₹{int(surplus):,}/month for 3 more months could qualify you "
            f"for home loan offers above ₹{max_home_loan:,}."
        )

    return {
        "max_personal_loan": max_personal_loan,
        "max_home_loan": max_home_loan,
        "eligible_offers": eligible,
        "improvement_tip": tip,
        "offers_count": len(eligible),
    }


# -------------------------------
# 🏆 FINANCIAL HEALTH SCORE
# -------------------------------


def generate_financial_health_score(
    summary: dict,
    monthly_summary,
    loan_metrics: dict,
    emi_analysis: dict,
    account_category: str,
) -> dict:
    scores = {}

    avg_income = loan_metrics.get("avg_monthly_income", 0) or 0
    avg_expense = loan_metrics.get("avg_monthly_expense", 0) or 0
    surplus = loan_metrics.get("monthly_surplus", 0) or 0
    closing_bal = summary.get("closing_balance", 0) or 0
    emi_summary = emi_analysis.get("emi_summary", {})

    # 1. Savings rate (25 pts)
    savings_rate = (surplus / avg_income) if avg_income > 0 else 0
    if savings_rate >= 0.30:
        s1, s1_label = 25, f"Excellent — saving {savings_rate:.0%} of income"
    elif savings_rate >= 0.20:
        s1, s1_label = 20, f"Good — saving {savings_rate:.0%} of income"
    elif savings_rate >= 0.10:
        s1, s1_label = 12, f"Average — saving {savings_rate:.0%} of income"
    elif savings_rate > 0:
        s1, s1_label = 6, f"Low — saving only {savings_rate:.0%} of income"
    else:
        s1, s1_label = 0, "Spending exceeds income"
    scores["savings_rate"] = {"score": s1, "max": 25, "label": s1_label}

    # 2. Expense control (20 pts)
    if len(monthly_summary) >= 2:
        exp_cv = float(monthly_summary["debit"].std()) / (
            float(monthly_summary["debit"].mean()) or 1
        )
        if exp_cv < 0.15:
            s2, s2_label = 20, "Very stable monthly spending"
        elif exp_cv < 0.30:
            s2, s2_label = 14, "Moderate spending variation"
        elif exp_cv < 0.50:
            s2, s2_label = 8, "High spending variation"
        else:
            s2, s2_label = 3, "Very erratic spending pattern"
    else:
        s2, s2_label = 10, "Not enough months to evaluate"
    scores["expense_control"] = {"score": s2, "max": 20, "label": s2_label}

    # 3. Income stability (20 pts)
    if len(monthly_summary) >= 2:
        inc_cv = float(monthly_summary["credit"].std()) / (
            float(monthly_summary["credit"].mean()) or 1
        )
        if inc_cv < 0.10:
            s3, s3_label = 20, "Very consistent income — salary-like"
        elif inc_cv < 0.25:
            s3, s3_label = 15, "Fairly consistent income"
        elif inc_cv < 0.50:
            s3, s3_label = 8, "Variable income — freelance/business pattern"
        else:
            s3, s3_label = 3, "Highly irregular income"
    else:
        s3, s3_label = 10, "Not enough months to evaluate"
    scores["income_stability"] = {"score": s3, "max": 20, "label": s3_label}

    # 4. EMI health (20 pts)
    bounce_count = emi_summary.get("emi_bounce_count_last_6_months", 0) or 0
    emi_to_income = emi_summary.get("emi_to_income_ratio") or 0
    if bounce_count == 0 and emi_to_income < 0.30:
        s4, s4_label = 20, "No EMI bounces, healthy EMI ratio"
    elif bounce_count == 0 and emi_to_income < 0.50:
        s4, s4_label = (
            14,
            f"No bounces but EMI ratio is {emi_to_income:.0%} — a bit high",
        )
    elif bounce_count <= 1:
        s4, s4_label = 8, f"{bounce_count} EMI bounce in last 6 months"
    else:
        s4, s4_label = 2, f"{bounce_count} EMI bounces in last 6 months — risky"
    scores["emi_health"] = {"score": s4, "max": 20, "label": s4_label}

    # 5. Balance cushion (15 pts)
    months_cushion = (closing_bal / avg_expense) if avg_expense > 0 else 0
    if months_cushion >= 3:
        s5, s5_label = 15, f"Strong — {months_cushion:.1f}x monthly expense in balance"
    elif months_cushion >= 1.5:
        s5, s5_label = (
            10,
            f"Adequate — {months_cushion:.1f}x monthly expense in balance",
        )
    elif months_cushion >= 0.5:
        s5, s5_label = (
            5,
            f"Thin — only {months_cushion:.1f}x monthly expense in balance",
        )
    else:
        s5, s5_label = 0, "Very low balance cushion"
    scores["balance_cushion"] = {"score": s5, "max": 15, "label": s5_label}

    total = sum(v["score"] for v in scores.values())

    if total >= 85:
        grade, grade_desc, color = (
            "Excellent",
            "You are in the top financial health bracket",
            "green",
        )
    elif total >= 70:
        grade, grade_desc, color = (
            "Good",
            "Solid finances with minor areas to improve",
            "teal",
        )
    elif total >= 50:
        grade, grade_desc, color = (
            "Average",
            "Room to improve savings and EMI management",
            "amber",
        )
    elif total >= 30:
        grade, grade_desc, color = (
            "Needs work",
            "Focus on reducing expenses and EMI burden",
            "orange",
        )
    else:
        grade, grade_desc, color = (
            "At Risk",
            "Urgent: expenses exceed income or frequent bounces",
            "red",
        )

    weakest = min(scores, key=lambda k: scores[k]["score"] / scores[k]["max"])
    weak_tips = {
        "savings_rate": "Try to save at least 20% of your income each month. Start a recurring deposit or SIP.",
        "expense_control": "Your spending varies too much month-to-month. Set a monthly budget cap for discretionary spends.",
        "income_stability": "Irregular income lowers your score. Building 3 months of consistent credits will improve it significantly.",
        "emi_health": "Reduce EMI burden or clear one active loan to improve your score and loan eligibility.",
        "balance_cushion": "Maintain at least 1.5x your monthly expense as balance buffer for emergencies.",
    }

    share_text = (
        f"My Bankie financial health score is {total}/100 ({grade}). "
        f"Check yours free at bankie.xyz — no sign-up, no data stored."
    )

    return {
        "health_score": total,
        "grade": grade,
        "grade_desc": grade_desc,
        "color": color,
        "breakdown": scores,
        "top_tip": weak_tips[weakest],
        "weakest_area": weakest,
        "share_text": share_text,
        "savings_rate_pct": round(savings_rate * 100, 1),
        "emi_burden_pct": round(emi_to_income * 100, 1) if emi_to_income else 0,
    }


# -------------------------------
# 🏦 BANK AUTO-DETECTION
# -------------------------------


def detect_bank(contents: bytes, password: str | None = None) -> str:
    try:
        from app.parser import _pnb_open

        with _pnb_open(io.BytesIO(contents), password) as pdf:
            first_page_text = pdf.pages[0].extract_text() or ""
        if (
            "punjab national bank" in first_page_text.lower()
            or "punb" in first_page_text.lower()
        ):
            return "pnb"
    except Exception:
        pass

    try:
        with pdfplumber.open(io.BytesIO(contents), password=password) as pdf:
            first_page_text = pdf.pages[0].extract_text() or ""
    except Exception:
        raise HTTPException(status_code=401, detail={"error": "PASSWORD_REQUIRED"})

    text_lower = first_page_text.lower()

    # ── Bank of Baroda detection ─────────────────────────────────────────────
    if (
        "bank of baroda" in text_lower
        or "barb0" in text_lower
        or "bob world" in text_lower
        or re.search(r"\bbarb\w+", text_lower)
    ):
        return "bob"

    if "state bank of india" in text_lower or "sbchq" in text_lower:
        return "sbi"
    if (
        "axis bank" in text_lower
        or "axisbank" in text_lower
        or "axis account" in text_lower
    ):
        return "axis"
    if "union bank" in text_lower or re.search(r"\bubin\b", text_lower):
        return "ubin"
    if (
        "idbi bank" in text_lower
        or "idbi tower" in text_lower
        or "www.idbi.com" in text_lower
    ):
        return "idbi"

    return "hdfc"


# -------------------------------
# 📂 CORE ANALYSIS FUNCTION
# -------------------------------


def process_statement(contents: bytes, password: str | None = None):
    bank = detect_bank(contents, password)

    if bank == "sbi":
        df = parse_sbi_pdf(io.BytesIO(contents), password)
    elif bank == "axis":
        df = parse_axis_pdf(io.BytesIO(contents), password)
    elif bank == "pnb":
        df = parse_pnb_pdf(io.BytesIO(contents), password)
    elif bank == "ubin":
        df = parse_ubin_pdf(io.BytesIO(contents), password)
    elif bank == "idbi":
        df = parse_idbi_pdf(io.BytesIO(contents), password)
    elif bank == "bob":                                    # ← NEW
        df = parse_bob_pdf(io.BytesIO(contents), password)
    else:
        df = parse_hdfc_pdf(io.BytesIO(contents), password)

    if df.empty:
        raise HTTPException(400, "No transactions detected.")

    df = df.sort_values(["date"]).reset_index(drop=True)
    df = add_categories(df)

    first = df.iloc[0]
    last = df.iloc[-1]
    credit = float(first.get("credit", 0) or 0)
    debit = float(first.get("debit", 0) or 0)
    balance = float(first["balance"])

    opening_balance = balance - credit if credit > 0 else balance + debit
    closing_balance = float(last["balance"])

    account_info = extract_account_info(contents, bank, password)
    _classification = classify_account_type(df)
    account_info["account_category"] = _classification["category"]
    account_info["account_type_signals"] = _classification["signals"]
    account_info["account_type_scores"] = _classification["scores"]

    summary = generate_summary(df)
    monthly_summary = generate_monthly_summary(df)

    summary["opening_balance"] = round(opening_balance, 2)
    summary["closing_balance"] = round(closing_balance, 2)

    loan_metrics = generate_loan_readiness(summary, monthly_summary)
    emi_analysis = analyze_emi(df)

    loan_offers = generate_loan_offers(
        loan_metrics, account_info.get("account_category", "Regular")
    )
    health_score = generate_financial_health_score(
        summary,
        monthly_summary,
        loan_metrics,
        emi_analysis,
        account_info.get("account_category", "Regular"),
    )

    return (
        df,
        summary,
        monthly_summary,
        loan_metrics,
        account_info,
        emi_analysis,
        loan_offers,
        health_score,
    )


# -------------------------------
# 📊 ANALYZE API
# -------------------------------


@app.post("/analyze")
async def analyze(file: UploadFile = File(...), password: str = Form(None)):
    if file.content_type != "application/pdf":
        raise HTTPException(400, "Only PDF allowed.")

    contents = await file.read()
    if len(contents) > MAX_FILE_SIZE:
        raise HTTPException(400, "File too large.")

    (
        df,
        summary,
        monthly_summary,
        loan_metrics,
        account_info,
        emi_analysis,
        loan_offers,
        health_score,
    ) = process_statement(contents, password)

    if not monthly_summary.empty:
        summary["highest_income_month"] = monthly_summary.loc[
            monthly_summary["credit"].idxmax(), "month"
        ]
        summary["highest_expense_month"] = monthly_summary.loc[
            monthly_summary["debit"].idxmax(), "month"
        ]
    else:
        summary["highest_income_month"] = None
        summary["highest_expense_month"] = None

    summary["total_transactions"] = len(df)
    category_summary = generate_category_summary(df)

    return {
        "account_info": account_info,
        "summary": summary,
        "loan_analysis": loan_metrics,
        "monthly_summary": monthly_summary.to_dict(orient="records"),
        "emi_analysis": emi_analysis,
        "category_summary": category_summary,
        "total_transactions": len(df),
        "loan_offers": loan_offers,
        "health_score": health_score,
    }


# -------------------------------
# 📥 DOWNLOAD EXCEL API
# -------------------------------


@app.post("/download-excel")
async def download_excel(file: UploadFile = File(...), password: str = Form(None)):
    contents = await file.read()
    if len(contents) > MAX_FILE_SIZE:
        raise HTTPException(400, "File too large.")

    (
        df,
        summary,
        monthly_summary,
        loan_metrics,
        account_info,
        emi_analysis,
        loan_offers,
        health_score,
    ) = process_statement(contents, password)

    df_export = df.copy()
    df_export["date"] = df_export["date"].dt.strftime("%d-%m-%Y")

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df_export.to_excel(writer, index=False, sheet_name="Transactions")
        pd.DataFrame(account_info.items(), columns=["Field", "Value"]).to_excel(
            writer, index=False, sheet_name="Account Info"
        )
        pd.DataFrame(summary.items(), columns=["Metric", "Value"]).to_excel(
            writer, index=False, sheet_name="Summary"
        )
        monthly_summary.to_excel(writer, index=False, sheet_name="Monthly")
        pd.DataFrame(loan_metrics.items(), columns=["Metric", "Value"]).to_excel(
            writer, index=False, sheet_name="Loan Analysis"
        )
        pd.DataFrame(
            emi_analysis["emi_summary"].items(), columns=["Metric", "Value"]
        ).to_excel(writer, index=False, sheet_name="EMI Analysis")

        category_summary = generate_category_summary(df)
        pd.DataFrame(category_summary["expense_by_category"]).to_excel(
            writer, index=False, sheet_name="Expense by Category"
        )
        pd.DataFrame(category_summary["income_by_category"]).to_excel(
            writer, index=False, sheet_name="Income by Category"
        )

    output.seek(0)
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=analysis.xlsx"},
    )


# -------------------------------
# 🏦 TALLY XML DOWNLOAD API
# -------------------------------


def generate_tally_xml(df, account_info, summary) -> str:
    bank_ledger = account_info.get("bank_name", "Bank Account")
    account_holder = account_info.get("account_holder", "")
    vouchers_xml = ""

    for _, row in df.iterrows():
        try:
            tally_date = pd.to_datetime(row["date"]).strftime("%Y%m%d")
        except Exception:
            continue

        narration = (
            str(row.get("narration", ""))
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
        )
        credit = float(row.get("credit", 0) or 0)
        debit = float(row.get("debit", 0) or 0)

        if credit > 0:
            vouchers_xml += f"""
    <VOUCHER VCHTYPE="Receipt" ACTION="Create">
        <DATE>{tally_date}</DATE>
        <NARRATION>{narration}</NARRATION>
        <VOUCHERTYPENAME>Receipt</VOUCHERTYPENAME>
        <ALLLEDGERENTRIES.LIST>
            <LEDGERNAME>{bank_ledger}</LEDGERNAME>
            <ISDEEMEDPOSITIVE>Yes</ISDEEMEDPOSITIVE>
            <AMOUNT>-{round(credit, 2)}</AMOUNT>
        </ALLLEDGERENTRIES.LIST>
        <ALLLEDGERENTRIES.LIST>
            <LEDGERNAME>Cash-in-Hand</LEDGERNAME>
            <ISDEEMEDPOSITIVE>No</ISDEEMEDPOSITIVE>
            <AMOUNT>{round(credit, 2)}</AMOUNT>
        </ALLLEDGERENTRIES.LIST>
    </VOUCHER>"""
        elif debit > 0:
            vouchers_xml += f"""
    <VOUCHER VCHTYPE="Payment" ACTION="Create">
        <DATE>{tally_date}</DATE>
        <NARRATION>{narration}</NARRATION>
        <VOUCHERTYPENAME>Payment</VOUCHERTYPENAME>
        <ALLLEDGERENTRIES.LIST>
            <LEDGERNAME>Cash-in-Hand</LEDGERNAME>
            <ISDEEMEDPOSITIVE>Yes</ISDEEMEDPOSITIVE>
            <AMOUNT>-{round(debit, 2)}</AMOUNT>
        </ALLLEDGERENTRIES.LIST>
        <ALLLEDGERENTRIES.LIST>
            <LEDGERNAME>{bank_ledger}</LEDGERNAME>
            <ISDEEMEDPOSITIVE>No</ISDEEMEDPOSITIVE>
            <AMOUNT>{round(debit, 2)}</AMOUNT>
        </ALLLEDGERENTRIES.LIST>
    </VOUCHER>"""

    return f"""<?xml version="1.0" encoding="UTF-8"?>
<ENVELOPE>
    <HEADER>
        <TALLYREQUEST>Import Data</TALLYREQUEST>
    </HEADER>
    <BODY>
        <IMPORTDATA>
            <REQUESTDESC>
                <REPORTNAME>Vouchers</REPORTNAME>
                <STATICVARIABLES>
                    <SVCURRENTCOMPANY>{account_holder}</SVCURRENTCOMPANY>
                </STATICVARIABLES>
            </REQUESTDESC>
            <REQUESTDATA>
                <TALLYMESSAGE xmlns:UDF="TallyUDF">
                    {vouchers_xml}
                </TALLYMESSAGE>
            </REQUESTDATA>
        </IMPORTDATA>
    </BODY>
</ENVELOPE>"""


@app.post("/download-tally-xml")
async def download_tally_xml(file: UploadFile = File(...), password: str = Form(None)):
    contents = await file.read()
    if len(contents) > MAX_FILE_SIZE:
        raise HTTPException(400, "File too large.")

    (
        df,
        summary,
        monthly_summary,
        loan_metrics,
        account_info,
        emi_analysis,
        loan_offers,
        health_score,
    ) = process_statement(contents, password)

    xml_content = generate_tally_xml(df, account_info, summary)
    filename = (
        f"tally_{account_info.get('bank_name', 'bank').lower().replace(' ', '_')}.xml"
    )

    return StreamingResponse(
        io.BytesIO(xml_content.encode("utf-8")),
        media_type="application/xml",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


# -------------------------------
# 📥 DOWNLOAD TRANSACTIONS ONLY API
# -------------------------------


@app.post("/download-transactions")
async def download_transactions(
    file: UploadFile = File(...), password: str = Form(None)
):
    contents = await file.read()
    if len(contents) > MAX_FILE_SIZE:
        raise HTTPException(400, "File too large.")

    (
        df,
        summary,
        monthly_summary,
        loan_metrics,
        account_info,
        emi_analysis,
        loan_offers,
        health_score,
    ) = process_statement(contents, password)

    df_export = df.copy()
    df_export["date"] = df_export["date"].dt.strftime("%d-%m-%Y")

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df_export.to_excel(writer, index=False, sheet_name="Transactions")

    output.seek(0)
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=transactions.xlsx"},
    )