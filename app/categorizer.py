import re
import pandas as pd

# ---------------------------------------------------------------------------
# 🏷️ TRANSACTION CATEGORIZER
# ---------------------------------------------------------------------------

CATEGORIES = {
    "Salary": [
        r"\bSAL\b",
        r"\bSALARY\b",
        r"NETSAL",
        r"PAYROLL",
        r"PAYSAL",
        r"SAL[/_\-]",
        r"SALARY.*CREDIT",
        r"CREDIT.*SALARY",
        r"\bEMPLOYEE\b",
        r"EMPLYR",
        r"STIPEND",
        r"WAGES",
    ],
    "UPI": [
        r"\bUPI\b",
        r"UPI/",
        r"/UPI",
        r"UPI-",
        r"UPIREF",
        r"GPAY",
        r"PHONEPE",
        r"PAYTM",
        r"BHIM",
        r"RAZORPAY",
        r"AMAZONPAY",
        r"MOBIKWIK",
        r"FREECHARGE",
    ],
    "NEFT / RTGS": [
        r"\bNEFT\b",
        r"\bRTGS\b",
        r"NEFT/",
        r"RTGS/",
        r"NEFT-",
        r"RTGS-",
        r"INF/NEFT",
        r"NEFT INWARD",
        r"NEFT OUTWARD",
    ],
    "IMPS": [r"\bIMPS\b", r"IMPS/", r"IMPS-"],
    "ATM": [
        r"\bATM\b",
        r"ATM-",
        r"ATM/",
        r"CASH WITHDRAWAL",
        r"CASHW",
        r"ATM CASH",
        r"CDM",
        r"CASH DISPENSER",
    ],
    "Cash Deposit": [
        r"CASH DEPOSIT",
        r"CASH DEP",
        r"\bCASHDEP\b",
        r"CDM DEPOSIT",
        r"CASH CREDIT",
        r"BRANCH CASH",
    ],
    "EMI / Loan": [
        r"\bEMI\b",
        r"EMI/",
        r"EMI-",
        r"\bLOAN\b",
        r"LOAN REPAY",
        r"\bACH\b",
        r"ACH-",
        r"NACH",
        r"ECS",
        r"ECS/",
        r"LOAN.*DEBIT",
        r"HOME LOAN",
        r"CAR LOAN",
        r"PERSONAL LOAN",
        r"BAJAJ",
        r"HDFC LOAN",
        r"ICICI LOAN",
        r"\bEMI BOUNCE\b",
    ],
    "Investment": [
        r"\bSIP\b",
        r"SIP/",
        r"MUTUAL FUND",
        r"MF/",
        r"/MF",
        r"ZERODHA",
        r"GROWW",
        r"UPSTOX",
        r"ANGEL",
        r"KUVERA",
        r"NSE",
        r"BSE",
        r"DEMAT",
        r"EQUITY",
        r"STOCK",
        r"LIC",
        r"INSURANCE.*PREMIUM",
        r"PREMIUM.*LIC",
        r"PPF",
        r"NPS",
        r"FD BOOKING",
        r"RD BOOKING",
    ],
    "Utility Bills": [
        r"ELECTRICITY",
        r"ELECTRIC",
        r"\bBEST\b",
        r"\bMSEB\b",
        r"\bBESCOM\b",
        r"\bTNEB\b",
        r"\bUPPCL\b",
        r"POWER BILL",
        r"WATER BILL",
        r"\bGAS\b",
        r"MAHANAGAR GAS",
        r"IGL\b",
        r"MGL\b",
        r"BROADBAND",
        r"AIRTEL.*BILL",
        r"BSNL",
        r"JIO.*BILL",
        r"INTERNET BILL",
        r"TATA.*BILL",
        r"RELIANCE.*BILL",
    ],
    "Recharge": [
        r"RECHARGE",
        r"MOBILE RECHARGE",
        r"DTH",
        r"TATASKY",
        r"DISHTV",
        r"AIRTEL RECHARGE",
        r"JIO RECHARGE",
        r"VODAFONE",
        r"PREPAID",
    ],
    "Shopping": [
        r"AMAZON",
        r"FLIPKART",
        r"MYNTRA",
        r"SNAPDEAL",
        r"MEESHO",
        r"NYKAA",
        r"AJIO",
        r"BIGBASKET",
        r"GROFERS",
        r"BLINKIT",
        r"ZEPTO",
        r"DMART",
        r"RELIANCE RETAIL",
        r"BIG BAZAAR",
        r"SHOPIFY",
        r"PURCHASE",
        r"POS ",
        r"POS/",
    ],
    "Food & Dining": [
        r"SWIGGY",
        r"ZOMATO",
        r"RESTAURANT",
        r"CAFE",
        r"HOTEL.*FOOD",
        r"DOMINOS",
        r"MCDONALDS",
        r"KFC",
        r"SUBWAY",
        r"BURGER",
        r"PIZZA",
        r"DUNZO",
        r"DINING",
    ],
    "Travel": [
        r"IRCTC",
        r"INDIAN RAILWAY",
        r"TRAIN",
        r"FLIGHT",
        r"MAKEMYTRIP",
        r"GOIBIBO",
        r"OYO",
        r"CLEARTRIP",
        r"YATRA",
        r"AIRASIA",
        r"INDIGO",
        r"SPICEJET",
        r"VISTARA",
        r"AIRINDIA",
        r"\bOLA\b",
        r"\bUBER\b",
        r"RAPIDO",
        r"TAXI",
        r"CAB",
        r"FASTAG",
        r"NHAI",
        r"TOLL",
    ],
    "Health": [
        r"PHARMACY",
        r"MEDPLUS",
        r"APOLLO.*PHARMA",
        r"1MG",
        r"NETMEDS",
        r"HOSPITAL",
        r"CLINIC",
        r"DOCTOR",
        r"HEALTH.*INS",
        r"MEDICLAIM",
        r"STAR HEALTH",
        r"MAX BUPA",
        r"NIVA BUPA",
    ],
    "GST / Tax": [
        r"\bGST\b",
        r"GST/",
        r"GSTIN",
        r"\bTDS\b",
        r"TDS/",
        r"INCOME TAX",
        r"TAX PAYMENT",
        r"TAX/",
        r"NSDL",
        r"CHALLAN",
        r"ADVANCE TAX",
    ],
    "Bank Charges": [
        r"CHARGES",
        r"SERVICE CHARGE",
        r"ANNUAL FEE",
        r"PROCESSING FEE",
        r"BANK CHARGE",
        r"INT\.? CHARGED",
        r"INTEREST DEBITED",
        r"PENAL",
        r"PENALTY",
        r"SMS CHARGE",
        r"DEMAT.*CHARGE",
        r"LOCKER.*CHARGE",
        r"MIN BAL",
        r"NON-MAINTENANCE",
    ],
    "Refund": [
        r"REFUND",
        r"REVERSAL",
        r"CASHBACK",
        r"CASH BACK",
        r"RETURN",
        r"REF/",
        r"REBATE",
    ],
    "Cheque": [
        r"\bCHQ\b",
        r"\bCHEQUE\b",
        r"CHQ/",
        r"CHQ-",
        r"CHEQUE NO",
        r"\bCLG\b",
        r"CLEARING",
    ],
}


def categorize_transaction(narration: str) -> str:
    text = str(narration).upper().strip()
    for category, patterns in CATEGORIES.items():
        for pattern in patterns:
            if re.search(pattern, text):
                return category
    return "Others"


def add_categories(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["category"] = df["narration"].apply(categorize_transaction)
    return df


def generate_category_summary(df: pd.DataFrame) -> dict:
    df = df.copy()
    if "category" not in df.columns:
        df = add_categories(df)

    # ensure date is datetime
    df["date"] = pd.to_datetime(df["date"])

    # ── Expense breakdown ─────────────────────────────────────────────────
    expense_df = df[df["debit"] > 0].copy()
    total_expense = expense_df["debit"].sum() or 1

    expense_summary = (
        expense_df.groupby("category")
        .agg(amount=("debit", "sum"), count=("debit", "count"))
        .reset_index()
        .sort_values("amount", ascending=False)
    )
    expense_summary["percentage"] = (
        expense_summary["amount"] / total_expense * 100
    ).round(1)
    expense_list = expense_summary.to_dict(orient="records")

    # ── Income breakdown ──────────────────────────────────────────────────
    income_df = df[df["credit"] > 0].copy()
    total_income = income_df["credit"].sum() or 1

    income_summary = (
        income_df.groupby("category")
        .agg(amount=("credit", "sum"), count=("credit", "count"))
        .reset_index()
        .sort_values("amount", ascending=False)
    )
    income_summary["percentage"] = (
        income_summary["amount"] / total_income * 100
    ).round(1)
    income_list = income_summary.to_dict(orient="records")

    # ── Monthly expense trend per category ───────────────────────────────
    monthly_expense_trend = (
        df[df["debit"] > 0]
        .groupby([df["date"].dt.to_period("M").astype(str), "category"])
        .agg(amount=("debit", "sum"))
        .reset_index()
        .rename(columns={"date": "month"})
        .to_dict(orient="records")
    )

    # ── Transactions grouped by category ─────────────────────────────────
    txn_df = df[["date", "narration", "debit", "credit", "category"]].copy()
    txn_df["date"] = txn_df["date"].dt.strftime("%d-%m-%Y")
# ✅ NEW
    transactions_by_category = (
    txn_df.groupby("category", group_keys=False)
    .apply(lambda x: x.to_dict(orient="records"))
    .to_dict()
    )

    return {
        "expense_by_category": expense_list,
        "income_by_category": income_list,
        "top_expense_category": expense_list[0]["category"] if expense_list else None,
        "top_income_category": income_list[0]["category"] if income_list else None,
        "monthly_expense_trend": monthly_expense_trend,
        "transactions_by_category": transactions_by_category,
    }
