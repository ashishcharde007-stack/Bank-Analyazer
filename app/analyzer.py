import re
import pandas as pd
from datetime import timedelta

_EMI_KW_STRONG = [
    r"\bEMI",
    r"\bECS\b",
    r"\bNACH\b",
    r"\bLOAN\b",
]

_EMI_KW_WEAK = [
    r"\b(?:NACH|ACH)\s*DR",
    r"\bACHD-",
    r"\bFINANCE\b",
    r"\bBAJAJ\b",
    r"TATA\s*CAP",
    r"HDFC\s*LTD",
    r"HOME\s*LOAN",
    r"\bVEHICLE\s*LOAN",
    r"\bBNPL\b",
    r"\bSLICE\b",
    r"\bKREDITBEE\b",
    r"\bFINFLEX\b",
    r"\bMONEYTAP\b",
    r"CAPITAL\s*FIRST",
]

_BOUNCE_KW = [
    r"\bRETURN\b",
    r"\bBOUNCE\b",
    r"INSUFF",
    r"NACH\s*RTN",
    r"EMI\s*RTN",
    r"\bRTN\b",
    r"REVERSAL",
    r"DISHONOUR",
    r"NOT\s*PAID",
    r"\bUNPAID\b",
]

_EMI_EXCLUDE_KW = [
    r"BILLPAY",
    r"BILLD",
    r"BILLDESK",
    r"CREDITCARD",
    r"CREDIT\s*CARD",
    r"SBICARDS",
    r"KOTAKCARDS",
    r"CC\s*DUE",
]


def _match_any(text, patterns):
    t = text.upper()
    return any(re.search(p, t) for p in patterns)


def _amounts_match(a, b, pct=0.02):
    mx = max(a, b)
    return abs(a - b) / mx <= pct if mx > 0 else a == b


def _extract_loan_id(narr):
    n = narr.upper()
    m = re.search(r"\bEMI\s*(\d{5,12})", n)
    if m:
        return m.group(1)
    m = re.search(
        r"(?:NACH|ECS|ACH|LOAN|FINANCE|CAPITAL|LTD|HOME)[\s\-_]*(\d{6,12})", n
    )
    if m:
        return m.group(1)
    nums = re.findall(r"\b(\d{7,12})\b", n)
    if nums:
        return nums[0]
    return None


def _is_emi_strong(narr):
    return _match_any(narr, _EMI_KW_STRONG) and not _match_any(narr, _EMI_EXCLUDE_KW)


def _is_emi_weak(narr):
    return _match_any(narr, _EMI_KW_WEAK) and not _match_any(narr, _EMI_EXCLUDE_KW)


def _emi_cluster_key(row):
    loan_id = _extract_loan_id(row["narration"])
    if loan_id:
        return f"LOAN:{loan_id}"
    prefix = re.sub(r"\d", "", row["narration"].upper())[:20].strip()
    bucket = round(row["debit"] / 100) * 100
    return f"PREFIX:{prefix}:{bucket}"


def analyze_emi(df):
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    debits = df[df["debit"] > 0].copy().reset_index(drop=True)
    credits = df[df["credit"] > 0].copy().reset_index(drop=True)

    max_date = df["date"].max()
    cutoff_6m = max_date - pd.DateOffset(months=6)

    deb_6m = debits[debits["date"] >= cutoff_6m].copy().reset_index(drop=True)
    cr_6m = credits[credits["date"] >= cutoff_6m].copy().reset_index(drop=True)

    # Step 1: Tag strong/weak EMI on all debits
    for frame in (debits, deb_6m):
        frame["_is_strong"] = frame["narration"].apply(_is_emi_strong)
        frame["_is_weak"] = frame["narration"].apply(_is_emi_weak)
        frame["_loan_key"] = frame.apply(_emi_cluster_key, axis=1)
        frame["_month"] = frame["date"].dt.to_period("M")

    # Step 2: Promote weak keys that appear in 2+ distinct months
    weak_debits = debits[debits["_is_weak"] & ~debits["_is_strong"]]
    weak_recurring = set()
    for key, grp in weak_debits.groupby("_loan_key"):
        if grp["_month"].nunique() >= 2:
            weak_recurring.add(key)

    # Step 3: Final EMI flag
    for frame in (debits, deb_6m):
        frame["_is_emi"] = frame["_is_strong"] | (
            frame["_is_weak"] & frame["_loan_key"].isin(weak_recurring)
        )

    emi_all = debits[debits["_is_emi"]].copy()
    emi_6m = deb_6m[deb_6m["_is_emi"]].copy()

    # Step 4: Active EMIs — unique loan_keys, last seen ≤60 days ago
    if not emi_all.empty:
        emi_deduped = emi_all.drop_duplicates(
            subset=["_loan_key", "_month"], keep="first"
        )
        clusters = (
            emi_deduped.groupby("_loan_key")
            .agg(
                amount=("debit", "median"),
                count=("debit", "count"),
                last_date=("date", "max"),
            )
            .reset_index()
        )
        active = clusters[(max_date - clusters["last_date"]).dt.days <= 60]
        total_active_emis = int(len(active))
        est_monthly_emi = round(float(active["amount"].sum()), 2)
    else:
        total_active_emis = 0
        est_monthly_emi = 0.0

    # Step 5: Total attempts = unique (loan_key, month) pairs in last 6m
    total_emi_attempts = (
        int(len(emi_6m.drop_duplicates(subset=["_loan_key", "_month"])))
        if not emi_6m.empty
        else 0
    )

    # Step 6: Bounce detection — max 1 bounce per (loan_key, month)
    # Deduplicate emi_6m first: keep the row with the LONGEST narration per
    # (loan_key, month) so we always match against the fully-joined narration.
    # This prevents duplicate rows (from multi-line PDF parsing) causing bounce=2.
    if not emi_6m.empty:
        emi_6m = (
            emi_6m.assign(_narr_len=emi_6m["narration"].str.len())
            .sort_values("_narr_len", ascending=False)
            .drop_duplicates(subset=["_loan_key", "_month"], keep="first")
            .drop(columns=["_narr_len"])
            .reset_index(drop=True)
        )

    bounced_pairs = set()
    for _, row in emi_6m.iterrows():
        pair = (row["_loan_key"], str(row["_month"]))
        if pair in bounced_pairs:
            continue
        narr_up = row["narration"].upper()
        amt = row["debit"]
        date = row["date"]

        # Rule A: bounce keyword in debit narration
        if _match_any(narr_up, _BOUNCE_KW):
            bounced_pairs.add(pair)
            continue

        # Rule B-Prime: balance went negative after this EMI debit = definitive bounce
        # This is the most accurate signal — no false positives possible.
        if "balance" in row.index and pd.notna(row["balance"]) and row["balance"] < 0:
            bounced_pairs.add(pair)
            continue

        # Rule B: same-day + same-loan + same-amount credit that is ALSO an EMI narration
        # (HDFC-style bounce reversal). Requiring the credit to be an EMI narration
        # prevents unrelated UPI/NEFT credits from falsely triggering.
        deb_loan = _extract_loan_id(row["narration"])
        if deb_loan:
            same_day = cr_6m[
                (cr_6m["date"] == date)
                & (cr_6m["credit"].apply(lambda c: _amounts_match(amt, c, pct=0.02)))
                & (cr_6m["narration"].apply(lambda n: _extract_loan_id(n) == deb_loan))
                & (
                    cr_6m["narration"].apply(_is_emi_strong)
                )  # must also be EMI narration
            ]
        else:
            same_day = cr_6m[
                (cr_6m["date"] == date)
                & (cr_6m["narration"] == row["narration"])
                & (cr_6m["credit"].apply(lambda c: _amounts_match(amt, c, pct=0.02)))
            ]
        if not same_day.empty:
            bounced_pairs.add(pair)
            continue

        # Rule C: same-amount credit with bounce keyword within 1-3 days
        # STRICT: credit must ALSO be related to the EMI (same loan_id in narration)
        # to avoid false positives from unrelated NEFT RTN / REFUND credits.
        w_s = date - pd.Timedelta(days=1)
        w_e = date + pd.Timedelta(days=3)
        nearby = cr_6m[(cr_6m["date"] >= w_s) & (cr_6m["date"] <= w_e)]
        for _, cr in nearby.iterrows():
            if _amounts_match(amt, cr["credit"], pct=0.03):
                cn = cr["narration"].upper()
                has_bounce_kw = _match_any(cn, _BOUNCE_KW) or any(
                    k in cn for k in ["RETURN", "RTN", "REVERSAL", "REFUND"]
                )
                # Credit must share the same loan_id to confirm it's an EMI bounce
                cr_loan = _extract_loan_id(cr["narration"])
                deb_loan = _extract_loan_id(row["narration"])
                same_loan = cr_loan and deb_loan and cr_loan == deb_loan
                if has_bounce_kw and same_loan:
                    bounced_pairs.add(pair)
                    break
        if pair in bounced_pairs:
            continue

        # Rule D: bounce charge ₹200-₹600 within 3 days
        w_a = date + pd.Timedelta(days=3)
        chgs = deb_6m[
            (deb_6m["date"] > date)
            & (deb_6m["date"] <= w_a)
            & (deb_6m["debit"] >= 200)
            & (deb_6m["debit"] <= 600)
        ]
        for _, bc in chgs.iterrows():
            bn = bc["narration"].upper()
            if any(
                k in bn for k in ["BOUNCE", "CHARGE", "PENALTY", "DISHONOUR", "RTN"]
            ):
                bounced_pairs.add(pair)
                break

    bounce_count = len(bounced_pairs)
    bounce_ratio = (
        round(bounce_count / total_emi_attempts, 4) if total_emi_attempts > 0 else 0.0
    )

    # Step 7: Regularity score
    if total_emi_attempts == 0:
        regularity_score = 100.0
    else:
        score = 100.0
        score -= min(bounce_ratio * 60, 60)
        if not emi_6m.empty:
            dd = emi_6m.drop_duplicates(subset=["_loan_key", "_month"])["date"].dt.day
            day_std = float(dd.std())
            if not pd.isna(day_std):
                score -= min(day_std * 1.5, 30)
        regularity_score = round(max(0.0, score), 1)

    # Step 8: Monthly income
    cr6 = cr_6m.copy()
    cr6["_mo"] = cr6["date"].dt.to_period("M")
    mi = cr6.groupby("_mo")["credit"].sum()
    avg_monthly_income = round(float(mi.mean()), 2) if len(mi) else None

    # Step 9: EMI-to-income ratio
    emi_to_income = (
        round(est_monthly_emi / avg_monthly_income, 4)
        if avg_monthly_income and avg_monthly_income > 0 and est_monthly_emi > 0
        else None
    )

    # Step 10: Risk level
    b_risk = 0 if bounce_count == 0 else (1 if bounce_count <= 2 else 2)
    r_risk = (
        0
        if emi_to_income is None
        else (0 if emi_to_income < 0.30 else 1 if emi_to_income < 0.50 else 2)
    )
    risk_level = ["Low", "Moderate", "High"][max(b_risk, r_risk)]

    return {
        "emi_summary": {
            "total_active_emis": total_active_emis,
            "estimated_total_monthly_emi": est_monthly_emi,
            "emi_bounce_count_last_6_months": bounce_count,
            "total_emi_attempts_last_6_months": total_emi_attempts,
            "bounce_ratio": bounce_ratio,
            "emi_regularity_score": regularity_score,
            "average_monthly_income": avg_monthly_income,
            "emi_to_income_ratio": emi_to_income,
            "risk_level": risk_level,
        }
    }
