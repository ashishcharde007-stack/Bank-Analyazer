from fastapi import HTTPException
import pdfplumber
import pandas as pd
from datetime import datetime


# ---------------------------------------------------------------------------
# HDFC Bank Statement Parser
# ---------------------------------------------------------------------------

# HDFC PDF column x-boundaries (verified from real PDF word coordinates):
#
#   Date         : x0  33 –  65   format dd/mm/yy
#   Narration    : x0  66 – 284
#   Ref No.      : x0 285 – 359
#   Value Date   : x0 360 – 400
#   Withdrawal   : x0 401 – 489
#   Deposit      : x0 490 – 560
#   Balance      : x0 561+

_H_DATE_END = 66
_H_NARR_END = 285
_H_REF_END = 360
_H_VALDT_END = 401
_H_WITHDRAWAL_END = 490
_H_DEPOSIT_END = 561


def _hdfc_is_date(text):
    """True if text looks like an HDFC date: dd/mm/yy."""
    try:
        datetime.strptime(text, "%d/%m/%y")
        return True
    except ValueError:
        return False


def _hdfc_amt(text):
    """Parse amount string to float, return None if not an amount."""
    try:
        return float(text.replace(",", "").strip())
    except ValueError:
        return None


def _hdfc_is_header(texts):
    return texts[0] in ("Date", "Narration", "Statementof")


def _hdfc_is_footer(x0s):
    """Footer rows start at x < 30 (bank disclaimer text)."""
    return x0s[0] < 30


def parse_hdfc_pdf(file_stream, password=None):
    """
    Parse an HDFC bank statement PDF into a DataFrame with columns:
        date, narration, ref_no, value_date, debit, credit, balance

    HDFC transactions can span multiple PDF rows:
      Main row:   dd/mm/yy  narration-part-1  ref_no  value_dt  amount  balance
      Cont rows:  narration-part-2   (x=72, no date, no amounts)
                  narration-part-3   ...
    """
    transactions = []

    try:
        pdf = pdfplumber.open(file_stream, password=password)
    except Exception:
        raise HTTPException(status_code=401, detail={"error": "PASSWORD_REQUIRED"})

    with pdf:
        for page in pdf.pages:
            words = page.extract_words()

            # Group words by y-position into logical rows
            rows = {}
            for w in words:
                rows.setdefault(round(w["top"], 0), []).append(w)

            for _top, row_words in sorted(rows.items()):
                row_words = sorted(row_words, key=lambda w: w["x0"])
                texts = [w["text"] for w in row_words]
                x0s = [w["x0"] for w in row_words]

                # ── Skip headers and footers ────────────────────────────────
                if _hdfc_is_header(texts) or _hdfc_is_footer(x0s):
                    continue

                # ── New transaction: first word is a date ───────────────────
                if x0s[0] < _H_DATE_END and _hdfc_is_date(texts[0]):
                    narration = ""
                    ref_no = ""
                    value_date = ""
                    debit = 0.0
                    credit = 0.0
                    balance = 0.0

                    try:
                        txn_date = datetime.strptime(texts[0], "%d/%m/%y")
                    except ValueError:
                        continue

                    for w in row_words[1:]:  # skip the date token itself
                        x, t = w["x0"], w["text"]
                        if x < _H_NARR_END:
                            narration += t + " "
                        elif x < _H_REF_END:
                            ref_no += t + " "
                        elif x < _H_VALDT_END:
                            value_date += t + " "
                        elif x < _H_WITHDRAWAL_END:
                            v = _hdfc_amt(t)
                            if v is not None:
                                debit = v
                        elif x < _H_DEPOSIT_END:
                            v = _hdfc_amt(t)
                            if v is not None:
                                credit = v
                        else:
                            v = _hdfc_amt(t)
                            if v is not None:
                                balance = v

                    transactions.append(
                        {
                            "date": txn_date,
                            "narration": narration.strip(),
                            "ref_no": ref_no.strip(),
                            "value_date": value_date.strip(),
                            "debit": debit,
                            "credit": credit,
                            "balance": balance,
                        }
                    )
                    continue

                # ── Continuation row: no date, no amounts ───────────────────
                # All words must be in the narration zone (x < ref boundary)
                # and there must be no amount-zone words
                if not transactions:
                    continue

                has_amounts = any(x >= _H_REF_END for x in x0s)
                if has_amounts:
                    continue  # has ref/amount cols → not a narration continuation

                # Append all words to narration of last transaction
                for w in row_words:
                    x, t = w["x0"], w["text"]
                    if x < _H_NARR_END:
                        transactions[-1]["narration"] += " " + t

                transactions[-1]["narration"] = transactions[-1]["narration"].strip()

    df = pd.DataFrame(transactions)
    return df


# ---------------------------------------------------------------------------
# SBI Bank Statement Parser
# ---------------------------------------------------------------------------

_SBI_MONTHS = {
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec",
}

# SBI column x-boundaries (verified from real PDF word coordinates):
#   Txn/Value Date : x0   0 – 142
#   Description    : x0 143 – 274
#   Ref No.        : x0 275 – 354
#   Debit          : x0 355 – 424  (large lakh amounts shift left to ~361)
#   Credit         : x0 425 – 499  (large lakh amounts at ~425-430)
#   Balance        : x0 500+       (large lakh amounts at ~504)

_S_DESC_START = 143
_S_DESC_END = 275
_S_REF_END = 355
_S_DEBIT_END = 425
_S_CREDIT_END = 500


def _sbi_amt(text):
    try:
        return float(text.replace(",", "").strip())
    except ValueError:
        return None


def _sbi_is_txn_start(texts):
    return len(texts) >= 2 and texts[0].isdigit() and texts[1] in _SBI_MONTHS


def _sbi_is_header(texts):
    return "Txn" in texts or (texts[0] == "Date" and len(texts) <= 3)


def parse_sbi_pdf(file_stream, password=None):
    """
    Parse an SBI bank statement PDF into a DataFrame with columns:
        date, narration, ref_no, value_date, debit, credit, balance

    SBI transactions span multiple PDF rows:
      Main row:   [day mon [yr]] [day mon [yr]]  desc  ref  amount  balance
      Cont rows:  [yr yr]? desc_continued  ref_continued
                  desc_continued ...
    Year tokens (x < 143) in continuation rows are silently skipped.
    """
    transactions = []

    try:
        pdf = pdfplumber.open(file_stream, password=password)
    except Exception:
        raise HTTPException(status_code=401, detail={"error": "PASSWORD_REQUIRED"})

    last_year = None

    with pdf:
        for page in pdf.pages:
            words = page.extract_words()

            rows = {}
            for w in words:
                rows.setdefault(round(w["top"], 0), []).append(w)

            for _top, row_words in sorted(rows.items()):
                row_words = sorted(row_words, key=lambda w: w["x0"])
                texts = [w["text"] for w in row_words]
                x0s = [w["x0"] for w in row_words]

                # ── Skip headers ────────────────────────────────────────────
                if _sbi_is_header(texts):
                    continue

                # ── New transaction row ─────────────────────────────────────
                if _sbi_is_txn_start(texts):
                    try:
                        if len(texts) > 2 and len(texts[2]) == 4 and texts[2].isdigit():
                            year = int(texts[2])
                            last_year = year
                            desc_start = 6
                        else:
                            year = last_year if last_year else datetime.now().year
                            desc_start = 4

                        month = datetime.strptime(texts[1], "%b").month
                        txn_date = datetime(year, month, int(texts[0]))
                    except (ValueError, IndexError):
                        continue

                    desc = ref = ""
                    debit = credit = balance = 0.0

                    for i in range(desc_start, len(row_words)):
                        x, t = row_words[i]["x0"], row_words[i]["text"]
                        if x < _S_DESC_END:
                            desc += t + " "
                        elif x < _S_REF_END:
                            ref += t + " "
                        elif x < _S_DEBIT_END:
                            v = _sbi_amt(t)
                            if v is not None:
                                debit = v
                        elif x < _S_CREDIT_END:
                            v = _sbi_amt(t)
                            if v is not None:
                                credit = v
                        else:
                            v = _sbi_amt(t)
                            if v is not None:
                                balance = v

                    transactions.append(
                        {
                            "date": txn_date,
                            "narration": desc.strip(),
                            "ref_no": ref.strip(),
                            "value_date": "",
                            "debit": debit,
                            "credit": credit,
                            "balance": balance,
                        }
                    )
                    continue

                # ── Continuation row ────────────────────────────────────────
                if not transactions:
                    continue

                # If any word is in amount zone → not a continuation
                if any(x >= _S_REF_END for x in x0s):
                    continue

                for w in row_words:
                    x, t = w["x0"], w["text"]
                    if x < _S_DESC_START:
                        pass  # date zone year tokens → skip
                    elif x < _S_DESC_END:
                        transactions[-1]["narration"] += " " + t
                    elif x < _S_REF_END:
                        transactions[-1]["ref_no"] += " " + t

                transactions[-1]["narration"] = transactions[-1]["narration"].strip()
                transactions[-1]["ref_no"] = transactions[-1]["ref_no"].strip()

    df = pd.DataFrame(transactions)
    return df


# ---------------------------------------------------------------------------
# Axis Bank Statement Parser
# ---------------------------------------------------------------------------
#
# Axis Bank PDF column x-boundaries (verified from real PDF word coordinates):
#
#   Date         : x0   0 –  90   format dd-mm-yyyy
#   Chq No       : x0  91 – 131   (usually empty in salary accounts)
#   Particulars  : x0 132 – 320   narration zone
#   Debit        : x0 321 – 395
#   Credit       : x0 396 – 496
#   Balance      : x0 497 +
#   Init.Br      : x0 537 +       (branch code, ignored)
#
# KEY DIFFERENCE from HDFC/SBI:
#   Axis puts narration BEFORE the date row, then the date row contains
#   the tail end of the narration + amounts.
#
#   Pattern A (prefix + tail):
#     row y=N   : "ATM-CASH/NITIN S NERS"           <- narration prefix (no date)
#     row y=N+9 : "30-01-2020 | NH79/BHILWARA/...  200.00  180.00  241"
#
#   Pattern B (two prefix rows + tail):
#     row y=N   : "ATM-"
#     row y=N+9 : "CASH/JHALARAPATAN/..."
#     row y=N+18: "12-02-2020 | 0220  2500.00  4544.00  241"
#
#   Pattern C (narration fully on date row):
#     row y=N   : "07-02-2020 | BRN-SALARY PAYMENT-Salary  8514.00  8544.00  101"
#
# ---------------------------------------------------------------------------

_A_DATE_END = 90  # date token must start before this x
_A_NARR_START = 132  # narration zone starts here
_A_NARR_END = 321  # narration zone ends here (debit starts)
_A_DEBIT_END = 396  # debit zone ends here (credit starts)
_A_CREDIT_END = 497  # credit zone ends here (balance starts)
_A_INITBR_START = 535  # Init.Br column – ignored

# Rows containing only these texts are header/footer lines to skip
_A_SKIP_TEXTS = {
    "Tran",
    "Date",
    "Chq",
    "No",
    "Particulars",
    "Debit",
    "Credit",
    "Balance",
    "Init.",
    "Br",
    "OPENING",
    "BALANCE",
    "CLOSING",
    "TRANSACTION",
    "TOTAL",
    "Legends",
    ":",
}

_A_FOOTER_PREFIXES = (
    "Unless",
    "The",
    "We",
    "With",
    "REGISTERED",
    "know",
    "clarif",
    "he/she",
    "excludes",
    "from",
    "suspicious",
    "debit",
    "ICONN",
    "VMT",
    "AUTOSWEEP",
    "REV",
    "SWEEP",
    "CWDR",
    "PUR",
    "TIP",
    "RATE",
    "CLG",
    "EDC",
    "SETU",
    "Int.",
    "++++ End",
)


def _axis_is_date(text):
    """True if text looks like an Axis Bank date: dd-mm-yyyy."""
    try:
        from datetime import datetime

        datetime.strptime(text, "%d-%m-%Y")
        return True
    except ValueError:
        return False


def _axis_amt(text):
    """Parse amount string → float. Returns None if not a number."""
    try:
        return float(text.replace(",", "").strip())
    except ValueError:
        return None


def _axis_is_skip_row(texts, x0s):
    """True for header, footer or total rows that should be ignored."""
    if not texts:
        return True
    # All tokens are header keywords
    if all(t in _A_SKIP_TEXTS for t in texts):
        return True
    # Footer lines start far left (x0 < 40) with known prose words
    if x0s[0] < 40 and texts[0].startswith(_A_FOOTER_PREFIXES):
        return True
    # "++++ End of Statement ++++" type lines
    if texts[0].startswith("++++"):
        return True
    return False


def parse_axis_pdf(file_stream, password=None):
    """
    Parse an Axis Bank statement PDF into a DataFrame with columns:
        date, narration, debit, credit, balance

    Axis Bank narrations can span multiple rows PRECEDING the date row.
    We accumulate a `pending_narration` buffer that is flushed when a
    date row is encountered.
    """
    from datetime import datetime as _dt
    from fastapi import HTTPException

    transactions = []

    try:
        import pdfplumber

        pdf = pdfplumber.open(file_stream, password=password)
    except Exception:
        raise HTTPException(status_code=401, detail={"error": "PASSWORD_REQUIRED"})

    pending_narration = ""  # narration text collected BEFORE the date row

    with pdf:
        for page in pdf.pages:
            words = page.extract_words()

            # Group words into logical rows by y-position
            rows = {}
            for w in words:
                rows.setdefault(round(w["top"], 0), []).append(w)

            for _top, row_words in sorted(rows.items()):
                row_words = sorted(row_words, key=lambda w: w["x0"])
                texts = [w["text"] for w in row_words]
                x0s = [w["x0"] for w in row_words]

                # ── Skip header / footer / total rows ────────────────────
                if _axis_is_skip_row(texts, x0s):
                    pending_narration = ""  # reset on section breaks
                    continue

                # ── Detect date row ──────────────────────────────────────
                # A date row has a dd-mm-yyyy token starting before x=90
                has_date = x0s[0] < _A_DATE_END and _axis_is_date(texts[0])

                if has_date:
                    try:
                        txn_date = _dt.strptime(texts[0], "%d-%m-%Y")
                    except ValueError:
                        pending_narration = ""
                        continue

                    narration_tail = ""
                    debit = credit = balance = 0.0

                    for w in row_words[1:]:  # skip date token
                        x, t = w["x0"], w["text"]
                        if x >= _A_INITBR_START:
                            pass  # branch code – ignore
                        elif x >= _A_CREDIT_END:
                            v = _axis_amt(t)
                            if v is not None:
                                balance = v
                        elif x >= _A_DEBIT_END:
                            v = _axis_amt(t)
                            if v is not None:
                                credit = v
                        elif x >= _A_NARR_END:
                            v = _axis_amt(t)
                            if v is not None:
                                debit = v
                        elif x >= _A_NARR_START:
                            narration_tail += t + " "
                        # x < _A_NARR_START → Chq No zone (skip)

                    full_narration = (pending_narration + " " + narration_tail).strip()
                    full_narration = " ".join(
                        full_narration.split()
                    )  # normalise spaces

                    transactions.append(
                        {
                            "date": txn_date,
                            "narration": full_narration,
                            "debit": debit,
                            "credit": credit,
                            "balance": balance,
                        }
                    )
                    pending_narration = ""  # reset after consuming
                    continue

                # ── Narration-prefix row (no date, no amounts) ───────────
                # Accumulate text from the narration zone into the buffer
                for w in row_words:
                    x, t = w["x0"], w["text"]
                    if _A_NARR_START <= x < _A_NARR_END:
                        pending_narration += t + " "

    import pandas as pd

    df = pd.DataFrame(transactions)
    return df
