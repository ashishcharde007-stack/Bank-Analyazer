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


# ---------------------------------------------------------------------------
# PNB (Punjab National Bank) Statement Parser
# ---------------------------------------------------------------------------
#
# PNB PDF column x-boundaries (verified from real PDF word coordinates):
#
#   Transaction Date : x0   0 – 119   format dd/mm/yyyy
#   Cheque Number    : x0 120 – 174   (usually empty)
#   Withdrawal       : x0 175 – 262   debit amounts
#   Deposit          : x0 263 – 319   credit amounts
#   Balance          : x0 320 – 369
#   "Cr." marker     : x0 370 – 393   (always present, ignored)
#   Narration        : x0 394 +
#
# STRUCTURE:
#   Every transaction occupies exactly 2 PDF rows:
#     Main row : date | [withdrawal|deposit] | balance | Cr. | narration-part-1
#     Cont row : narration-part-2  (x=394, no date, no amounts)
#
#   Some narrations have 2 continuation rows (e.g. IMPS- / IN/... / U KUM).
#
# PASSWORD:
#   PNB PDFs are encrypted with the 16-digit account number as the password.
#   The account number is embedded in the PDF title line and is tried
#   automatically.  If that fails, the caller must supply the password.
# ---------------------------------------------------------------------------

_P_DATE_END = 120
_P_CHQ_END = 175
_P_WDRAW_END = 263
_P_DEP_END = 320
_P_BAL_END = 370
_P_CR_END = 394  # "Cr." marker zone – skip
_P_NARR_START = 394

_P_SKIP_TEXTS = {
    "Transaction",
    "Date",
    "Cheque",
    "Number",
    "Withdrawal",
    "Deposit",
    "Balance",
    "Narration",
    "Page",
    "of",
}

_P_FOOTER_STARTS = (
    "****",
    "*",
    "Unless",
    "Computer",
    "Please",
    "Customers",
    "Abbreviations",
)


def _pnb_is_date(text):
    """True if text looks like a PNB date: dd/mm/yyyy."""
    try:
        from datetime import datetime

        datetime.strptime(text, "%d/%m/%Y")
        return True
    except ValueError:
        return False


def _pnb_amt(text):
    """Parse amount string → float. Returns None if not a number."""
    try:
        return float(text.replace(",", "").strip())
    except ValueError:
        return None


def _pnb_is_skip_row(texts, x0s):
    """True for header, footer, page-number or total rows."""
    if not texts:
        return True
    if all(t in _P_SKIP_TEXTS for t in texts):
        return True
    if texts[0].startswith(_P_FOOTER_STARTS):
        return True
    # Page N of M
    if texts[0] == "Page" or (len(texts) <= 4 and texts[0].isdigit()):
        return True
    return False


def _pnb_open(file_stream, password=None):
    """
    Open PNB PDF.  PNB encrypts with the 16-digit account number.
    We extract it from the title line and try it automatically.
    If a password is explicitly supplied, that takes priority.
    """
    import pdfplumber, re, io
    from fastapi import HTTPException

    raw = file_stream.read()
    file_stream.seek(0)

    # If explicit password supplied, try it first
    if password:
        try:
            return pdfplumber.open(io.BytesIO(raw), password=password)
        except Exception:
            pass

    # Try no password (some PNB exports are unencrypted)
    try:
        pdf = pdfplumber.open(io.BytesIO(raw))
        pdf.pages[0].extract_text()  # force a read to check
        return pdf
    except Exception:
        pass

    # Try to extract account number from raw bytes and use as password
    m = re.search(rb"Account[:\s]+(\d{10,20})", raw)
    if m:
        acc_pwd = m.group(1).decode()
        try:
            return pdfplumber.open(io.BytesIO(raw), password=acc_pwd)
        except Exception:
            pass

    raise HTTPException(status_code=401, detail={"error": "PASSWORD_REQUIRED"})


def parse_pnb_pdf(file_stream, password=None):
    """
    Parse a PNB bank statement PDF into a DataFrame with columns:
        date, narration, debit, credit, balance

    PNB transactions always have:
      Main row  : date + amount(s) + balance + Cr. + narration-start
      Cont rows : narration-continuation at x >= 394 (1–2 extra rows typical)
    """
    from datetime import datetime as _dt
    import pandas as pd

    transactions = []
    pending_narration = ""  # narration buffer from PREVIOUS rows

    pdf = _pnb_open(file_stream, password)

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

                # ── Skip header / footer / page-number rows ──────────────
                if _pnb_is_skip_row(texts, x0s):
                    continue

                # ── Date row detection ───────────────────────────────────
                has_date = x0s[0] < _P_DATE_END and _pnb_is_date(texts[0])

                if has_date:
                    try:
                        txn_date = _dt.strptime(texts[0], "%d/%m/%Y")
                    except ValueError:
                        continue

                    debit = credit = balance = 0.0
                    narration = ""

                    for w in row_words[1:]:
                        x, t = w["x0"], w["text"]
                        if x >= _P_NARR_START:
                            narration += t + " "
                        elif x >= _P_CR_END:
                            pass  # "Cr." marker – skip
                        elif x >= _P_BAL_END:
                            pass  # "Cr." zone – skip
                        elif x >= _P_DEP_END:
                            pass  # should not appear
                        elif x >= _P_WDRAW_END:
                            # In balance zone (320–369)
                            v = _pnb_amt(t)
                            if v is not None:
                                balance = v
                        elif x >= _P_CHQ_END:
                            # Withdrawal (175–262) OR deposit (263–319)
                            v = _pnb_amt(t)
                            if v is not None:
                                if x < _P_WDRAW_END:
                                    debit = v
                                else:
                                    credit = v
                        # x < _P_CHQ_END → cheque number zone (skip)

                    # Re-parse more carefully with explicit zone checks
                    debit = credit = balance = 0.0
                    narration = ""
                    for w in row_words[1:]:
                        x, t = w["x0"], w["text"]
                        if x >= _P_NARR_START:
                            narration += t + " "
                        elif x >= _P_CR_END:
                            pass  # "Cr." – skip
                        elif x >= _P_DEP_END:  # 320–393: balance
                            v = _pnb_amt(t)
                            if v is not None:
                                balance = v
                        elif x >= _P_WDRAW_END:  # 263–319: deposit
                            v = _pnb_amt(t)
                            if v is not None:
                                credit = v
                        elif x >= _P_CHQ_END:  # 175–262: withdrawal
                            v = _pnb_amt(t)
                            if v is not None:
                                debit = v
                        # x < 175: date / cheque – skip

                    transactions.append(
                        {
                            "date": txn_date,
                            "narration": narration.strip(),
                            "debit": debit,
                            "credit": credit,
                            "balance": balance,
                            "_pending": True,  # may still get continuation rows
                        }
                    )
                    continue

                # ── Narration continuation row ───────────────────────────
                # Only text at x >= _P_NARR_START, no date, no amounts
                if not transactions:
                    continue

                is_narr_cont = all(x >= _P_NARR_START for x in x0s)
                if is_narr_cont:
                    extra = " ".join(texts)
                    transactions[-1]["narration"] = (
                        transactions[-1]["narration"] + " " + extra
                    ).strip()

    # Clean up helper flag and normalise spaces
    for t in transactions:
        t.pop("_pending", None)
        t["narration"] = " ".join(t["narration"].split())

    df = pd.DataFrame(transactions)
    return df


# ---------------------------------------------------------------------------
# Union Bank of India Statement Parser
# ---------------------------------------------------------------------------
#
# Union Bank PDF column x-boundaries (verified from real PDF word coordinates):
#
#   Date       : x0   0 –  100   format dd-mm-yyyy
#   Time       : x0   0 –  100   HH:MM:SS on the row immediately below date → SKIP
#   Remarks    : x0 102 –  213   narration (multi-line, 1-4 continuation rows)
#   Tran Id-1  : x0 214 –  279   S-prefixed transaction ID  → ignored
#   UTR Number : x0 280 –  384   UTR / Sender ref           → ignored
#   Instr. ID  : x0 385 –  459   usually "-"                → ignored
#   Withdrawals: x0 460 –  562   debit
#   Deposits   : x0 563 –  657   credit
#   Balance    : x0 658 +        running balance
#
# STRUCTURE:
#   Row 1 (main)  : date | remarks-start | tran-id | utr | instr | [withdrawal|deposit] | balance
#   Row 2         : time (HH:MM:SS at x≈39)  ← SKIP — not a new transaction
#   Row 3+        : remarks-continuation (x≈102, no amounts) ← append to narration
#
# ---------------------------------------------------------------------------

_U_DATE_END = 101  # date/time token starts before this x
_U_NARR_END = 214  # remarks zone  102 – 213
_U_AMT_START = 460  # first amount column starts here
_U_WDRAW_END = 563  # withdrawal ends here  (deposit starts)
_U_DEP_END = 658  # deposit ends here     (balance starts)

# Rows to skip: header, footer, page-number, "Records from..." line
_U_SKIP_TEXTS = {
    "Date",
    "Remarks",
    "Tran",
    "Id-1",
    "UTR",
    "Number",
    "Instr.",
    "ID",
    "Withdrawals",
    "Deposits",
    "Balance",
    "Page",
    "No1",
    "No2",
    "No3",
    "No4",
}
_U_FOOTER_STARTS = (
    "For",
    "This",
    "TO",
    "Records",
    "Statement",
    "City",
    "State",
    "Country",
    "Zip",
    "Mobile",
    "E-mail",
)

# Time pattern  HH:MM:SS  — these rows appear directly below the date row
import re as _re

_U_TIME_RE = _re.compile(r"^\d{2}:\d{2}:\d{2}$")


def _ubin_is_date(text):
    """True if text looks like a Union Bank date: dd-mm-yyyy."""
    try:
        from datetime import datetime

        datetime.strptime(text, "%d-%m-%Y")
        return True
    except ValueError:
        return False


def _ubin_amt(text):
    """Parse amount string (may have commas) → float. Returns None if not a number."""
    try:
        return float(text.replace(",", "").strip())
    except ValueError:
        return None


def _ubin_is_skip_row(texts, x0s):
    """True for header, footer, page-number or summary rows."""
    if not texts:
        return True
    # All tokens are known header labels
    if all(t in _U_SKIP_TEXTS for t in texts):
        return True
    # Footer lines
    if texts[0].startswith(_U_FOOTER_STARTS):
        return True
    # "Page No1" style
    if texts[0] == "Page":
        return True
    return False


def parse_ubin_pdf(file_stream, password=None):
    """
    Parse a Union Bank of India statement PDF into a DataFrame with columns:
        date, narration, debit, credit, balance

    Each transaction spans multiple PDF rows:
      Main row  : date + remarks-part-1 + amounts
      Time row  : HH:MM:SS at x<100    ← skipped (not a new transaction)
      Cont rows : remarks-continuation at x≈102 (no amounts)
    """
    from datetime import datetime as _dt
    import pandas as pd
    import pdfplumber
    from fastapi import HTTPException

    transactions = []

    try:
        pdf = pdfplumber.open(file_stream, password=password)
    except Exception:
        raise HTTPException(status_code=401, detail={"error": "PASSWORD_REQUIRED"})

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

                # ── Skip header / footer / page-number rows ──────────────
                if _ubin_is_skip_row(texts, x0s):
                    continue

                # ── Skip time rows (HH:MM:SS directly below the date) ────
                # These appear at x<100 and match HH:MM:SS pattern
                if (
                    len(texts) == 1
                    and x0s[0] < _U_DATE_END
                    and _U_TIME_RE.match(texts[0])
                ):
                    continue

                # ── Date row (new transaction) ───────────────────────────
                has_date = x0s[0] < _U_DATE_END and _ubin_is_date(texts[0])

                if has_date:
                    try:
                        txn_date = _dt.strptime(texts[0], "%d-%m-%Y")
                    except ValueError:
                        continue

                    narration = ""
                    debit = credit = balance = 0.0

                    for w in row_words[1:]:
                        x, t = w["x0"], w["text"]

                        if x >= _U_DEP_END:  # Balance column
                            v = _ubin_amt(t)
                            if v is not None:
                                balance = v

                        elif x >= _U_WDRAW_END:  # Deposit column
                            v = _ubin_amt(t)
                            if v is not None:
                                credit = v

                        elif x >= _U_AMT_START:  # Withdrawal column
                            v = _ubin_amt(t)
                            if v is not None:
                                debit = v

                        elif x >= _U_NARR_END:
                            pass  # Tran ID / UTR / "-" → skip

                        elif x >= 102:  # Remarks / narration zone
                            narration += t + " "

                        # x < 102: date zone — skip

                    transactions.append(
                        {
                            "date": txn_date,
                            "narration": narration.strip(),
                            "debit": debit,
                            "credit": credit,
                            "balance": balance,
                        }
                    )
                    continue

                # ── Narration continuation row ───────────────────────────
                # Only text in remarks zone (x≈102–213), no amounts
                if not transactions:
                    continue

                has_amounts = any(x >= _U_AMT_START for x in x0s)
                if has_amounts:
                    continue  # has amount tokens → not a continuation

                # Append only words in the remarks zone
                extra = " ".join(
                    t
                    for w in row_words
                    for t, x in [(w["text"], w["x0"])]
                    if 102 <= x < _U_NARR_END
                )
                if extra.strip():
                    transactions[-1]["narration"] = (
                        transactions[-1]["narration"] + " " + extra
                    ).strip()

    # Normalise spaces in all narrations
    for t in transactions:
        t["narration"] = " ".join(t["narration"].split())

    import pandas as pd

    df = pd.DataFrame(transactions)
    return df


# ---------------------------------------------------------------------------
# IDBI Bank Statement Parser
# ---------------------------------------------------------------------------
#
# IDBI Bank PDF column x-boundaries (verified from real PDF word coordinates):
#
#   Srl          : x0  51 –  67   serial number (integer)
#   Txn Date     : x0  68 – 104   format dd/mm/yyyy
#   Time         : x0 105 – 157   HH:MM:SS + AM/PM tokens  → ignored
#   Value Date   : x0 158 – 209   format dd/mm/yyyy
#   Description  : x0 210 – 453   narration (may span multiple space-separated tokens)
#   CR/DR        : x0 454 – 481   "Dr." or "Cr."
#   CCY          : x0 482 – 517   always "INR"              → ignored
#   Amount       : x0 518 – 587
#   Balance      : x0 588 +
#
# STRUCTURE:
#   Every transaction fits on exactly one PDF row.
#   Row starts with an integer serial number, followed by the transaction date.
#   The CR/DR column ("Dr." / "Cr.") determines debit vs credit direction.
#   No multi-row narration continuation — all description tokens sit in
#   x0 210–453 on the same row.
#
# ---------------------------------------------------------------------------

_I_SRL_END = 67  # serial number ends here
_I_DATE_END = 105  # txn date ends here (time tokens follow)
_I_TIME_END = 210  # time + value-date zone ends here
_I_DESC_END = 454  # description zone ends here
_I_CRDR_END = 482  # CR/DR marker ends here
_I_CCY_END = 518  # CCY column ends here ("INR")
_I_AMT_END = 588  # amount ends here; balance follows

# First-token values that indicate header / footer / summary rows to skip
_I_SKIP_FIRST = {
    "Srl",
    "Page",
    "IDBI",
    "Our",
    "Important",
    "Contents",
    "debit,",
    "DO",
    "reason.",
    "ask",
    "Service",
    "This",
    "Statement",
    "Dr",
    "Debits",
    "Credits",
    "Transaction",
    "YOUR",
    "A/C",
    "Status",
    "*",
}


def _idbi_is_date(text):
    """True if text looks like an IDBI date: dd/mm/yyyy."""
    try:
        from datetime import datetime

        datetime.strptime(text, "%d/%m/%Y")
        return True
    except ValueError:
        return False


def _idbi_amt(text):
    """Parse amount string (may have commas) → float. Returns None if not a number."""
    try:
        return float(text.replace(",", "").strip())
    except ValueError:
        return None


def parse_idbi_pdf(file_stream, password=None):
    """
    Parse an IDBI Bank statement PDF into a DataFrame with columns:
        date, narration, value_date, debit, credit, balance

    Each transaction occupies exactly one PDF row:
        srl | txn_date | time | am_pm | value_date | description | CR/DR | INR | amount | balance

    The CR/DR column ("Dr." or "Cr.") drives the debit/credit split.
    """
    from datetime import datetime as _dt
    import pandas as pd
    import pdfplumber
    from fastapi import HTTPException

    transactions = []

    try:
        pdf = pdfplumber.open(file_stream, password=password)
    except Exception:
        raise HTTPException(status_code=401, detail={"error": "PASSWORD_REQUIRED"})

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

                if not texts:
                    continue

                # ── Skip header / footer / summary rows ──────────────────
                # Transaction rows always start with an integer serial number
                # immediately followed by a dd/mm/yyyy date.
                if texts[0] in _I_SKIP_FIRST:
                    continue
                if not texts[0].isdigit():
                    continue
                if len(texts) < 2 or not _idbi_is_date(texts[1]):
                    continue

                # ── Parse transaction row ────────────────────────────────
                try:
                    txn_date = _dt.strptime(texts[1], "%d/%m/%Y")
                except ValueError:
                    continue

                desc = ""
                value_date = ""
                cr_dr = ""
                amount = 0.0
                balance = 0.0

                for w in row_words[2:]:  # skip srl + txn_date tokens
                    x, t = w["x0"], w["text"]

                    if x >= _I_AMT_END:  # Balance column
                        v = _idbi_amt(t)
                        if v is not None:
                            balance = v

                    elif x >= _I_CCY_END:  # Amount column
                        v = _idbi_amt(t)
                        if v is not None:
                            amount = v

                    elif x >= _I_CRDR_END:  # CCY column ("INR") → skip
                        pass

                    elif x >= _I_DESC_END:  # CR/DR column ("Dr." or "Cr.")
                        cr_dr = t

                    elif x >= _I_TIME_END:  # Description zone (x 210–453)
                        desc += t + " "

                    else:  # Time / AM-PM / Value Date zone
                        if _idbi_is_date(t):
                            value_date = t
                        # HH:MM:SS, AM, PM tokens are silently skipped

                debit = amount if cr_dr == "Dr." else 0.0
                credit = amount if cr_dr == "Cr." else 0.0

                transactions.append(
                    {
                        "date": txn_date,
                        "narration": " ".join(desc.split()),  # normalise spaces
                        "value_date": value_date,
                        "debit": debit,
                        "credit": credit,
                        "balance": balance,
                    }
                )

    df = pd.DataFrame(transactions)
    return df




