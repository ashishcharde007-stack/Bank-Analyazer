# app/parser.py

import pdfplumber
import pandas as pd
from datetime import datetime


def parse_hdfc_pdf(file_stream):

    transactions = []
    current_txn = None
    with pdfplumber.open(file_stream) as pdf:
        for page in pdf.pages:

            words = page.extract_words()

            # Group words by row using vertical position
            rows = {}
            for w in words:
                top = round(w["top"], 0)
                rows.setdefault(top, []).append(w)

            for row_words in rows.values():

                # Sort words left to right
                row_words = sorted(row_words, key=lambda x: x["x0"])

                full_row = " ".join([w["text"] for w in row_words])

                # Skip header rows
                if "Date" in full_row and "Narration" in full_row:
                    continue

                # Check if first word is date
                first_word = row_words[0]["text"]

                if "/" in first_word and len(first_word) >= 8:
                    try:
                        date = datetime.strptime(first_word, "%d/%m/%y")

                        # Column detection by X position
                        narration = ""
                        ref_no = ""
                        value_date = ""
                        withdrawal = ""
                        deposit = ""
                        balance = ""

                        for w in row_words:

                            x = w["x0"]

                            if 60 < x < 250:
                                narration += w["text"] + " "

                            elif 250 <= x < 330:
                                ref_no += w["text"] + " "

                            elif 330 <= x < 390:
                                value_date += w["text"] + " "

                            elif 390 <= x < 470:
                                withdrawal += w["text"] + " "

                            elif 470 <= x < 550:
                                deposit += w["text"] + " "

                            elif x >= 550:
                                balance += w["text"] + " "

                        withdrawal = float(withdrawal.replace(",", "").strip()) if withdrawal.strip() else 0.0
                        deposit = float(deposit.replace(",", "").strip()) if deposit.strip() else 0.0
                        balance = float(balance.replace(",", "").strip()) if balance.strip() else 0.0

                        transactions.append({
                            "date": date,
                            "narration": narration.strip(),
                            "ref_no": ref_no.strip(),
                            "value_date": value_date.strip(),
                            "debit": withdrawal,
                            "credit": deposit,
                            "balance": balance
                        })

                    except:
                        continue

   
    df = pd.DataFrame(transactions)
    return df