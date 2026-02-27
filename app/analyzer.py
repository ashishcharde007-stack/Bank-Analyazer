# app/analyzer.py

import pandas as pd


def analyze_transactions(df):

    total_credit = df["credit"].sum()
    total_debit = df["debit"].sum()
    net_flow = total_credit - total_debit
    avg_balance = df["balance"].mean()
    total_transactions = len(df)

    # Monthly Summary
    df["month"] = df["date"].dt.to_period("M")

    monthly_summary = (
        df.groupby("month")
        .agg(
            total_credit=("credit", "sum"),
            total_debit=("debit", "sum"),
            avg_balance=("balance", "mean"),
            transactions=("date", "count"),
        )
        .reset_index()
    )

    summary = {
        "total_credit": round(total_credit, 2),
        "total_debit": round(total_debit, 2),
        "net_flow": round(net_flow, 2),
        "avg_balance": round(avg_balance, 2),
        "total_transactions": total_transactions
    }

    return summary, monthly_summary