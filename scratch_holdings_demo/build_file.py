def fw(v, w):
    v = str(v)
    if len(v) > w:
        raise ValueError(f"{v!r} exceeds width {w}")
    return v.ljust(w)

def line(account_id, security_id, as_of_date, quantity, market_value, currency):
    return fw(account_id, 12) + fw(security_id, 12) + fw(as_of_date, 8) + fw(quantity, 15) + fw(market_value, 15) + fw(currency, 3)

currencies = ["USD", "EUR", "GBP"]
lines = []
for i in range(1, 21):
    acc = f"ACC{i:05d}"
    sec = f"SEC{i:05d}"
    qty = f"{(i * 137) % 5000 + 100}.0000"
    mv = f"{(i * 977) % 90000 + 10000}.00"
    ccy = currencies[i % 3]
    lines.append(line(acc, sec, "20260301", qty, mv, ccy))

with open("HOLDINGS_20260301.txt", "w", encoding="utf-8") as f:
    f.write("\n".join(lines) + "\n")

print("wrote", len(lines), "rows,", sum(len(l) + 1 for l in lines), "bytes")
