"""
TechTrove E-Commerce - Data Integration Pipeline (Week 08)
=========================================================
อ่านข้อมูลจาก CSV / Excel / JSON -> สำรวจคุณภาพข้อมูล -> ปรับ schema ->
รวมข้อมูล -> ทำความสะอาด -> merge -> คำนวณ net_sales ->
สร้าง Dimension / Fact / Data Quality Report / Summary

รันได้ตั้งแต่ต้นจนจบ:  python pipeline.py
"""

import json
import os
import unicodedata

import pandas as pd
import numpy as np

DATA_DIR = "data"
OUT_DIR = "output"
os.makedirs(OUT_DIR, exist_ok=True)

pd.set_option("display.width", 200)
pd.set_option("display.max_columns", 50)

dq_log = []   # เก็บบรรทัดของ Data Quality Report


def log(dataset, check, before, after, note=""):
    dq_log.append(
        {"dataset": dataset, "check": check, "before": before, "after": after, "note": note}
    )


# =====================================================================
# STEP 1 : READ  (CSV / Excel / JSON)
# =====================================================================
print("=" * 70)
print("STEP 1: อ่านข้อมูลจากทุกแหล่ง")
print("=" * 70)

orders_jan = pd.read_csv(f"{DATA_DIR}/orders_2026_01.csv")
orders_feb = pd.read_csv(f"{DATA_DIR}/orders_2026_02.csv")
customers = pd.read_csv(f"{DATA_DIR}/customers_crm.csv")
products = pd.read_excel(f"{DATA_DIR}/product_master.xlsx", sheet_name="products")

with open(f"{DATA_DIR}/payments.json", encoding="utf-8") as f:
    payments_raw = json.load(f)
payments = pd.json_normalize(payments_raw)          # flatten payment.method / payment.status

for name, df in [
    ("orders_2026_01", orders_jan),
    ("orders_2026_02", orders_feb),
    ("customers_crm", customers),
    ("product_master", products),
    ("payments", payments),
]:
    print(f"{name:<18} shape={df.shape}  cols={list(df.columns)}")


# =====================================================================
# STEP 2 : DATA PROFILING (ก่อนทำความสะอาด)
# =====================================================================
print()
print("=" * 70)
print("STEP 2: สำรวจคุณภาพข้อมูลเบื้องต้น (BEFORE)")
print("=" * 70)


def profile(name, df, key=None):
    print(f"\n----- {name} -----")
    print("rows / cols :", df.shape)
    info = pd.DataFrame(
        {
            "dtype": df.dtypes.astype(str),
            "n_null": df.isna().sum(),
            "pct_null": (df.isna().mean() * 100).round(2),
            "n_unique": df.nunique(),
        }
    )
    print(info)
    print("duplicated rows :", int(df.duplicated().sum()))
    if key:
        print(f"duplicated {key} :", int(df.duplicated(subset=[key]).sum()))
    return info


profile("orders_2026_01", orders_jan, "order_id")
profile("orders_2026_02", orders_feb, "order_id")
profile("customers_crm", customers, "customer_id")
profile("product_master", products, "product_id")
profile("payments", payments, "payment_id")

# --- บันทึกสภาพ "ก่อน" ลง DQ report ---
before_stats = {
    "orders_rows": len(orders_jan) + len(orders_feb),
    "orders_dup_id": int(orders_jan.order_id.duplicated().sum() + orders_feb.order_id.duplicated().sum()),
    "orders_null_price": int(orders_jan.unit_price.isna().sum() + orders_feb.unit_price.isna().sum()),
    "orders_bad_qty": int((orders_jan.quantity <= 0).sum() + (orders_feb.qty <= 0).sum()),
    "cust_rows": len(customers),
    "cust_dup_id": int(customers.customer_id.duplicated().sum()),
    "cust_null_email": int(customers.email.isna().sum()),
    "cust_province_variants": int(customers.province.nunique()),
    "pay_rows": len(payments),
    "pay_dup_id": int(payments.payment_id.duplicated().sum()),
}
print("\nสรุปปัญหาที่พบก่อนทำความสะอาด:", before_stats)


# =====================================================================
# STEP 3 : SCHEMA ALIGNMENT  (ทำ schema ของ 2 เดือนให้ตรงกัน)
# =====================================================================
print()
print("=" * 70)
print("STEP 3: ปรับ Schema ของคำสั่งซื้อแต่ละเดือนให้ตรงกัน")
print("=" * 70)

STD_COLS = [
    "order_id", "order_date", "customer_id", "product_id",
    "quantity", "unit_price", "discount", "channel",
]

jan = orders_jan.rename(columns={})          # ม.ค. ใช้ชื่อมาตรฐานอยู่แล้ว
feb = orders_feb.rename(
    columns={"ordered_at": "order_date", "qty": "quantity", "discount_pct": "discount"}
)

jan["source_file"] = "orders_2026_01.csv"
feb["source_file"] = "orders_2026_02.csv"

print("Jan columns :", list(jan.columns))
print("Feb columns :", list(feb.columns))
log("orders", "schema alignment",
    "ordered_at/qty/discount_pct", "order_date/quantity/discount",
    "เปลี่ยนชื่อคอลัมน์ของไฟล์ ก.พ. ให้ตรงกับ ม.ค.")


# =====================================================================
# STEP 4 : CONCAT
# =====================================================================
print()
print("=" * 70)
print("STEP 4: รวมคำสั่งซื้อด้วย pd.concat()")
print("=" * 70)

orders = pd.concat([jan[STD_COLS + ["source_file"]], feb[STD_COLS + ["source_file"]]],
                   ignore_index=True)
print("orders รวม =", orders.shape)


# =====================================================================
# STEP 5 : CLEANING & STANDARDIZATION
# =====================================================================
print()
print("=" * 70)
print("STEP 5: ทำความสะอาดและปรับข้อมูลให้เป็นมาตรฐาน")
print("=" * 70)

# ---------- 5.1 วันที่ (2 รูปแบบ) ----------
d1 = pd.to_datetime(orders["order_date"], format="%Y-%m-%d %H:%M:%S", errors="coerce")
d2 = pd.to_datetime(orders["order_date"], format="%d/%m/%Y %H:%M", errors="coerce")
orders["order_date"] = d1.fillna(d2)
n_bad_date = int(orders["order_date"].isna().sum())
print("แปลงวันที่ไม่สำเร็จ :", n_bad_date)
log("orders", "date format", "2 รูปแบบ (ISO / DD-MM-YYYY)", "datetime64 รูปแบบเดียว",
    f"แปลงไม่ได้ {n_bad_date} แถว")

# ---------- 5.2 ส่วนลด ('5%' -> 0.05) ----------
disc = orders["discount"].astype(str).str.strip()
has_pct = disc.str.endswith("%")
orders["discount"] = np.where(
    has_pct,
    pd.to_numeric(disc.str.rstrip("%"), errors="coerce") / 100,
    pd.to_numeric(disc, errors="coerce"),
).astype(float)
orders["discount"] = orders["discount"].fillna(0).clip(0, 1)
print("ค่า discount หลังแปลง :", sorted(orders.discount.unique()))
log("orders", "discount format", "0.05 และ '5%'", "float 0-1", "แปลง % เป็นสัดส่วน")

# ---------- 5.3 ชนิดข้อมูล ----------
orders["quantity"] = pd.to_numeric(orders["quantity"], errors="coerce").astype("Int64")
orders["unit_price"] = pd.to_numeric(orders["unit_price"], errors="coerce")
for c in ["order_id", "customer_id", "product_id", "channel"]:
    orders[c] = orders[c].astype(str).str.strip()

# ---------- 5.4 ข้อมูลซ้ำ ----------
n0 = len(orders)
orders = orders.drop_duplicates()
n1 = len(orders)
orders = orders.drop_duplicates(subset=["order_id"], keep="first")
n2 = len(orders)
print(f"ลบแถวซ้ำทั้งแถว {n0-n1} แถว, ลบ order_id ซ้ำอีก {n1-n2} แถว")
log("orders", "duplicate rows", n0, n2, f"ซ้ำทั้งแถว {n0-n1}, order_id ซ้ำ {n1-n2}")

# ---------- 5.5 ค่าผิดปกติ: quantity <= 0 ----------
all_order_ids = set(orders["order_id"])   # เก็บไว้ตรวจ orphan payment
bad_qty = orders[orders["quantity"] <= 0]
print("quantity <= 0 :", len(bad_qty))
print(bad_qty[["order_id", "customer_id", "product_id", "quantity"]])
orders = orders[orders["quantity"] > 0].copy()
log("orders", "invalid quantity (<=0)", len(bad_qty), 0, "ตัดออกจาก fact table")

# ---------- 5.6 unit_price ที่หายไป -> เติมด้วย standard_price ----------
price_map = products.set_index("product_id")["standard_price"]
missing_price = orders["unit_price"].isna().sum()
orders["price_imputed"] = orders["unit_price"].isna()
orders["unit_price"] = orders["unit_price"].fillna(orders["product_id"].map(price_map))
still_missing = int(orders["unit_price"].isna().sum())
print(f"unit_price ว่าง {missing_price} แถว -> เติมจาก standard_price, เหลือว่าง {still_missing}")
log("orders", "missing unit_price", int(missing_price), still_missing,
    "เติมด้วย standard_price จาก product_master")

# ---------- 5.7 customers : email / province / duplicates ----------
cust = customers.copy()
cust["email"] = cust["email"].astype(str).str.strip().str.lower().replace("nan", np.nan)
cust["full_name"] = cust["full_name"].astype(str).str.strip()
cust["customer_id"] = cust["customer_id"].astype(str).str.strip()
cust["signup_date"] = pd.to_datetime(cust["signup_date"], errors="coerce")

PROVINCE_MAP = {
    "กรุงเทพมหานคร": "กรุงเทพมหานคร", "กทม.": "กรุงเทพมหานคร", "กทม": "กรุงเทพมหานคร",
    "bangkok": "กรุงเทพมหานคร",
    "ชลบุรี": "ชลบุรี", "chonburi": "ชลบุรี",
    "ระยอง": "ระยอง", "rayong": "ระยอง",
    "เชียงใหม่": "เชียงใหม่", "chiang mai": "เชียงใหม่", "chiangmai": "เชียงใหม่",
    "ขอนแก่น": "ขอนแก่น", "khon kaen": "ขอนแก่น",
    "ภูเก็ต": "ภูเก็ต", "phuket": "ภูเก็ต",
}


def norm_province(p):
    if pd.isna(p):
        return "ไม่ระบุ"
    s = unicodedata.normalize("NFC", str(p)).strip()
    s = s.replace("เเ", "แ")               # เ+เ ที่พิมพ์แทน แ
    s = " ".join(s.split())
    return PROVINCE_MAP.get(s.lower(), PROVINCE_MAP.get(s, s))


print("\nจังหวัดก่อนปรับ :", sorted(cust.province.dropna().unique()))
cust["province"] = cust["province"].map(norm_province)
print("จังหวัดหลังปรับ :", sorted(cust.province.unique()))
log("customers", "province standardization",
    before_stats["cust_province_variants"], int(cust.province.nunique()),
    "รวมชื่อไทย/อังกฤษ/ตัวย่อ/สะกดผิด")

n0 = len(cust)
cust = cust.drop_duplicates()
cust = cust.drop_duplicates(subset=["customer_id"], keep="first")
print(f"ลูกค้า {n0} -> {len(cust)} แถว")
log("customers", "duplicate customer_id", n0, len(cust), "เก็บแถวแรก")
log("customers", "email lowercase/trim", int(customers.email.dropna().str.contains("[A-Z]").sum()), 0,
    "แปลงเป็นตัวพิมพ์เล็กและตัดช่องว่าง")
log("customers", "missing email", int(cust.email.isna().sum()), int(cust.email.isna().sum()),
    "คงไว้ (ไม่ใช่ key)")

# ---------- 5.8 products ----------
prod = products.copy()
prod["product_id"] = prod["product_id"].astype(str).str.strip()
prod["product_name"] = prod["product_name"].astype(str).str.strip()
prod["category"] = prod["category"].astype(str).str.strip().str.title()
prod["standard_price"] = pd.to_numeric(prod["standard_price"], errors="coerce")
prod["active_flag"] = prod["active_flag"].astype(str).str.upper().str[0]
n0 = len(prod)
prod = prod.drop_duplicates(subset=["product_id"], keep="first")
log("products", "duplicate product_id", n0, len(prod), "-")

# ---------- 5.9 payments ----------
pay = payments.rename(columns={"payment.method": "payment_method",
                               "payment.status": "payment_status"}).copy()
pay["paid_at"] = pd.to_datetime(pay["paid_at"], errors="coerce")
pay["payment_status"] = pay["payment_status"].astype(str).str.strip().str.upper()
pay["payment_method"] = pay["payment_method"].astype(str).str.strip()
n0 = len(pay)
pay = pay.drop_duplicates()
pay = pay.drop_duplicates(subset=["order_id"], keep="first")
print(f"\npayments {n0} -> {len(pay)} แถว")
log("payments", "duplicate payment", n0, len(pay), "ซ้ำทั้งแถว/ซ้ำ order_id")


# =====================================================================
# STEP 6 : MERGE  (orders + customers + products + payments)
# =====================================================================
print()
print("=" * 70)
print("STEP 6: เชื่อมโยงข้อมูลด้วย pd.merge() และตรวจสอบการจับคู่")
print("=" * 70)

df = orders.merge(cust[["customer_id", "full_name", "province", "signup_date"]],
                  on="customer_id", how="left", indicator="m_cust")
df = df.merge(prod[["product_id", "product_name", "category", "standard_price", "active_flag"]],
              on="product_id", how="left", indicator="m_prod")
df = df.merge(pay[["order_id", "payment_id", "payment_method", "payment_status", "paid_at"]],
              on="order_id", how="left", indicator="m_pay")

unmatched_cust = df.loc[df.m_cust == "left_only", "customer_id"].unique()
unmatched_prod = df.loc[df.m_prod == "left_only", "product_id"].unique()
unmatched_pay = int((df.m_pay == "left_only").sum())
orphan_pay = pay.loc[~pay.order_id.isin(all_order_ids), "order_id"].tolist()

print("order ที่หา customer ไม่เจอ :", sorted(unmatched_cust), f"({(df.m_cust=='left_only').sum()} แถว)")
print("order ที่หา product ไม่เจอ  :", sorted(unmatched_prod), f"({(df.m_prod=='left_only').sum()} แถว)")
print("order ที่ไม่มีการชำระเงิน    :", unmatched_pay)
print("payment ที่ไม่มี order       :", orphan_pay)

log("integration", "orders ไม่พบ customer", int((df.m_cust == "left_only").sum()), 0,
    f"customer_id: {', '.join(sorted(unmatched_cust)) or '-'} -> ใส่ UNKNOWN")
log("integration", "orders ไม่พบ product", int((df.m_prod == "left_only").sum()), 0,
    f"product_id: {', '.join(sorted(unmatched_prod)) or '-'} -> ใส่ UNKNOWN")
log("integration", "orders ไม่มี payment", unmatched_pay, unmatched_pay, "-")
log("integration", "payment ไม่มี order (orphan)", len(orphan_pay), len(orphan_pay),
    f"{', '.join(orphan_pay) or '-'} -> ไม่นำเข้า fact")

# เติมสมาชิก UNKNOWN ให้ dimension เพื่อไม่ให้ join แล้วข้อมูลหาย
df["province"] = df["province"].fillna("ไม่ระบุ")
df["full_name"] = df["full_name"].fillna("UNKNOWN CUSTOMER")
df["product_name"] = df["product_name"].fillna("UNKNOWN PRODUCT")
df["category"] = df["category"].fillna("Unknown")
df["payment_status"] = df["payment_status"].fillna("NO_PAYMENT")
df["payment_method"] = df["payment_method"].fillna("NO_PAYMENT")


# =====================================================================
# STEP 7 : NET SALES
# =====================================================================
print()
print("=" * 70)
print("STEP 7: คำนวณยอดขายสุทธิ net_sales = quantity * unit_price * (1 - discount)")
print("=" * 70)

df["quantity"] = df["quantity"].astype(int)
df["net_sales"] = (df["quantity"] * df["unit_price"] * (1 - df["discount"])).round(2)
df["gross_sales"] = (df["quantity"] * df["unit_price"]).round(2)
df["is_paid"] = df["payment_status"].eq("PAID")
df["order_month"] = df["order_date"].dt.to_period("M").astype(str)

print(df[["order_id", "quantity", "unit_price", "discount", "net_sales"]].head())
print("\nยอดขายสุทธิรวมทุกสถานะ :", f"{df.net_sales.sum():,.2f}")
print("ยอดขายสุทธิเฉพาะ PAID  :", f"{df.loc[df.is_paid, 'net_sales'].sum():,.2f}")


# =====================================================================
# STEP 8 : DIMENSION / FACT
# =====================================================================
print()
print("=" * 70)
print("STEP 8: สร้าง Dimension Table และ Fact Table")
print("=" * 70)

dim_customer = cust[["customer_id", "full_name", "email", "province", "signup_date"]].copy()
unknown_c = pd.DataFrame(
    [{"customer_id": c, "full_name": "UNKNOWN CUSTOMER", "email": np.nan,
      "province": "ไม่ระบุ", "signup_date": pd.NaT} for c in sorted(unmatched_cust)]
)
dim_customer = pd.concat([dim_customer, unknown_c], ignore_index=True).sort_values("customer_id")

dim_product = prod[["product_id", "product_name", "category", "standard_price", "active_flag"]].copy()
unknown_p = pd.DataFrame(
    [{"product_id": p, "product_name": "UNKNOWN PRODUCT", "category": "Unknown",
      "standard_price": np.nan, "active_flag": "N"} for p in sorted(unmatched_prod)]
)
dim_product = pd.concat([dim_product, unknown_p], ignore_index=True).sort_values("product_id")

fact_sales = df[[
    "order_id", "order_date", "order_month", "customer_id", "product_id",
    "quantity", "unit_price", "discount", "gross_sales", "net_sales",
    "channel", "payment_id", "payment_method", "payment_status", "paid_at",
    "is_paid", "price_imputed", "source_file",
]].sort_values("order_id").reset_index(drop=True)

print("dim_customer :", dim_customer.shape)
print("dim_product  :", dim_product.shape)
print("fact_sales   :", fact_sales.shape)


# =====================================================================
# STEP 9 : SUMMARY
# =====================================================================
print()
print("=" * 70)
print("STEP 9: วิเคราะห์ยอดขายตามจังหวัดและหมวดสินค้า (เฉพาะรายการที่ชำระเงินสำเร็จ)")
print("=" * 70)

paid = fact_sales[fact_sales.is_paid].merge(
    dim_customer[["customer_id", "province"]], on="customer_id", how="left"
).merge(dim_product[["product_id", "category"]], on="product_id", how="left")

summary_by_province = (
    paid.groupby("province")
    .agg(n_orders=("order_id", "nunique"),
         n_customers=("customer_id", "nunique"),
         total_qty=("quantity", "sum"),
         net_sales=("net_sales", "sum"))
    .reset_index()
)
summary_by_province["avg_order_value"] = (
    summary_by_province.net_sales / summary_by_province.n_orders).round(2)
summary_by_province["net_sales"] = summary_by_province.net_sales.round(2)
summary_by_province["pct_of_total"] = (
    summary_by_province.net_sales / summary_by_province.net_sales.sum() * 100).round(2)
summary_by_province = summary_by_province.sort_values("net_sales", ascending=False).reset_index(drop=True)

summary_by_category = (
    paid.groupby("category")
    .agg(n_orders=("order_id", "nunique"),
         total_qty=("quantity", "sum"),
         net_sales=("net_sales", "sum"))
    .reset_index()
)
summary_by_category["avg_order_value"] = (
    summary_by_category.net_sales / summary_by_category.n_orders).round(2)
summary_by_category["net_sales"] = summary_by_category.net_sales.round(2)
summary_by_category["pct_of_total"] = (
    summary_by_category.net_sales / summary_by_category.net_sales.sum() * 100).round(2)
summary_by_category = summary_by_category.sort_values("net_sales", ascending=False).reset_index(drop=True)

print(summary_by_province.to_string(index=False))
print()
print(summary_by_category.to_string(index=False))


# =====================================================================
# STEP 10 : DATA QUALITY REPORT (AFTER) + EXPORT
# =====================================================================
print()
print("=" * 70)
print("STEP 10: Data Quality Report และบันทึกไฟล์ผลลัพธ์")
print("=" * 70)

log("summary", "orders ทั้งหมดหลัง integration", before_stats["orders_rows"], len(fact_sales),
    "หลังลบซ้ำและตัดค่าผิดปกติ")
log("summary", "customers ใน dim", before_stats["cust_rows"], len(dim_customer), "รวมสมาชิก UNKNOWN")
log("summary", "products ใน dim", len(products), len(dim_product), "รวมสมาชิก UNKNOWN")
log("summary", "null ทั้งหมดใน fact_sales", "-", int(fact_sales.isna().sum().sum()),
    "ส่วนใหญ่มาจาก order ที่ไม่มี payment")

dq_report = pd.DataFrame(dq_log)[["dataset", "check", "before", "after", "note"]]
print(dq_report.to_string(index=False))

dim_customer.to_csv(f"{OUT_DIR}/dim_customer.csv", index=False, encoding="utf-8-sig")
dim_product.to_csv(f"{OUT_DIR}/dim_product.csv", index=False, encoding="utf-8-sig")
fact_sales.to_csv(f"{OUT_DIR}/fact_sales.csv", index=False, encoding="utf-8-sig")
dq_report.to_csv(f"{OUT_DIR}/data_quality_report.csv", index=False, encoding="utf-8-sig")
summary_by_province.to_csv(f"{OUT_DIR}/summary_by_province.csv", index=False, encoding="utf-8-sig")
summary_by_category.to_csv(f"{OUT_DIR}/summary_by_category.csv", index=False, encoding="utf-8-sig")

print("\nบันทึกไฟล์เรียบร้อย 6 ไฟล์ที่โฟลเดอร์", OUT_DIR)

# ----- ตัวเลขสำหรับตอบคำถามวิเคราะห์ -----
print("\n===== ตัวเลขประกอบการตอบคำถาม =====")
print("จังหวัดยอดขายสูงสุด :", summary_by_province.iloc[0].province,
      f"{summary_by_province.iloc[0].net_sales:,.2f}")
print("หมวดสินค้ายอดขายสูงสุด :", summary_by_category.iloc[0].category,
      f"{summary_by_category.iloc[0].net_sales:,.2f}")
print("\nยอดขายตามช่องทาง:")
print(paid.groupby("channel").net_sales.sum().sort_values(ascending=False).round(2).to_string())
print("\nยอดขายตามเดือน:")
print(paid.groupby("order_month").net_sales.sum().round(2).to_string())
print("\nสถานะการชำระเงิน:")
print(fact_sales.payment_status.value_counts().to_string())
print("\nTop 5 สินค้าตามยอดขาย:")
top5 = (paid.merge(dim_product[["product_id", "product_name"]], on="product_id")
        .groupby(["product_id", "product_name"]).net_sales.sum()
        .sort_values(ascending=False).head(5).round(2))
print(top5.to_string())
print("\nTop 5 ลูกค้าตามยอดขาย:")
top5c = (paid.drop(columns=["province"])
         .merge(dim_customer[["customer_id", "province"]], on="customer_id")
         .groupby(["customer_id", "province"]).net_sales.sum()
         .sort_values(ascending=False).head(5).round(2))
print(top5c.to_string())
print("\nส่วนลดที่ให้ไปทั้งหมด :",
      f"{(fact_sales.gross_sales - fact_sales.net_sales).sum():,.2f}")
print("มูลค่าที่สูญเสียจาก FAILED/REFUNDED :",
      f"{fact_sales.loc[~fact_sales.is_paid, 'net_sales'].sum():,.2f}")