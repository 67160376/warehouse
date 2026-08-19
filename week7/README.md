# Omnichannel Retail ETL Pipeline — README

## 1. โครงสร้างไฟล์

```
pipeline.py              โค้ด ETL ทั้งหมด (extract -> transform -> validate -> load)
source_data/              ไฟล์ต้นฉบับ (ไม่ถูกแก้ไขโดย pipeline)
    customers.csv
    products.csv
    orders_batch_1.csv
    orders_batch_2.csv
    orders_batch_3.csv
output/
    retail_dw.db          SQLite Star Schema หลังโหลดครบ 3 batch
    quarantine.csv         ระเบียนที่ไม่ผ่านคุณภาพ พร้อม reason_code / source_batch
    pipeline_run_log.csv   ประวัติการรันแต่ละครั้ง (export จากตาราง pipeline_run_log)
    README.md              ไฟล์นี้
```

## 2. วิธีติดตั้ง

```bash
pip install pandas
```

(ใช้ sqlite3 และ csv จาก standard library ของ Python เท่านั้น ไม่ต้องติดตั้งฐานข้อมูลแยก)

## 3. วิธีรัน

```bash
python3 pipeline.py
```

สคริปต์จะสร้าง `output/retail_dw.db`, `output/quarantine.csv`, `output/pipeline_run_log.csv` ใหม่ทุกครั้ง (ลบไฟล์เก่าก่อนเริ่ม) แล้วรันตามลำดับ:

1. `orders_batch_1` (โหลดครั้งแรก)
2. `orders_batch_1` ซ้ำ (พิสูจน์ idempotency — จำนวนแถวใน `fact_sales` ต้องไม่เพิ่ม)
3. `orders_batch_2` (incremental)
4. `orders_batch_3` (incremental)

ท้ายสุดจะพิมพ์สรุป KPI (rows read / valid / rejected / loaded / net sales) ออกทาง console

ถ้าต้องการใช้ pipeline กับ batch อื่น หรือ error mode อื่น ให้เรียกผ่าน `PipelineConfig` และ `run_pipeline()` โดยตรง เช่น

```python
from pipeline import PipelineConfig, run_pipeline

config = PipelineConfig(
    input_path="source_data",
    output_database="output/retail_dw.db",
    batch_list=["orders_batch_2"],
    error_mode="quarantine",   # หรือ "fail_fast"
)
run_pipeline(config)
```

## 4. โครงสร้าง Star Schema

Grain ของ `fact_sales`: **หนึ่งรายการขายสินค้าที่ผ่านการตรวจสอบแล้ว ต่อหนึ่ง order_id**

| ตาราง | บทบาท | คีย์ |
|---|---|---|
| `dim_customer` | Dimension ลูกค้า | `customer_key` (PK, autoincrement), `customer_id` (unique business key) |
| `dim_product` | Dimension สินค้า | `product_key` (PK, autoincrement), `product_id` (unique business key) |
| `dim_date` | Dimension วันที่ | `date_key` (PK, รูปแบบ YYYYMMDD) |
| `fact_sales` | Fact การขาย | `fact_key` (PK), `order_id` (unique), FK ไปยัง `date_key` / `customer_key` / `product_key` |
| `pipeline_run_log` | Metadata การรัน | `run_id` (PK) — batch, started_at, ended_at, rows_read/valid/rejected/loaded, status |
| `load_watermark` | Watermark ต่อ order_id | `order_id` (PK), `updated_at` ล่าสุดที่โหลดสำเร็จ — ใช้ทำ incremental load |

**การป้องกันข้อมูลซ้ำ / Idempotency:** `fact_sales.order_id` เป็น `UNIQUE`, และก่อน insert ทุกแถวจะเทียบ `updated_at` ของแถวใหม่กับค่าที่บันทึกไว้ใน `load_watermark` — ถ้าไม่ใหม่กว่าเดิมจะข้าม (no-op) ถ้าใหม่กว่าจะ `UPDATE` (upsert ผ่าน `ON CONFLICT ... DO UPDATE`) ทำให้รัน batch เดิมซ้ำกี่ครั้งก็ไม่เพิ่มจำนวนแถว และรองรับ late-arriving/updated records ข้าม batch ได้ด้วย

**Dedup ภายใน batch:** ถ้า `order_id` ซ้ำกันในไฟล์เดียวกัน จะเก็บเฉพาะแถวที่ `updated_at` ล่าสุด (`drop_duplicates(keep="last")` หลัง sort ตามเวลา) ก่อนส่งเข้า load

**สูตร Run Log (Acceptance Test #7):**
`rows_read = rows_valid(หลัง dedup) + duplicates_removed_ใน_batch + rows_rejected`

จำนวน duplicates ที่ถูกตัดออกจะปรากฏใน log ตอนรัน (log line `TRANSFORM ... deduplicated N duplicate order_id rows`) แม้จะไม่มีคอลัมน์แยกใน `pipeline_run_log` ก็สามารถคำนวณย้อนกลับได้จากสูตรข้างต้น

## 5. กฎ Data Quality ที่ตรวจสอบ (เรียงตามลำดับความสำคัญ ใครติดก่อนไปก่อน)

`MISSING_ORDER_ID → INVALID_DATETIME → INVALID_UPDATED_AT → MISSING_CUSTOMER_ID → CUSTOMER_NOT_FOUND → PRODUCT_NOT_FOUND → QUANTITY_NOT_NUMERIC → QUANTITY_OUT_OF_RANGE (ต้องอยู่ 1-20) → PRICE_NOT_NUMERIC → PRICE_NOT_POSITIVE → DISCOUNT_NOT_NUMERIC → DISCOUNT_OUT_OF_RANGE (0-100) → UNKNOWN_PAYMENT_METHOD → UNKNOWN_SALES_CHANNEL`

ทุกแถวที่ไม่ผ่านกฎใดกฎหนึ่งจะถูกส่งไป `quarantine.csv` พร้อม `reason_code` และ `source_batch` โดยไม่ทำให้ batch หรือ pipeline ทั้งระบบหยุดทำงาน (ยกเว้นเปิด `error_mode="fail_fast"` เอง)

`payment_method` และ `sales_channel` ถูก normalize ด้วย mapping ที่ตายตัว (case-insensitive):
- payment_method: cash/credit card/promptpay/bank transfer → Cash / Credit Card / PromptPay / Bank Transfer
- sales_channel: store/online/e-commerce/marketplace → Store / Online (e-commerce ถูก map เป็นช่องทางเดียวกับ Online) / Marketplace

## 6. Reflection: เหตุใด Availability จึงมักสำคัญกว่า Strictness ใน Production Pipeline

ใน production pipeline ข้อมูลต้นทางมักไม่สมบูรณ์แบบอยู่เสมอ ไม่ว่าจะเป็นค่าว่าง รูปแบบวันที่ผิด หรือ foreign key ที่อ้างอิงไม่เจอ ถ้า pipeline ออกแบบให้ "เข้มงวด" จนหยุดทั้งระบบทันทีที่เจอแถวผิดเพียงแถวเดียว (fail-fast) ผลกระทบจะลามไปยังข้อมูลที่ถูกต้องอีกหลายพันแถวที่ควรจะโหลดสำเร็จได้ตามปกติ ทำให้รายงานยอดขายรายวันล่าช้าหรือขาดหายทั้งช่วงเวลา ซึ่งสร้างความเสียหายทางธุรกิจมากกว่าการปล่อยให้มีแถวเสียบางส่วนไปอยู่ใน quarantine

การให้ pipeline ยังคง "พร้อมใช้งาน" (available) โดย isolate เฉพาะแถวที่มีปัญหาออกไปพร้อมเหตุผล (reason_code) ทำให้ทีมวิเคราะห์ยังได้ข้อมูลส่วนใหญ่ทันเวลาเพื่อใช้ตัดสินใจ ในขณะที่ทีมข้อมูลสามารถไปแก้ไข root cause ของแถวที่มีปัญหาแยกต่างหากได้ภายหลังโดยไม่ต้อง block การส่งมอบข้อมูล

Strictness ยังคงสำคัญ แต่ควรถูกบังคับใช้ ณ จุดที่เหมาะสม เช่น constraint ระดับฐานข้อมูล (`UNIQUE`, `CHECK`, foreign key) ที่ป้องกันข้อมูลเสียไม่ให้ปนเข้าไปใน fact table จริง มากกว่าการทำให้ทั้ง pipeline ล้มเหลว การแยก error mode เป็น `quarantine` (ค่าเริ่มต้น) กับ `fail_fast` (สำหรับกรณีที่ต้องการความเข้มงวดสูงสุด เช่น regulatory data) จึงเป็นการให้ทางเลือกตามความเสี่ยงของแต่ละ use case แทนที่จะบังคับพฤติกรรมเดียวกับข้อมูลทุกประเภท

สุดท้าย availability ที่ดียังต้องมาพร้อมความโปร่งใส — `pipeline_run_log` และ `reason_code` ทำให้ทุกการตัดสินใจ "ยอมให้ผ่าน" หรือ "กันออก" ตรวจสอบย้อนหลังได้เสมอ ไม่ใช่การซ่อนปัญหาไว้เงียบ ๆ
