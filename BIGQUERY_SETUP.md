# Hướng Dẫn Cấu Hình Google BigQuery

## Bước 1: Tạo Google Cloud Project

1. Truy cập [Google Cloud Console](https://console.cloud.google.com/)
2. Tạo project mới hoặc chọn project có sẵn
3. Ghi nhớ **Project ID** (ví dụ: `my-project-12345`)

## Bước 2: Kích Hoạt BigQuery API

1. Trong Google Cloud Console, vào menu **APIs & Services** > **Library**
2. Tìm kiếm "BigQuery API"
3. Click **Enable** để kích hoạt API

## Bước 3: Tạo Service Account

1. Vào menu **IAM & Admin** > **Service Accounts**
2. Click **Create Service Account**
3. Điền thông tin:
   - **Service account name**: `bigquery-connector` (hoặc tên bạn muốn)
   - **Service account ID**: sẽ tự động tạo
   - **Description**: "Service account for BigQuery access"
4. Click **Create and Continue**

## Bước 4: Gán Quyền (Roles)

Trong bước "Grant this service account access to project", thêm các roles sau:

- **BigQuery Admin** (để có quyền đầy đủ)
  
  HOẶC các quyền cụ thể hơn:
  - **BigQuery Data Viewer** (xem dữ liệu)
  - **BigQuery Job User** (chạy queries)
  - **BigQuery User** (sử dụng BigQuery)

Click **Continue** > **Done**

## Bước 5: Tạo và Tải Key JSON

1. Trong danh sách Service Accounts, click vào service account vừa tạo
2. Chọn tab **Keys**
3. Click **Add Key** > **Create new key**
4. Chọn **JSON** format
5. Click **Create** - file JSON sẽ được tải về máy

**⚠️ LƯU Ý QUAN TRỌNG:**
- File JSON này chứa thông tin xác thực quan trọng
- **KHÔNG** commit file này lên Git
- Lưu file ở nơi an toàn
- Đổi tên file thành `service-account-key.json` (hoặc tên bạn muốn)

## Bước 6: Cài Đặt Dependencies

```bash
pip install -r requirements.txt
```

## Bước 7: Cấu Hình Code

### Cách 1: Sử dụng đường dẫn trực tiếp trong code

Mở file `main.py` và chỉnh sửa:

```python
CREDENTIALS_PATH = "/path/to/your/service-account-key.json"  # Đường dẫn đầy đủ tới file JSON
PROJECT_ID = "your-project-id"  # Project ID từ bước 1
```

### Cách 2: Sử dụng biến môi trường (KHUYẾN NGHỊ)

**Linux/Mac:**
```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/service-account-key.json"
export GCP_PROJECT_ID="your-project-id"
```

**Windows (Command Prompt):**
```cmd
set GOOGLE_APPLICATION_CREDENTIALS=C:\path\to\your\service-account-key.json
set GCP_PROJECT_ID=your-project-id
```

**Windows (PowerShell):**
```powershell
$env:GOOGLE_APPLICATION_CREDENTIALS="C:\path\to\your\service-account-key.json"
$env:GCP_PROJECT_ID="your-project-id"
```

### Cách 3: Tạo file .env (KHUYẾN NGHỊ cho development)

1. Tạo file `.env` trong thư mục project:
```
GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/service-account-key.json
GCP_PROJECT_ID=your-project-id
```

2. Cài đặt python-dotenv:
```bash
pip install python-dotenv
```

3. Thêm vào đầu `main.py`:
```python
from dotenv import load_dotenv
load_dotenv()
```

4. Thêm `.env` vào `.gitignore`:
```
.env
service-account-key.json
*.json
```

## Bước 8: Chạy Code

```bash
python main.py
```

## Kiểm Tra Kết Nối

Nếu cấu hình đúng, bạn sẽ thấy:
```
✓ Đã kết nối thành công tới BigQuery project: your-project-id
```

## Xử Lý Lỗi Thường Gặp

### Lỗi: "Could not automatically determine credentials"
- Kiểm tra đường dẫn file JSON có đúng không
- Kiểm tra biến môi trường `GOOGLE_APPLICATION_CREDENTIALS` đã được set chưa

### Lỗi: "Permission denied"
- Service account chưa có quyền truy cập BigQuery
- Quay lại Bước 4 để gán quyền

### Lỗi: "Project not found"
- Kiểm tra Project ID có chính xác không
- Đảm bảo project đã được tạo và kích hoạt

### Lỗi: "API not enabled"
- BigQuery API chưa được kích hoạt
- Quay lại Bước 2

## Sử Dụng Code

### Query đơn giản:
```python
bq = BigQueryConnector(
    credentials_path="path/to/key.json",
    project_id="your-project-id"
)

query = """
    SELECT *
    FROM `your-project.your_dataset.your_table`
    LIMIT 10
"""
df = bq.execute_query(query)
print(df)
```

### Liệt kê datasets:
```python
bq.list_datasets()
```

### Liệt kê tables trong dataset:
```python
bq.list_tables("your_dataset_id")
```

### Xem schema của table:
```python
bq.get_table_schema("your_dataset_id", "your_table_id")
```

## Tài Nguyên Tham Khảo

- [BigQuery Documentation](https://cloud.google.com/bigquery/docs)
- [Python Client Library](https://cloud.google.com/python/docs/reference/bigquery/latest)
- [BigQuery SQL Reference](https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax)
