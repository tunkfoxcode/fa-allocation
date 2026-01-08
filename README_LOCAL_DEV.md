# Local Development Guide

Hướng dẫn chạy Finance Allocation Report API trực tiếp từ IDE (không cần Docker).

## Prerequisites

- Python 3.11+
- pip
- GCP credentials file

## Setup

### 1. Cài đặt dependencies

```bash
# Tạo virtual environment (recommended)
python -m venv venv

# Activate virtual environment
# On Linux/Mac:
source venv/bin/activate
# On Windows:
# venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configure Environment

File `.env` đã được tạo sẵn với config cho local development. Kiểm tra và update nếu cần:

```bash
# Edit .env file
nano .env  # hoặc dùng editor bất kỳ
```

**Quan trọng**: Đảm bảo `GCP_CREDENTIALS_PATH` trỏ đúng đến file credentials của bạn:
```
GCP_CREDENTIALS_PATH=/home/tunk/Desktop/fp-a-project-0c82aa55ae6a.json
```

## Run Application

### Cách 1: Sử dụng script runner (Recommended)

```bash
python run_local.py
```

### Cách 2: Chạy trực tiếp với uvicorn

```bash
python -m uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
```

### Cách 3: Run từ IDE

**VS Code:**
1. Mở file `run_local.py`
2. Click vào nút "Run" hoặc nhấn F5
3. Hoặc mở Terminal trong VS Code và chạy: `python run_local.py`

**PyCharm:**
1. Right-click vào file `run_local.py`
2. Chọn "Run 'run_local'"
3. Hoặc nhấn Shift+F10

## Access API

Sau khi start thành công, API sẽ available tại:

- **Base URL**: http://localhost:8000
- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc
- **Health Check**: http://localhost:8000/health

## Test API

### 1. Health Check

```bash
curl http://localhost:8000/health
```

### 2. Load Report

```bash
curl -X POST http://localhost:8000/api/report/load \
  -H "Content-Type: application/json" \
  -d '{
    "my_rep_temp": "FK1",
    "my_z_block_plan": "PC-AC-PLA5-KRF",
    "my_z_block_forecast": "FC-AC-FOR1-KRF",
    "my_alt": "PLA5",
    "my_last_report_month": "M2504"
  }'
```

### 3. Build Report

```bash
curl -X POST http://localhost:8000/api/report/build \
  -H "Content-Type: application/json" \
  -d '{
    "my_rep_temp": "FK1",
    "my_z_block_plan": "PC-AC-PLA5-KRF",
    "my_z_block_forecast": "FC-AC-FOR1-KRF",
    "my_alt": "PLA5",
    "my_last_report_month": "M2504"
  }'
```

## Development Mode

File `.env` đã được config với `DEBUG=true`, điều này enable:
- **Auto-reload**: Code changes sẽ tự động reload server
- **Detailed logging**: Chi tiết error messages
- **Stack traces**: Full stack trace khi có lỗi

## Troubleshooting

### Issue: ModuleNotFoundError

**Solution**: 
```bash
# Đảm bảo bạn đang ở đúng directory
cd /home/tunk/coding/python/FinanceAllocation

# Reinstall dependencies
pip install -r requirements.txt
```

### Issue: Cannot connect to BigQuery

**Solution**:
```bash
# Kiểm tra credentials path
echo $GCP_CREDENTIALS_PATH

# Hoặc check trong .env file
cat .env | grep GCP_CREDENTIALS_PATH

# Verify file exists
ls -la /home/tunk/Desktop/fp-a-project-0c82aa55ae6a.json
```

### Issue: Port 8000 already in use

**Solution**:
```bash
# Find process using port 8000
lsof -i :8000

# Kill the process
kill -9 <PID>

# Or change port in .env file
API_PORT=8001
```

### Issue: Import errors

**Solution**:
```bash
# Make sure you're running from project root
cd /home/tunk/coding/python/FinanceAllocation

# Set PYTHONPATH if needed
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
```

## Hot Reload

Khi `DEBUG=true`, server sẽ tự động reload khi bạn save file. Bạn có thể:
1. Edit code trong IDE
2. Save file (Ctrl+S)
3. Server tự động restart
4. Test ngay trên Swagger UI

## IDE Configuration

### VS Code - launch.json

Tạo file `.vscode/launch.json`:

```json
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Python: FastAPI",
            "type": "python",
            "request": "launch",
            "module": "uvicorn",
            "args": [
                "api.main:app",
                "--reload",
                "--host",
                "0.0.0.0",
                "--port",
                "8000"
            ],
            "jinja": true,
            "justMyCode": true,
            "env": {
                "PYTHONPATH": "${workspaceFolder}"
            }
        }
    ]
}
```

### PyCharm - Run Configuration

1. Run → Edit Configurations
2. Add New Configuration → Python
3. Script path: `run_local.py`
4. Working directory: `/home/tunk/coding/python/FinanceAllocation`
5. Environment variables: Load from `.env`

## Project Structure

```
FinanceAllocation/
├── api/
│   └── main.py              # FastAPI app
├── calculate/
│   └── report_runner.py     # Business logic
├── db/
│   └── bigquery_connector.py
├── models/
│   └── report_models.py
├── config.py                # Config management
├── .env                     # Local environment (created)
├── run_local.py            # Local runner (created)
└── requirements.txt
```

## Next Steps

1. **Start coding**: Edit files trong IDE
2. **Test API**: Sử dụng Swagger UI tại http://localhost:8000/docs
3. **Debug**: Set breakpoints trong IDE và debug như bình thường
4. **Deploy**: Khi ready, sử dụng Docker để deploy lên server

## Tips

- Sử dụng Swagger UI để test API interactively
- Check logs trong terminal để debug
- Enable auto-save trong IDE để tận dụng hot reload
- Sử dụng Postman hoặc Thunder Client (VS Code extension) để test API

## Environment Variables Reference

| Variable | Local Value | Description |
|----------|-------------|-------------|
| `DEBUG` | `true` | Enable debug mode & auto-reload |
| `GCP_CREDENTIALS_PATH` | `/home/tunk/Desktop/...` | Path to GCP credentials |
| `API_PORT` | `8000` | API port |
| `GCP_PROJECT_ID` | `fp-a-project` | BigQuery project |

Để thay đổi config, edit file `.env` và restart server.
