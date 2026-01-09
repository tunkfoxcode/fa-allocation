# In-Memory Job Queue - Hướng dẫn đơn giản

## Tổng quan

Hệ thống sử dụng **in-memory queue** đơn giản, không cần Redis hay bất kỳ service ngoài nào.

### Ưu điểm
✅ **Không cần cài đặt thêm**: Chỉ cần Python
✅ **Đơn giản**: Không cần quản lý Redis service
✅ **Tự động**: Background worker tự động start khi API start
✅ **Nhanh**: Batch queries tối ưu, giảm 180x số lượng queries

### Kiến trúc

```
FastAPI Server (1 process)
    ↓
├─ API Endpoints (non-blocking)
└─ Background Worker (asyncio)
    ↓
    In-Memory Queue
    ↓
    Process jobs one-by-one
```

## Setup & Run

### Local Development

**Chỉ cần 1 terminal:**

```bash
# Install dependencies
pip install -r requirements.txt

# Run API (worker tự động start)
python run_local.py
```

Đó là tất cả! Worker sẽ tự động chạy trong background.

### Docker

```bash
# Build và run (1 container duy nhất)
docker-compose up -d

# View logs
docker-compose logs -f

# Stop
docker-compose down
```

## API Usage

### 1. Build Report (Non-blocking)

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

**Response (instant):**
```json
{
  "status": "queued",
  "job_id": "abc-123-def",
  "message": "Report build job enqueued successfully"
}
```

### 2. Check Job Status

```bash
curl http://localhost:8000/api/job/abc-123-def
```

**Response:**
```json
{
  "job_id": "abc-123-def",
  "status": "running",
  "progress": "Building report...",
  "created_at": "2026-01-09T10:00:00",
  "started_at": "2026-01-09T10:00:02"
}
```

**Status values:**
- `queued`: Đang chờ
- `running`: Đang xử lý
- `completed`: Hoàn thành
- `failed`: Thất bại

### 3. Queue Info

```bash
curl http://localhost:8000/api/queue/info
```

**Response:**
```json
{
  "queue_name": "in_memory",
  "queued_jobs": 2,
  "running_jobs": 1,
  "completed_jobs": 10,
  "failed_jobs": 0,
  "total_jobs": 13
}
```

### 4. Load Report (Unchanged)

```bash
curl -X POST http://localhost:8000/api/report/load \
  -H "Content-Type: application/json" \
  -d '{...}'
```

## Performance

### Optimizations Implemented

1. **Batch BigQuery Queries**
   - Before: 1,800 queries
   - After: ~10 queries
   - **180x faster**

2. **In-Memory Processing**
   - Fetch data once
   - Filter in RAM
   - No repeated queries

3. **Bulk Insert**
   - Before: 1,200 individual inserts
   - After: 3 batch inserts (500 rows each)
   - **400x faster**

4. **Non-blocking API**
   - API returns immediately
   - Processing happens in background
   - Server can handle other requests

### Performance Comparison

| Metric | Before | After |
|--------|--------|-------|
| API Response | 5-10 minutes | < 1 second |
| BigQuery Queries | ~1,800 | ~10 |
| Server Blocking | Yes | No |
| Concurrent Requests | 1 | Unlimited |

## How It Works

### Background Worker

Worker tự động start khi API start và xử lý jobs tuần tự:

```python
# In api/main.py
@app.on_event("startup")
async def startup_event():
    job_queue = get_job_queue()
    await job_queue.start_worker()  # Auto-start worker
```

### Job Processing

1. Client gọi `/api/report/build`
2. API tạo job và add vào queue
3. API trả về `job_id` ngay lập tức
4. Background worker lấy job từ queue
5. Worker xử lý job (build report)
6. Client poll `/api/job/{job_id}` để check status

### Memory Management

Jobs được lưu trong memory với các trạng thái:
- `queued`: Chờ xử lý
- `running`: Đang xử lý
- `completed`: Đã xong (giữ result trong 1 giờ)
- `failed`: Lỗi (giữ error info)

## Workflow Example

```bash
# Step 1: Submit job
JOB_ID=$(curl -X POST http://localhost:8000/api/report/build \
  -H "Content-Type: application/json" \
  -d '{...}' | jq -r '.job_id')

echo "Job ID: $JOB_ID"

# Step 2: Poll status (every 5 seconds)
while true; do
  STATUS=$(curl -s http://localhost:8000/api/job/$JOB_ID | jq -r '.status')
  echo "Status: $STATUS"
  
  if [ "$STATUS" = "completed" ]; then
    echo "Job completed!"
    break
  elif [ "$STATUS" = "failed" ]; then
    echo "Job failed!"
    break
  fi
  
  sleep 5
done

# Step 3: Load report data
curl -X POST http://localhost:8000/api/report/load \
  -H "Content-Type: application/json" \
  -d '{...}'
```

## Monitoring

### Check API Health

```bash
curl http://localhost:8000/health
```

### Check Queue Status

```bash
curl http://localhost:8000/api/queue/info
```

### View Logs

```bash
# Local
# Check terminal where you ran python run_local.py

# Docker
docker-compose logs -f finance-allocation-api
```

## Troubleshooting

### Issue: Jobs không được xử lý

**Check:**
```bash
# Verify worker is running
curl http://localhost:8000/api/queue/info

# Should show running_jobs or queued_jobs
```

**Solution:**
```bash
# Restart API (worker auto-restarts)
# Ctrl+C then:
python run_local.py
```

### Issue: Job bị stuck

**Check job status:**
```bash
curl http://localhost:8000/api/job/{job_id}
```

**If stuck in "running":**
- Worker có thể đang xử lý job lớn
- Check logs để xem progress
- Đợi thêm hoặc restart API

### Issue: Memory usage cao

**Cause:** Nhiều completed jobs trong memory

**Solution:** Restart API để clear memory
```bash
# Docker
docker-compose restart finance-allocation-api

# Local
# Ctrl+C và run lại
python run_local.py
```

## Limitations

1. **Single Worker**: Xử lý 1 job tại 1 thời điểm
   - Pros: Tránh overload BigQuery
   - Cons: Jobs phải chờ nhau

2. **In-Memory Only**: Jobs mất khi restart
   - Pros: Đơn giản, không cần database
   - Cons: Restart = mất queue

3. **No Persistence**: Không lưu job history lâu dài
   - Pros: Không cần storage
   - Cons: Không có audit trail

## Best Practices

1. **Poll Interval**: Check status mỗi 5-10 giây
2. **Timeout**: Set timeout 30 phút cho client
3. **Queue Check**: Check queue info trước khi submit nhiều jobs
4. **Error Handling**: Always check job status sau khi submit
5. **Load API**: Dùng `/api/report/load` cho end-users (tự động build nếu cần)

## Comparison: In-Memory vs Redis

| Feature | In-Memory | Redis |
|---------|-----------|-------|
| Setup | ✅ Không cần | ❌ Cần install Redis |
| Deployment | ✅ 1 container | ❌ 2+ containers |
| Persistence | ❌ Mất khi restart | ✅ Persistent |
| Scaling | ❌ Single worker | ✅ Multiple workers |
| Complexity | ✅ Đơn giản | ❌ Phức tạp hơn |
| Performance | ✅ Nhanh (in-process) | ✅ Nhanh |

**Kết luận**: In-memory queue phù hợp cho:
- Development
- Small-medium workloads
- Simple deployment
- Không cần persistence

## Next Steps

Nếu cần scale lên:
1. Thêm Redis (code đã có sẵn trong `jobs/queue_manager.py`)
2. Deploy multiple workers
3. Add job persistence
4. Add retry logic

Nhưng với use case hiện tại, in-memory queue là đủ! 🚀
