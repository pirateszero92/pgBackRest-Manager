# pgBackRest Manager (PostgreSQL 16 PITR + MinIO S3)

ระบบจัดการสำรองข้อมูล (Backup) และกู้คืนข้อมูลย้อนหลังแบบระบุเวลา (Point-in-Time Recovery: PITR) สำหรับ PostgreSQL 16 โดยเก็บ Backup และ WAL Archive แบบ Near-Realtime ลง MinIO S3 พร้อม Web UI สำหรับควบคุมและติดตามสถานะจากศูนย์กลาง

---

## 1. สาระสำคัญและสถาปัตยกรรมระบบ

- **Web UI (`pgBackRest Manager`):** `http://10.1.1.211:8088` (รันด้วย Docker Compose บน `10.1.1.211` ที่ `~/pgbackrest-manager`)
- **Backup Storage (MinIO S3):** `http://10.1.1.211:9000` (Bucket: `pg-backups`, Path: `/pgbackrest`)
- **ความถี่ในการเก็บข้อมูล (Near-Realtime PITR):**
  - **WAL Archive:** PostgreSQL ส่ง WAL ขึ้น MinIO อัตโนมัติทันทีที่ไฟล์ WAL เต็ม หรือทุกๆ **60 วินาที** (`archive_timeout = 60`)
  - **Retention Policy:** เก็บ `Full Backup = 2 ชุด` และ `Diff Backup = 7 ชุด` (ย้อนหลังได้ประมาณ 2 สัปดาห์เมื่อรัน Full สัปดาห์ละ 1 ครั้ง + Diff วันละ 1 ครั้ง)
- **ระบบป้องกัน Production:** เครื่อง `PROD` ทุกเครื่องถูกตั้งค่า `allow_restore: false` เพื่อล็อกไม่ให้กด Restore ทับ Production จากหน้าเว็บโดยตรง (สามารถ Restore ข้ามเครื่องไปทดสอบบน `UAT` ได้)

### รายการเครื่อง Database ในระบบ
| ID | Server Name | IP | Stanza | Env | Restore บนหน้า UI | สถานะ |
|---|---|---|---|---|---|---|
| `uat-test` | `test-backup (UAT)` | `10.1.1.203` | `uat-test` | `UAT` | เปิด (`true`) | ติดตั้งและทดสอบ Full / Diff / PITR Restore สำเร็จแล้ว |
| `interface-prod` | `db-interface-production` | `10.1.1.165` | `interface-prod` | `PROD` | ล็อก (`false`) | ลงทะเบียนใน UI แล้ว (รอติดตั้งช่วง Maintenance) |
| `wms-prod` | `db-wms-production` | `10.1.1.9` | `wms-prod` | `PROD` | ล็อก (`false`) | ลงทะเบียนใน UI แล้ว (รอติดตั้งช่วง Maintenance) |
| `tms-prod` | `db-tms-production` | `10.1.1.24` | `tms-prod` | `PROD` | ล็อก (`false`) | ลงทะเบียนใน UI แล้ว (รอติดตั้งช่วง Maintenance) |

### โครงสร้างไฟล์โปรเจกต์
```text
pgbackrest-manager/
├── app.py                 # FastAPI Backend (Auth, SSH/Paramiko, Stanza Status, Job Runner, PITR, Logs)
├── static/
│   └── index.html         # Web UI (Tailwind CSS + Vanilla JS)
├── data/
│   ├── servers.json       # ข้อมูลเครื่อง DB ที่ลงทะเบียนไว้ (mount เข้า /app/data)
│   └── logs.json          # ประวัติ Activity & Job Logs ถาวร (เก็บล่าสุด 500 รายการ)
├── Dockerfile
├── docker-compose.yml     # รันพอร์ต 8088, mount ./data และ MinIO data volume
└── README.md
```

---

## 2. Step-by-Step: การติดตั้งและตั้งค่า pgBackRest บนเครื่อง DB (Ubuntu 22.04 + PostgreSQL 16)

> **หมายเหตุสำคัญ:** แพ็กเกจ `pgbackrest` เริ่มต้นของ Ubuntu 22.04 เป็นเวอร์ชัน `2.37` ซึ่ง **ไม่รองรับ PostgreSQL 16** ต้องติดตั้งจาก **PGDG Repository** (ได้เวอร์ชัน `2.60+`)

### Step 1: เพิ่ม PGDG Repository และติดตั้ง `pgbackrest`
```bash
sudo apt-get update
sudo apt-get install -y curl ca-certificates gnupg lsb-release
sudo install -d /usr/share/postgresql-common/pgdg
sudo curl -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc --fail \
  https://www.postgresql.org/media/keys/ACCC4CF8.asc

echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] \
https://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" | \
  sudo tee /etc/apt/sources.list.d/pgdg.list

sudo apt-get update
sudo apt-get install -y pgbackrest
pgbackrest version   # ตรวจสอบว่าได้ >= 2.60
```

### Step 2: สร้างไฟล์ Configuration `/etc/pgbackrest.conf`
เปลี่ยน `<STANZA_NAME>` เป็นชื่อ stanza ของเครื่องนั้น (เช่น `interface-prod`, `wms-prod`, `tms-prod`)
```bash
sudo tee /etc/pgbackrest.conf > /dev/null <<'EOF'
[global]
repo1-type=s3
repo1-path=/pgbackrest
repo1-s3-bucket=pg-backups
repo1-s3-endpoint=http://10.1.1.211:9000
repo1-s3-region=us-east-1
repo1-s3-uri-style=path
repo1-s3-key=<MINIO_ACCESS_KEY>
repo1-s3-key-secret=<MINIO_SECRET_KEY>
repo1-storage-verify-tls=n
repo1-retention-full=2
repo1-retention-diff=7
compress-type=zst
process-max=2
start-fast=y

[<STANZA_NAME>]
pg1-path=/var/lib/postgresql/16/main
EOF

sudo chmod 640 /etc/pgbackrest.conf
sudo chown root:postgres /etc/pgbackrest.conf
```

### Step 3: เปิดใช้งาน WAL Archiving ใน PostgreSQL 16
สร้างไฟล์ `/etc/postgresql/16/main/conf.d/99-pitr.conf` (เปลี่ยน `<STANZA_NAME>` ให้ตรงกับ Step 2):
```bash
sudo tee /etc/postgresql/16/main/conf.d/99-pitr.conf > /dev/null <<'EOF'
wal_level = replica
archive_mode = on
archive_command = 'pgbackrest --stanza=<STANZA_NAME> archive-push %p'
archive_timeout = 60
EOF
```
จากนั้น **Restart PostgreSQL 1 ครั้ง** เพื่อให้ `archive_mode = on` มีผล (ต้องทำในช่วง Maintenance):
```bash
sudo systemctl restart postgresql@16-main
sudo -u postgres psql -c "SHOW wal_level; SHOW archive_mode; SHOW archive_command;"
```

### Step 4: สร้าง Stanza และตรวจสอบความพร้อม
```bash
sudo -u postgres pgbackrest --stanza=<STANZA_NAME> stanza-create
sudo -u postgres pgbackrest --stanza=<STANZA_NAME> check
```
หากสำเร็จจะขึ้น `INFO: check command end: completed successfully`

### Step 5: รัน Full Backup รอบแรก และตั้งค่า Cron อัตโนมัติ
```bash
# รัน Full Backup รอบแรก
sudo -u postgres pgbackrest --stanza=<STANZA_NAME> --type=full backup

# ตรวจสอบรายการ Backup
sudo -u postgres pgbackrest --stanza=<STANZA_NAME> info
```
ตั้งค่า Cron ของ user `postgres` (`sudo crontab -u postgres -e`):
```cron
# Full Backup ทุกวันอาทิตย์ เวลา 02:00 น.
0 2 * * 0 pgbackrest --stanza=<STANZA_NAME> --type=full backup

# Diff Backup วันจันทร์-เสาร์ เวลา 02:00 น.
0 2 * * 1-6 pgbackrest --stanza=<STANZA_NAME> --type=diff backup
```

### กรณีฉุกเฉิน: วิธี Restore เมื่อ OS พัง (ติดตั้ง OS + PostgreSQL 16 ใหม่)
1. ติดตั้ง Ubuntu + PostgreSQL 16 และทำ **Step 1** + **Step 2** (ใช้ `<STANZA_NAME>` เดิม)
2. หยุด PostgreSQL และสั่ง Restore จาก MinIO:
   ```bash
   sudo systemctl stop postgresql@16-main
   sudo -u postgres pgbackrest --stanza=<STANZA_NAME> --delta --type=default --target-timeline=latest --target-action=promote restore
   sudo systemctl start postgresql@16-main
   ```

---

## 3. คู่มือการใช้งาน pgBackRest Manager (แต่ละหน้า / แต่ละปุ่ม)

เข้าใช้งานได้ที่ `http://10.1.1.211:8088` (กำหนด `APP_USER` / `APP_PASS` ผ่าน `.env`)

### แถบด้านบน (Top Header) และแถบซ้าย (Sidebar)
- **Auto-Refresh (`Off / 15s / 30s / 1m`):** ตั้งเวลารีเฟรชสถานะ Stanza, WAL Archive Lag และ MinIO Free Space อัตโนมัติ
- **ปุ่ม `Refresh`:** ดึงข้อมูลล่าสุดจากทุกเครื่องทันที
- **ปุ่ม `Logout`:** ออกจากระบบ (ล้าง session token)
- **กล่อง `MinIO Storage` (ซ้ายล่าง):** แสดงสถานะ Disk ที่ใช้เก็บ Backup และพื้นที่คงเหลือบน `10.1.1.211`

---

### หน้า 1: 📊 `Stanzas & Backups`
หน้าหลักสำหรับดูภาพรวมและสั่ง Backup ด้วยตนเอง
- **KPI Cards ด้านบน:**
  - `Configured Stanzas`: จำนวนเครื่อง DB ที่ลงทะเบียนไว้
  - `Healthy (ok)`: จำนวนเครื่องที่ `pgBackRest` พร้อมใช้งานและไม่พบ error
  - `Total Backup Sets`: จำนวนชุด Backup ทั้งหมดที่มีอยู่ในระบบ (Full + Diff)
  - `MinIO Free Space`: พื้นที่ว่างของ Storage
- **ตาราง Stanzas:**
  - `Status`: `ok` (พร้อมใช้งาน) หรือ `not configured` (ยังไม่ได้ติดตั้ง/สร้าง stanza)
  - `Latest Backup`: ชื่อชุด Backup ล่าสุด (`F` = Full, `D` = Diff) พร้อมขนาดใน Repo
  - `WAL Archive (Near-Realtime)`: แสดงสถานะ `● Near-Realtime` หาก WAL ล่าสุดส่งขึ้น MinIO ภายใน 120 วินาที หรือแสดงเวลาที่ส่งล่าสุด + จำนวนที่ failed
  - `Recoverable Window`: ช่วงเวลาเก่าสุดจนถึงล่าสุดที่สามารถย้อนเวลา Restore (PITR) ได้
- **ปุ่ม Actions ในแต่ละแถว:**
  - **`Check`:** ทดสอบการเชื่อมต่อ MinIO และตรวจดูว่า WAL Archive ทำงานปกติหรือไม่ (ไม่สร้างไฟล์ backup ใหม่)
  - **`Diff`:** สั่งทำ **Differential Backup** ทันที (เก็บเฉพาะส่วนที่เปลี่ยนจาก Full ล่าสุด เหมาะกับกดก่อน Deploy/แก้ข้อมูล)
  - **`Full`:** สั่งทำ **Full Backup** ทันที (สำรองข้อมูลทั้งหมดของ cluster)
- **`Live Job Console` ด้านล่าง:** แสดง log การทำงานของคำสั่ง `Check / Diff / Full` แบบ real-time

---

### หน้า 2: ⏪ `PITR Restore Wizard`
หน้าสำหรับกู้คืนข้อมูลย้อนหลังแบบระบุเวลา
- **`Source Stanza (Backup Source)`:** เลือก Backup ต้นทางที่ต้องการนำมากู้คืน (เลือกได้ทั้ง UAT และ PROD)
- **`Target Server`:** เลือกเครื่องปลายทางที่จะถูกกู้คืนข้อมูลทับลงไป (**เครื่อง `PROD` ที่ล็อกไว้จะไม่ปรากฏให้เลือก** เพื่อป้องกันความผิดพลาด)
- **`Restore Mode`:**
  - `Point-in-Time (Specify Timestamp)`: กู้คืนข้อมูลย้อนหลัง ณ วินาทีที่กำหนด
  - `Latest Recoverable Point (Default)`: กู้คืนไปยังจุดล่าสุดเท่าที่ WAL มีอยู่
- **`Target Timestamp` + ปุ่ม Quick-pick:**
  - กรอกเวลาในรูปแบบ `YYYY-MM-DD HH:MM:SS+07`
  - ปุ่มช่วยเลือกเวลาด่วน: **`Latest Archive`** (เวลา WAL ล่าสุด), **`1 min ago`**, **`5 min ago`**, **`15 min ago`**
- **`Confirm Stanza`:** พิมพ์ชื่อ Stanza ต้นทางให้ตรงกันเพื่อยืนยันก่อนรันจริง
- **ปุ่ม `Execute PITR Restore`:** ระบบจะหยุด PostgreSQL ปลายทาง → รัน `pgbackrest --delta ... restore` → สั่ง promote และสตาร์ท PostgreSQL กลับมาให้อัตโนมัติ พร้อมแสดงผลใน `Restore Console`

---

### หน้า 3: 🖥️ `DB Servers`
หน้าจัดการรายชื่อเครื่อง Database ที่เชื่อมต่อกับ Manager
- **ฟอร์ม `Add / Update Server`:**
  - กรอก `Server ID`, `Display Name`, `Host / IP`, `SSH User`, `SSH / Sudo Pass`, `Stanza Name`, `Environment (UAT/PROD)`, `PG Systemd Service` (เช่น `postgresql@16-main`)
  - **`Allow Direct Restore`:** ติ๊กเลือกเฉพาะเครื่อง **UAT/Test** เท่านั้น (**ห้ามติ๊ก** สำหรับเครื่อง Production)
  - ปุ่ม **`Save Server`**: บันทึกหรืออัปเดตข้อมูลเครื่องลง `data/servers.json`
- **รายการ `Registered Servers`:**
  - ปุ่ม **`Test SSH`**: ทดสอบ SSH เข้าเครื่อง, ตรวจ `pgbackrest version` และเช็กสถานะ service PostgreSQL
  - ปุ่ม **`Remove`**: ลบเครื่องออกจากหน้า Manager (ไม่กระทบข้อมูลบนเครื่อง DB หรือ MinIO)

---

### หน้า 4: 🗓️ `Schedules & Cron`
หน้าตรวจสอบตารางเวลา Backup อัตโนมัติบนเครื่อง DB
- แสดงผล `crontab -u postgres -l` จากทุกเครื่องที่ลงทะเบียนไว้
- ปุ่ม **`Reload Cron`**: ดึงรายการ Cron ล่าสุดจากทุกเครื่องมาแสดงใหม่

---

### หน้า 5: 📜 `Activity & Logs`
หน้าดูประวัติการใช้งานและ Log การทำงานย้อนหลัง
- **ส่วนบน (`UI Activity & Job Logs`):**
  - เก็บประวัติการ `AUTH` (Login/Logout), `SERVER` (Add/Delete/Test SSH), `JOB` (`Check/Diff/Full`), และ `RESTORE` ลงไฟล์ `/app/data/logs.json`
  - มีตัวกรองตาม **Category** และ **Server** พร้อมปุ่ม **`Refresh Logs`**
  - ปุ่ม **`View`**: เปิดดูข้อความ output เต็มๆ ของงานนั้นๆ ในกล่องด้านล่าง
- **ส่วนล่าง (`Remote Server pgBackRest Logs`):**
  - เลือกเครื่อง DB และจำนวนบรรทัด (`100 / 200 / 400`) แล้วกด **`Fetch Server Log`** เพื่อดึงไฟล์ `/var/log/pgbackrest/*.log` จากเครื่อง DB มาตรวจสอบผลการทำงานของ Cron หรือ WAL Archiving ได้โดยไม่ต้อง SSH เข้าเครื่องเอง
