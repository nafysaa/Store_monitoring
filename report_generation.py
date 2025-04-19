import uuid
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.responses import FileResponse
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Time
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import pytz

app = FastAPI()

DATABASE_URL = "sqlite:///./store_data.db"
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

data_dir = Path("D:/Downloads/store-monitoring-data")
REPORTS_DIR = Path("D:/reports")
REPORTS_DIR.mkdir(exist_ok=True)

class StoreStatus(Base):
    __tablename__ = "store_status"
    id = Column(Integer, primary_key=True)
    store_id = Column(String)
    timestamp_utc = Column(DateTime)
    status = Column(String) 

class BusinessHours(Base):
    __tablename__ = "business_hours"
    id = Column(Integer, primary_key=True)
    store_id = Column(String)
    day_of_week = Column(Integer)
    start_time_local = Column(Time)
    end_time_local = Column(Time)

class StoreTimezone(Base):
    __tablename__ = "store_timezones"
    id = Column(Integer, primary_key=True)
    store_id = Column(String)
    timezone_str = Column(String)

class ReportStatus(Base):
    __tablename__ = "report_status"
    report_id = Column(String, primary_key=True)
    status = Column(String)
    file_path = Column(String)

Base.metadata.create_all(bind=engine)

def load_csv_to_db():
    session = SessionLocal()
    print("Clearing existing data...")
    session.query(StoreStatus).delete()
    session.query(BusinessHours).delete()
    session.query(StoreTimezone).delete()
    session.commit()
    print("Cleared tables.")

    print("Loading first 50 rows from store_status.csv...")
    df_status = pd.read_csv(data_dir / "store_status.csv")
    df_status = df_status.head(50)  # Limit to 50 rows
    df_status["timestamp_utc"] = pd.to_datetime(df_status["timestamp_utc"], utc=True)
    print("Inserting store_status.csv (50 rows)...")
    status_objects = []
    for _, row in df_status.iterrows():
        try:
            status_objects.append(StoreStatus(
                store_id=row['store_id'],
                timestamp_utc = row['timestamp_utc'],
                status=row['status']
            ))
        except Exception as e:
            print(f"Skipping row due to error: {e}")

    session.bulk_save_objects(status_objects)
    session.commit()
    print("Inserted store_status (50 rows).")

    print("Loading menu_hours.csv...")
    df_hours = pd.read_csv(data_dir / "menu_hours.csv")
    print("Inserting menu_hours.csv...")
    for _, row in df_hours.iterrows():
        session.add(BusinessHours(
            store_id=row['store_id'],
            day_of_week=row['dayOfWeek'],
            start_time_local=datetime.strptime(row["start_time_local"], "%H:%M:%S").time(),
            end_time_local=datetime.strptime(row["end_time_local"], "%H:%M:%S").time(),
        ))
    session.commit()
    print("Inserted business_hours.")

    print("Loading timezones.csv...")
    df_tz = pd.read_csv(data_dir / "timezones.csv")
    print("Inserting timezones.csv...")
    for _, row in df_tz.iterrows():
        session.add(StoreTimezone(
            store_id=row['store_id'],
            timezone_str=row['timezone_str']
        ))
    session.commit()
    print("Inserted timezones.")

    session.close()
    print("DB load complete.")

def get_timezone(store_id: str, session) -> str:
    tz = session.query(StoreTimezone).filter_by(store_id=store_id).first()
    return tz.timezone_str if tz else "America/Chicago"

def generate_report(report_id: str):
    print(f"Starting report generation for report_id: {report_id}")
    session = SessionLocal()
    try:
        statuses = pd.read_sql("SELECT store_id, timestamp_utc, status FROM store_status", engine)
        print(f"Loaded {len(statuses)} rows from store_status")
        statuses["timestamp_utc"] = pd.to_datetime(statuses["timestamp_utc"])
        if statuses["timestamp_utc"].dt.tz is None:
            statuses["timestamp_utc"] = statuses["timestamp_utc"].dt.tz_localize('UTC')
        else:
            statuses["timestamp_utc"] = statuses["timestamp_utc"].dt.tz_convert('UTC')

        current_time = statuses["timestamp_utc"].max()
        report = []

        for store_id in statuses["store_id"].unique():
            tz_str = get_timezone(store_id, session)
            tz = pytz.timezone(tz_str)
            now_local = current_time.astimezone(tz)

            durations = {
                "1h": (now_local - timedelta(hours=1), now_local),
                "1d": (now_local - timedelta(days=1), now_local),
                "1w": (now_local - timedelta(weeks=1), now_local),
            }
            store_status = statuses[statuses.store_id == store_id].copy()
            store_status.sort_values("timestamp_utc", inplace=True)
            print(f"Store {store_id} has {len(store_status)} status rows")
            for label, (start, end) in durations.items():
                df = store_status[(store_status.timestamp_utc >= start.astimezone(pytz.utc)) &
                                  (store_status.timestamp_utc <= end.astimezone(pytz.utc))]
                total_minutes = (end - start).total_seconds() / 60
                if df.empty:
                    uptime = downtime = 0
                else:
                    active_minutes = len(df[df.status == "active"]) * 60
                    inactive_minutes = len(df[df.status == "inactive"]) * 60
                    uptime = min(active_minutes, total_minutes)
                    downtime = min(inactive_minutes, total_minutes)

                durations[label] = (uptime / 60, downtime / 60) 

            report.append({
                "store_id": store_id,
                "uptime_last_hour": round(durations["1h"][0] * 60, 2),
                "uptime_last_day": round(durations["1d"][0], 2),
                "update_last_week": round(durations["1w"][0], 2),
                "downtime_last_hour": round(durations["1h"][1] * 60, 2),
                "downtime_last_day": round(durations["1d"][1], 2),
                "downtime_last_week": round(durations["1w"][1], 2),
            })

        df_report = pd.DataFrame(report)
        filepath = REPORTS_DIR / f"{report_id}.csv"
        df_report.to_csv(filepath, index=False)
        print(f"Report written to {filepath}")
        session.query(ReportStatus).filter_by(report_id=report_id).update({
        "status": "Complete",
        "file_path": str(filepath)
})
        session.commit()
        print(f"Report status updated to Complete")
    finally:
        session.close()

@app.on_event("startup")
def startup_event():
    print("Starting up...")
    try:
        load_csv_to_db()
        print("CSV loaded")
    except Exception as e:
        print(f"Startup failed: {e}")

@app.post("/trigger_report")
def trigger_report(background_tasks: BackgroundTasks):
    report_id = str(uuid.uuid4())
    session = SessionLocal()
    session.add(ReportStatus(report_id=report_id, status="Running", file_path=""))
    session.commit()
    session.close()
    background_tasks.add_task(generate_report, report_id)
    return {"report_id": report_id}

@app.get("/get_report")
def get_report(report_id: str):
    session = SessionLocal()
    report = session.query(ReportStatus).filter_by(report_id=report_id).first()
    session.close()
    if not report:
        raise HTTPException(status_code=404, detail="Report ID not found")
    if report.status == "Running":
        return {"status": "Running"}
    return FileResponse(report.file_path, media_type="text/csv", filename=f"{report_id}.csv")
