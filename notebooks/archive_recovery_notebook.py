# Databricks notebook source
# /// script
# [tool.databricks.environment]
# base_environment = "databricks_ai_v5"
# environment_version = "5"
# ///

# MAGIC %md
# MAGIC # Archive Recovery Notebook
# MAGIC Move files from archive folders back to source inbox

# COMMAND ----------

dbutils.widgets.text("env", "DEV", "Environment (DEV, QA, STG, PROD)")
dbutils.widgets.text("plan_name", "non_anthem", "Plan Name")
dbutils.widgets.text("archive_base_path", "/Volumes/pop_dev/ingestion/ingestion/archive", "Archive Base Path")
dbutils.widgets.text("source_path", "/Volumes/pop_dev/ingestion/ingestion/src", "Source Inbox Path")

env = dbutils.widgets.get("env").lower().strip()
plan_name = dbutils.widgets.get("plan_name").lower().strip()
archive_base_path = dbutils.widgets.get("archive_base_path").strip()
source_path = dbutils.widgets.get("source_path").strip()

print(f"Environment: {env}")
print(f"Plan Name: {plan_name}")
print(f"Archive Base: {archive_base_path}")
print(f"Source Inbox: {source_path}")

# COMMAND ----------

# Get list of all tables/subdirectories in archive
try:
    archive_dirs = dbutils.fs.ls(archive_base_path)
    tables = [d.name.rstrip('/') for d in archive_dirs if d.isDir()]
    print(f"✅ Found {len(tables)} table directories in archive: {tables}")
except Exception as e:
    print(f"❌ Error reading archive directory: {e}")
    dbutils.notebook.exit("Failed to read archive directory")

# COMMAND ----------

# DBTITLE 1,Recover files for all tables
recovery_stats = {
    "total_files_moved": 0,
    "failed_operations": [],
    "table_summary": {}
}

print("\n" + "=" * 80)
print("ARCHIVE RECOVERY: Moving files from archive back to source")
print("=" * 80)

for table_name in tables:
    table_archive_path = f"{archive_base_path}/{table_name}"

    print(f"\n📂 Processing table: {table_name}")
    print(f"   Archive path: {table_archive_path}")

    try:
        # List files in table archive subdirectory
        files = dbutils.fs.ls(table_archive_path)
        txt_files = [f for f in files if f.name.endswith('.txt')]

        if not txt_files:
            print(f"   ⚠️  No .txt files found")
            recovery_stats["table_summary"][table_name] = {"status": "no_files", "count": 0}
            continue

        print(f"   ✅ Found {len(txt_files)} file(s)")
        moved_count = 0

        for file_obj in txt_files:
            file_name = file_obj.name
            source_file_path = f"{source_path}/{file_name}"

            try:
                print(f"      ➡️  Moving: {file_name}")
                dbutils.fs.mv(file_obj.path, source_file_path, recurse=True)
                print(f"      ✅ Moved successfully")
                moved_count += 1
                recovery_stats["total_files_moved"] += 1

            except Exception as move_err:
                print(f"      ❌ Failed to move {file_name}: {move_err}")
                recovery_stats["failed_operations"].append({
                    "table": table_name,
                    "file": file_name,
                    "error": str(move_err)
                })

        recovery_stats["table_summary"][table_name] = {
            "status": "success",
            "count": moved_count
        }

    except Exception as e:
        print(f"   ⚠️  Error accessing archive: {e}")
        recovery_stats["table_summary"][table_name] = {
            "status": "error",
            "count": 0,
            "error": str(e)
        }

# COMMAND ----------

# DBTITLE 1,Recovery Summary
print("\n" + "=" * 80)
print("RECOVERY SUMMARY")
print("=" * 80)

print(f"\n📊 Results:")
print(f"   Total tables processed: {len(recovery_stats['table_summary'])}")
print(f"   Total files moved: {recovery_stats['total_files_moved']}")
print(f"   Failed operations: {len(recovery_stats['failed_operations'])}")

print(f"\n📋 Per-table breakdown:")
for table_name, result in recovery_stats["table_summary"].items():
    status = result["status"]
    count = result["count"]
    print(f"   • {table_name}: {status} ({count} files moved)")

if recovery_stats["failed_operations"]:
    print(f"\n⚠️  Failed Operations:")
    for failure in recovery_stats["failed_operations"]:
        print(f"   • {failure['table']}/{failure['file']}: {failure['error']}")
else:
    print(f"\n✅ All files recovered successfully!")

print("\n" + "=" * 80)
print(f"✅ Recovery completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 80)
