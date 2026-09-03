# Quick Archive Recovery Script
# Run this in a Databricks cell to move all archived files back to source inbox

# Configuration
ARCHIVE_BASE = "/Volumes/pop_dev/ingestion/ingestion/archive"
SOURCE_INBOX = "/Volumes/pop_dev/ingestion/ingestion/src"

# Get all table directories
try:
    archive_dirs = dbutils.fs.ls(ARCHIVE_BASE)
    table_names = [d.name.rstrip('/') for d in archive_dirs if d.isDir()]
    print(f"Found tables: {table_names}")
except Exception as e:
    print(f"Error: {e}")
    raise

# Recovery statistics
total_files = 0
total_tables = 0

print("\n" + "=" * 80)
print("ARCHIVE RECOVERY - Moving files back to source")
print("=" * 80)

for table_name in table_names:
    table_archive = f"{ARCHIVE_BASE}/{table_name}"
    print(f"\n📂 {table_name}")

    try:
        files = dbutils.fs.ls(table_archive)
        txt_files = [f for f in files if f.name.endswith('.txt')]

        if not txt_files:
            print(f"   ⚠️  No files")
            continue

        print(f"   ✅ Found {len(txt_files)} file(s)")
        total_tables += 1

        for file_obj in txt_files:
            dest = f"{SOURCE_INBOX}/{file_obj.name}"
            dbutils.fs.mv(file_obj.path, dest, recurse=True)
            print(f"      ✓ {file_obj.name}")
            total_files += 1

    except Exception as e:
        print(f"   ❌ Error: {e}")

print("\n" + "=" * 80)
print(f"✅ COMPLETE: Moved {total_files} files from {total_tables} tables")
print("=" * 80)
