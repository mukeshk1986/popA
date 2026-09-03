# Databricks notebook source
# Archive Recovery Script - Move files from archive back to source inbox
# This script recovers archived files by moving them from archive/{table_name}/ back to src/

import sys
sys.dont_write_bytecode = True

from datetime import datetime
from src.spark.helpers.config_util import get_config_yaml
from src.spark.helpers.logger_util import get_logger

logger = get_logger()

# COMMAND ----------

def recover_archived_files(dbutils, archive_base_dir: str, source_dir: str, tables_config: dict, logger):
    """
    Moves files from archive subdirectories back to source inbox.

    Args:
        dbutils: Databricks utilities
        archive_base_dir: Base archive directory path
        source_dir: Source inbox directory path
        tables_config: Dictionary of table names and file names
        logger: Logger instance

    Returns:
        dict: Summary of recovery operations
    """
    recovery_summary = {
        "total_tables": 0,
        "tables_with_files": 0,
        "total_files_moved": 0,
        "failed_tables": [],
        "details": {}
    }

    logger.info("=" * 80)
    logger.info("ARCHIVE RECOVERY: Moving files from archive back to source inbox")
    logger.info("=" * 80)
    logger.info(f"Archive base directory: {archive_base_dir}")
    logger.info(f"Source inbox directory: {source_dir}")

    table_names = list(tables_config.keys())
    recovery_summary["total_tables"] = len(table_names)

    for table_name in table_names:
        logger.info(f"\n🔍 Processing table: {table_name}")
        table_archive_dir = f"{archive_base_dir}/{table_name}"

        try:
            # Check if archive directory exists
            archive_files = dbutils.fs.ls(table_archive_dir)

            if not archive_files:
                logger.warning(f"⚠️  No files found in archive for {table_name}")
                recovery_summary["details"][table_name] = {
                    "status": "no_files",
                    "file_count": 0
                }
                continue

            logger.info(f"✅ Found {len(archive_files)} file(s) in archive for {table_name}")
            recovery_summary["tables_with_files"] += 1

            moved_count = 0
            for file_obj in archive_files:
                if file_obj.name.endswith('.txt'):
                    try:
                        source_path = f"{source_dir}/{file_obj.name}"
                        logger.info(f"   ➡️  Moving: {file_obj.name}")
                        dbutils.fs.mv(file_obj.path, source_path, recurse=True)
                        logger.info(f"   ✅ Moved to: {source_path}")
                        moved_count += 1
                        recovery_summary["total_files_moved"] += 1
                    except Exception as move_err:
                        logger.error(f"   ❌ Failed to move {file_obj.name}: {move_err}")
                        recovery_summary["failed_tables"].append({
                            "table": table_name,
                            "file": file_obj.name,
                            "error": str(move_err)
                        })

            recovery_summary["details"][table_name] = {
                "status": "success",
                "file_count": moved_count
            }

        except Exception as e:
            logger.warning(f"⚠️  Archive directory may not exist for {table_name}: {e}")
            recovery_summary["details"][table_name] = {
                "status": "not_found",
                "file_count": 0,
                "error": str(e)
            }

    logger.info("\n" + "=" * 80)
    logger.info("ARCHIVE RECOVERY SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Total tables processed: {recovery_summary['total_tables']}")
    logger.info(f"Tables with archived files: {recovery_summary['tables_with_files']}")
    logger.info(f"Total files moved: {recovery_summary['total_files_moved']}")

    if recovery_summary["failed_tables"]:
        logger.error(f"Failed operations: {len(recovery_summary['failed_tables'])}")
        for failure in recovery_summary["failed_tables"]:
            logger.error(f"  - {failure['table']}/{failure['file']}: {failure['error']}")
    else:
        logger.info("✅ All files recovered successfully")

    logger.info("=" * 80)

    return recovery_summary


# COMMAND ----------

# DBTITLE 1,Main Recovery Script
if __name__ == "__main__":
    # Load configuration
    logger.info("Loading configuration...")
    data_loader_config = get_config_yaml("../../../config/constants/data_loader_config.yaml")
    env_config = get_config_yaml("../../../config/environments/DEV/values.yaml")

    # Get directory paths
    catalog = env_config["catalog"]
    v_plan_name = "non_anthem"  # Default plan name
    v_path_plan_name = "non_anthem"

    archive_dir = data_loader_config["archive_dir"].format(env="dev", plan_name=v_plan_name)
    src_file_dir = data_loader_config["src_file_dir"].format(env="dev", plan_name=v_path_plan_name)
    file_names = data_loader_config["file_names"]

    logger.info(f"Archive directory: {archive_dir}")
    logger.info(f"Source directory: {src_file_dir}")
    logger.info(f"Tables to recover: {list(file_names.keys())}")

    # Execute recovery
    summary = recover_archived_files(dbutils, archive_dir, src_file_dir, file_names, logger)

    # Display summary
    logger.info(f"\n📊 Recovery Summary:")
    logger.info(f"   Processed: {summary['total_tables']} tables")
    logger.info(f"   Recovered: {summary['total_files_moved']} files")
    logger.info(f"   Failed: {len(summary['failed_tables'])} operations")
