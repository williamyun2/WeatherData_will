def simplified_manual_processing():
    """
    Simplified manual processing function for debugging.
    Start with small tests and gradually increase scope.
    """
    import os
    import sys
    from datetime import datetime, timedelta
    import logging
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    logger = logging.getLogger(__name__)
    
    try:
        # Step 1: Basic imports and setup
        logger.info("Step 1: Testing basic imports...")
        from hrrr_historical import ensure_directories, setup_google_drive
        from helper import helper
        
        ensure_directories()
        logger.info("✓ Directories ensured")
        
        # Step 2: Test Google Drive (but make it optional)
        logger.info("Step 2: Testing Google Drive...")
        drive = setup_google_drive()
        if drive:
            logger.info("✓ Google Drive connected")
        else:
            logger.warning("⚠ Google Drive not available, will skip uploads")
        
        # Step 3: Test helper
        logger.info("Step 3: Testing helper...")
        hp = helper(logger)
        logger.info("✓ Helper initialized")
        
        # Configuration
        product = "sfc"
        fxx = 1
        state = "CONUS"
        regex = r":(?:TMP|DPT|UGRD|VGRD|TCDC|DSWRF|COLMD|GUST|CPOFP|PRATE):((2|8|10|80) m above|entire atmosphere|surface|entire atmosphere single layer)"
        
        # =================================================================
        # PROGRESSIVE TESTING - UNCOMMENT ONE AT A TIME
        # =================================================================
        
        # TEST 1: Single day (24 hours) - SAFEST TEST
        logger.info("=== TEST 1: Single Day Processing ===")
        from hrrr_historical import process_one_day
        test_date = datetime.now() - timedelta(days=2)  # Use 2 days ago for data availability
        test_date_str = test_date.strftime("%Y-%m-%d")
        logger.info(f"Testing single day: {test_date_str}")
        
        success = process_one_day(test_date_str, fxx, product, regex, state, drive, hp)
        if success:
            logger.info(f"✓ Successfully processed single day: {test_date_str}")
        else:
            logger.error(f"✗ Failed to process single day: {test_date_str}")
            return False
        
        # TEST 2: Single month (if single day worked)
        # Uncomment this block after TEST 1 succeeds
        """
        logger.info("=== TEST 2: Single Month Processing ===")
        from hrrr_historical import process_one_month
        # Use a recent month that's complete (not current month)
        test_month = datetime.now().replace(day=1) - timedelta(days=1)  # Last month
        test_month = test_month.replace(day=1)  # First day of last month
        test_month_str = test_month.strftime("%Y-%m-%d")
        logger.info(f"Testing single month: {test_month.strftime('%Y-%m')}")
        
        success = process_one_month(test_month_str, fxx, product, regex, state, drive, hp)
        if success:
            logger.info(f"✓ Successfully processed single month")
        else:
            logger.error(f"✗ Failed to process single month")
            return False
        """
        
        # TEST 3: Multiple days (if previous tests worked)
        # Uncomment this block after TEST 2 succeeds
        """
        logger.info("=== TEST 3: Multiple Days Processing ===")
        from hrrr_historical import process_date_range_with_cleanup
        
        # Process just 3 days first
        start_date = datetime.now() - timedelta(days=5)
        end_date = datetime.now() - timedelta(days=3)
        
        logger.info(f"Testing date range: {start_date.date()} to {end_date.date()}")
        successful, failed = process_date_range_with_cleanup(
            start_date, end_date, fxx, product, regex, state, drive, hp, mode="day"
        )
        logger.info(f"Result: {successful} successful, {failed} failed")
        """
        
        # TEST 4: Multiple months (only after all previous tests work)
        # Uncomment this block after TEST 3 succeeds
        """
        logger.info("=== TEST 4: Multiple Months Processing ===")
        from hrrr_historical import process_date_range_with_cleanup
        
        # Process just 2 months first, not 6
        start_date = datetime(2025, 5, 1)  # May 2025
        end_date = datetime(2025, 6, 1)    # June 2025
        
        logger.info(f"Testing month range: {start_date.date()} to {end_date.date()}")
        successful, failed = process_date_range_with_cleanup(
            start_date, end_date, fxx, product, regex, state, drive, hp, mode="month"
        )
        logger.info(f"Result: {successful} successful, {failed} failed")
        """
        
        logger.info("✓ Simplified manual processing completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"✗ Error in simplified manual processing: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

def debug_specific_failure():
    """
    Debug specific failure points that might occur
    """
    import logging
    import os
    from datetime import datetime, timedelta
    
    logger = logging.getLogger(__name__)
    logger.info("=== DEBUGGING SPECIFIC FAILURE POINTS ===")
    
    # Check 1: Memory usage estimation
    logger.info("Memory estimation for different processing modes:")
    logger.info("- Single day (24 hours): ~1-2 GB memory usage")
    logger.info("- Single month (~720 hours): ~30-60 GB memory usage")
    logger.info("- 6 months (~4320 hours): ~180-360 GB memory usage")
    
    # Check 2: Disk space requirements
    logger.info("Disk space estimation:")
    logger.info("- Each hour of CONUS data: ~50-100 MB")
    logger.info("- Single day: ~1.2-2.4 GB")
    logger.info("- Single month: ~36-72 GB")
    logger.info("- 6 months: ~216-432 GB")
    
    # Check 3: Network bandwidth
    logger.info("Network considerations:")
    logger.info("- HRRR data servers may throttle/timeout on large requests")
    logger.info("- 6 months of data may exceed server limits")
    
    # Check 4: Processing time estimation
    logger.info("Time estimation:")
    logger.info("- Single day: 5-15 minutes")
    logger.info("- Single month: 2-6 hours") 
    logger.info("- 6 months: 12-36 hours")
    
    # Check available disk space
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        data_dir = os.path.join(base_dir, "data")
        
        if os.path.exists(data_dir):
            stat = os.statvfs(data_dir)
            free_space_gb = (stat.f_bavail * stat.f_frsize) / (1024**3)
            logger.info(f"Available disk space: {free_space_gb:.1f} GB")
            
            if free_space_gb < 500:
                logger.warning("⚠ Less than 500 GB available - may not be sufficient for 6 months")
        
    except Exception as e:
        logger.error(f"Could not check disk space: {e}")

# To use this for debugging, add this to your hrrr_historical.py:
#
# def manual_processing():
#     """Call the simplified version for debugging"""
#     return simplified_manual_processing()

if __name__ == "__main__":
    simplified_manual_processing()