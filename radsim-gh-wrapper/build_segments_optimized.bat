@echo off
setlocal enabledelayedexpansion

rem Optimized build script for large OSM datasets (like Germany)
rem Uses binary formats and streaming extraction to minimize memory usage

rem Usage: build_segments_optimized.bat path\to\input.osm.pbf [--build-dictionary]
if [%1]==[] (
  echo Usage: %~nx0 path\to\input.osm.pbf [--build-dictionary]
  echo.
  echo Options:
  echo   --build-dictionary    Build tag dictionary for better compression ^(slower^)
  echo.
  echo This optimized script uses:
  echo   - Binary memory-mapped node cache
  echo   - Compressed way tag cache with frequency dictionary
  echo   - Streaming extraction ^(no large HashSets in memory^)
  echo.
  echo Memory savings:
  echo   - Standard script: 15-30 GB heap for Germany
  echo   - Optimized script: 2-5 GB heap for Germany
  exit /b 1
)

set "osm=%~1"
set "base=%~n1"

rem Check for --build-dictionary flag
set "buildDict="
if [%2]==[--build-dictionary] set "buildDict=--build-dictionary"

rem Hardcoded locations (relative to this script folder)
set "segments=..\data\cache\%base%.rseg"
set "cache=..\data\cache"
set "outDir=..\data\output"
set "derived=%outDir%\%base%-segmented.pbf"

if not exist "..\data\cache" mkdir "..\data\cache"
if not exist "%outDir%" mkdir "%outDir%"

echo ================================================================
echo OPTIMIZED BUILD FOR LARGE DATASETS
echo ================================================================
echo.
echo Input OSM:   "%osm%"
echo Segments:    "%segments%"
echo Cache dir:   "%cache%"
echo Output PBF:  "%derived%"
if defined buildDict (
  echo Dictionary:  ENABLED ^(slower but better compression^)
) else (
  echo Dictionary:  DISABLED ^(faster, use --build-dictionary to enable^)
)
echo.
echo Expected memory usage: 2-5 GB heap
echo.

rem Step 1: Capture segments
echo ================================================================
echo STEP 1: Capture segments from GraphHopper import
echo ================================================================
call .\gradlew :wrapper:run --args="capture-segments --osm ""%osm%"" --segments ""%segments%"""
if errorlevel 1 (
  echo.
  echo ERROR: Segment capture failed
  exit /b %errorlevel%
)
echo.

rem Step 2: Extract OSM data using optimized binary caches
echo ================================================================
echo STEP 2: Extract OSM data with optimized binary caches
echo ================================================================
call .\gradlew :wrapper:run --args="extract-osm --osm ""%osm%"" --segments ""%segments%"" --out ""%cache%"" --optimized %buildDict%"
if errorlevel 1 (
  echo.
  echo ERROR: OSM extraction failed
  exit /b %errorlevel%
)
echo.

rem Step 3: Build derived PBF
echo ================================================================
echo STEP 3: Build derived PBF
echo ================================================================
call .\gradlew :wrapper:run --args="build-derived-pbf --segments ""%segments%"" --cache ""%cache%"" --out ""%derived%"""
if errorlevel 1 (
  echo.
  echo ERROR: Derived PBF build failed
  exit /b %errorlevel%
)
echo.

echo ================================================================
echo DONE!
echo ================================================================
echo.
echo Derived PBF written to: "%derived%"
echo.
echo Cache files created:
echo   - %cache%\nodes.bin       (binary node cache)
echo   - %cache%\way_tags.bin    (compressed way tag cache)
echo   - %cache%\relations.txt   (route relations)
echo.
echo To use standard (text) format instead, use build_segments.bat
echo.

endlocal
