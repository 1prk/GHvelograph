@echo off
setlocal enabledelayedexpansion

rem Usage: run-all.bat path\to\input.osm.pbf
if "%~1"=="" (
  echo Usage: %~nx0 path\to\input.osm.pbf
  exit /b 1
)

set "osm=%~1"
set "base=%~n1"

rem Hardcoded locations (relative to this script folder)
set "segments=..\data\cache\%base%.rseg"
set "cache=..\data\cache"
set "outDir=..\data\output"
set "derived=%outDir%\%base%-segmented.pbf"

if not exist "..\data\cache" mkdir "..\data\cache"
if not exist "%outDir%" mkdir "%outDir%"

echo Input OSM:   "%osm%"
echo Segments:    "%segments%"
echo Cache dir:   "%cache%"
echo Output PBF:  "%derived%"
echo.

rem Step 1: Capture segments
call .\gradlew :wrapper:run --args="capture-segments --osm ""%osm%"" --segments ""%segments%"""
if errorlevel 1 exit /b %errorlevel%

rem Step 2: Extract OSM data (writes nodes.txt/way_tags.txt/relations.txt into ..\data\cache)
call .\gradlew :wrapper:run --args="extract-osm --osm ""%osm%"" --segments ""%segments%"" --out ""%cache%"""
if errorlevel 1 exit /b %errorlevel%

rem Step 3: Build derived PBF
call .\gradlew :wrapper:run --args="build-derived-pbf --segments ""%segments%"" --cache ""%cache%"" --out ""%derived%"""
if errorlevel 1 exit /b %errorlevel%

echo.
echo Done.
echo Derived PBF written to: "%derived%"
endlocal
