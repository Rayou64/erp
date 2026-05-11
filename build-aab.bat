@echo off
setlocal enabledelayedexpansion

set JAVA_HOME=C:\Users\koudo\tools\jdk17
set PATH=%JAVA_HOME%\bin;%PATH%
set ANDROID_HOME=C:\Users\koudo\tools\android-sdk

cd /d "C:\Users\koudo\OneDrive\travail personnel\ERP\mobile\android"

echo ========================================
echo Step 1: Verifying JDK
echo ========================================
java -version

echo.
echo ========================================
echo Step 2: Building Release AAB
echo ========================================
call gradlew.bat --no-daemon bundleRelease

echo.
echo ========================================
echo Step 3: Checking Output
echo ========================================
if exist "app\build\outputs\bundle\release\app-release.aab" (
    echo SUCCESS: AAB generated
    dir /S "app\build\outputs\bundle\release\app-release.aab"
) else (
    echo FAILED: AAB not found
    dir /S "app\build\outputs" 2>nul || echo No outputs directory
)

pause
