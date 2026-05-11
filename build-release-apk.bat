@echo off
REM Build APK Script for Ryan ERP Mobile App
setlocal enabledelayedexpansion

set JAVA_HOME=C:\Users\koudo\tools\jdk17
set ANDROID_HOME=C:\Users\koudo\tools\android-sdk
set PATH=%JAVA_HOME%\bin;%ANDROID_HOME%\platform-tools;%PATH%

echo.
echo ============================================================
echo       Ryan ERP - Build Signed APK for Distribution
echo ============================================================
echo.
echo JAVA_HOME: %JAVA_HOME%
echo ANDROID_HOME: %ANDROID_HOME%
echo.

cd /d "C:\Users\koudo\OneDrive\travail personnel\ERP\mobile\android"

echo [1/4] Verifying JDK...
java -version 2>&1 | findstr /C:"openjdk"
if !ERRORLEVEL! NEQ 0 (
    echo ERROR: JDK not found
    pause
    exit /b 1
)

echo.
echo [2/4] Verifying keystore...
if not exist "app\ryan-erp-release.keystore" (
    echo ERROR: Keystore not found at app\ryan-erp-release.keystore
    pause
    exit /b 1
)
echo Keystore found: app\ryan-erp-release.keystore

echo.
echo [3/4] Building Release APK...
echo Command: gradlew assembleRelease
call gradlew.bat assembleRelease
if !ERRORLEVEL! NEQ 0 (
    echo ERROR: Build failed with exit code !ERRORLEVEL!
    pause
    exit /b 1
)

echo.
echo [4/4] Verifying output...
if exist "app\build\outputs\apk\release\app-release.apk" (
    echo.
    echo ============================================================
    echo  SUCCESS! APK Generated
    echo ============================================================
    dir "app\build\outputs\apk\release\app-release.apk"
    echo.
    echo File location:
    echo   c:\Users\koudo\OneDrive\travail personnel\ERP\mobile\android\app\build\outputs\apk\release\app-release.apk
    echo.
    echo This APK is signed and ready for distribution!
    echo Ready to install on Android devices.
    echo.
) else (
    echo ERROR: APK not found at expected location
    if exist "app\build\outputs" (
        echo Contents of app\build\outputs:
        dir /s "app\build\outputs"
    )
)

pause
