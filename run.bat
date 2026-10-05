@echo off
title FloodGuard BD
cd /d "%~dp0"
start "FloodGuard BD" http://127.0.0.1:5081
python app.py
pause
