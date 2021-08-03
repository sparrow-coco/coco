@echo off
SET bin=%~dp0
SET dir=%bin:~0,-1%
set currdate=%date:~0,4%%date:~5,2%%date:~8,2%

cd "%dir%"
cd ..\..\

call ".\venv\Scripts\activate"
.\venv\Scripts\python .\main.py .\configures_dev.json crawler facebook_search > logs\facebook_search_%currdate%.log 2>&1
exit