SET bin=%~dp0
SET dir=%bin:~0,-1%
SET venvfile = ".\venv\Scripts\activate"
set currdate=%date:~0,4%%date:~5,2%%date:~8,2%

cd "%dir%"
cd ..\..\

call ".\venv\Scripts\activate"
.\venv\Scripts\python .\main.py .\configures_dev.json crawler twitter_search > logs\twitter_search_%currdate%.log 2>&1
exit