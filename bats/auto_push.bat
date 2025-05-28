@echo off

REM Change to your project directory
cd C:\Users\Administrator\AppData\Local\Programs\Python\smartone_quant

REM Add all changes
git add .

REM Use the current date and time as the commit message
git commit -m "Auto commit: %date% %time%"

REM Push to the remote repository
git push origin master