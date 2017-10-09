call activate pywr
echo %1
python PywrRunner.py %1
call deactivate
