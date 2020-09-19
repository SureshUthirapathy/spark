python3 -m pytest test.py --junitxml=unit.xml
FS_SCORE=`python3 parser.py`
echo "FS_SCORE:$FS_SCORE%"
