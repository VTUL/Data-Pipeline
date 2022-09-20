dp=deploymentpackage.zip
func=doom-aleph
awspre='fileb://'
pd=`pwd`

rm $dp
cd $VIRTUAL_ENV/lib/python3.10/site-packages/
zip -r9 $pd/$dp *
cd $pd
zip -gr $dp lambda_function.py libinsights_allref \
    requirements.txt enigma.py
aws lambda update-function-code --function-name $func \
    --zip-file $awspre/$pd/$dp
