dp=deploymentpackage.zip

rm $dp
cd $VIRTUAL_ENV/lib/python3.8/site-packages/
pwd
zip -r9 $OLDPWD/$dp *
cd $OLDPWD
zip -gr $dp lambda_function.py libinsights_allref \
    requirements.txt enigma.py
