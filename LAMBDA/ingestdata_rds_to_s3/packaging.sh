[ -e my-deployment-package.zip ] && rm my-deployment-package.zip
[ -d package ] && rm -r package
pip install --target ./package pymysql pandas
cd package
zip -r ../my-deployment-package.zip .
cd ..
zip -g my-deployment-package.zip lambda_function.py
aws s3 cp my-deployment-package.zip s3://jrbutbucket/my-deployment-package.zip