VERSION=$(cat VERSION)
TS="$(date +%Y%m%d%H%M)"
NEW_VERSION=${VERSION}-${TS}


echo version ${NEW_VERSION}
echo '...building utils package'
pip3 install -r requirements.txt --target ./dependencies

echo '...creating libraries zip'
cd dependencies
zip -9mrv dependencies-${NEW_VERSION}.zip .

mv dependencies-${NEW_VERSION}.zip ..

echo '...removing temp dir'
cd ..
rm -rf dependencies

echo '...add utils modules to zip'
mv dependencies-${NEW_VERSION}.zip src
cd src
zip -ru9 dependencies-${NEW_VERSION}.zip main -x main/utils/__pycache__/\* \
                                              -x main/utils/jobs_etl_configs/\* \
                                              -x main/utils/queries/\* \
                                              -x main/JobRunner.py \
                                              -x main/__pycache__/\* \
                                              -x "*.zip"

rm $HOME/test/*.zip

cp dependencies-${NEW_VERSION}.zip $DESKTOP/versions_built/dependencies-${NEW_VERSION}.zip
mv dependencies-${NEW_VERSION}.zip $HOME/test/dependencies-${NEW_VERSION}.zip

# aws s3 cp dependencies-${NEW_VERSION}.zip SOME_S3_LOCATION








