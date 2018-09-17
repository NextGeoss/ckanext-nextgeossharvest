#!/bin/bash
set -e

echo "This is travis-build.bash..."

echo "Installing the packages that CKAN requires..."
sudo apt-get update -qq
sudo apt-get install solr-jetty libcommons-fileupload-java

echo "Installing CKAN and its Python dependencies..."
git clone https://github.com/ckan/ckan
cd ckan
if [ $CKANVERSION == 'master' ]
then
    echo "CKAN version: master"
else
    CKAN_TAG=$(git tag | grep ^ckan-$CKANVERSION | sort --version-sort | tail -n 1)
    git checkout $CKAN_TAG
    echo "CKAN version: ${CKAN_TAG#ckan-}"
fi
python setup.py develop
sed -i -e 's/psycopg2==2.4.5/psycopg2==2.7.3.2/g' requirements.txt
pip install -r requirements.txt --allow-all-external
sed -i -e 's/pycodestyle==2.2.0/# pycodestyle==2.2.0/g' dev-requirements.txt
pip install -r dev-requirements.txt --allow-all-external
cd -

echo "Setting up Solr..."
# solr is multicore for tests on ckan master now, but it's easier to run tests
# on Travis single-core still.
# see https://github.com/ckan/ckan/issues/2972
sed -i -e 's/solr_url.*/solr_url = http:\/\/127.0.0.1:8983\/solr/' ckan/test-core.ini
printf "NO_START=0\nJETTY_HOST=127.0.0.1\nJETTY_PORT=8983\nJAVA_HOME=$JAVA_HOME" | sudo tee /etc/default/jetty
sudo cp ckan/ckan/config/solr/schema.xml /etc/solr/conf/schema.xml
sudo service jetty restart

echo "Creating the PostgreSQL user and database..."
sudo -u postgres psql -c "CREATE USER ckan_default WITH PASSWORD 'pass';"
sudo -u postgres psql -c "CREATE USER datastore_default WITH PASSWORD 'pass';"
sudo -u postgres psql -c 'CREATE DATABASE ckan_test WITH OWNER ckan_default;'
sudo -u postgres psql -c 'CREATE DATABASE datastore_test WITH OWNER ckan_default;'

echo "Initialising the database..."
cd ckan
paster db init -c test-core.ini
cd -

echo "Adding PostGIS..."
sudo apt-get install postgresql-9.2-postgis-2.3
sudo -u postgres psql -d ckan_test -f /usr/share/postgresql/9.2/contrib/postgis-2.3/postgis.sql
sudo -u postgres psql -d ckan_test -f /usr/share/postgresql/9.2/contrib/postgis-2.3/spatial_ref_sys.sql
sudo -u postgres psql -d ckan_test -c 'ALTER VIEW geometry_columns OWNER TO ckan_default;'
sudo -u postgres psql -d ckan_test -c 'ALTER TABLE spatial_ref_sys OWNER TO ckan_default;'

echo "Install other libraries required..."
sudo apt-get install python-dev libxml2-dev libxslt1-dev libgeos-c1 python-gdal gdal-bin libgdal-dev libgeos-dev
export CPLUS_INCLUDE_PATH=/usr/include/gdal
export C_INCLUDE_PATH=/usr/include/gdal
pip install GDAL==1.10.0

echo "Installing ckanext-nextgeossharvest and its requirements..."
pip install -r requirements.txt
pip install -r dev-requirements.txt
# We need the harvester as well
pip install -e git+https://github.com/NextGeoss/ckanext-harvest.git@many-jobs#egg=ckanext-harvest
pip install -r https://raw.githubusercontent.com/ckan/ckanext-harvest/master/pip-requirements.txt


python setup.py develop

paster --plugin=ckanext-harvest harvester initdb -c ckan/test-core.ini

echo "Moving test.ini into a subdir..."
mkdir subdir
mv test-core.ini subdir

echo "travis-build.bash is done."
