# VAMPS-Upload-Notebook
## Prerequisite: Install VAMPS-node.js as follows:
##### Install Anaconda: (python3 and Jupyter) https://www.continuum.io/downloads
##### Install Mysql: https://dev.mysql.com/downloads/installer/
- Change root password
- Point PATH to mysql/bin
- Create a new database: (ie: vamps-db)
- Create a user/pass for new database
##### Install Node-js https://nodejs.org/en/download/
##### From github clone vamps-node.js
- In vamps-node.js direcory run: npm install
- Decompress db_schema_w_test_data.sql.gz and install schema in newly created mysql database
- as per README.md create config/db-connect.js, config/config.js, ~/.my.cnf_node files
- Install python module pymysql: pip install pymysql
- Install python module cogent: pip install cogent
- create files using command INITIALIZE_ALL_FILES.js in public/scripts/maintenance_scripts
- May need to rename newly created files in public/json
- Start node.js VAMPS server: node bin/www
## Test: there are three small test fasta files for testing
##### Open the jupyer notebook: 'jupyter notebook'


