

// dumb-conn.sh

#!/bin/bash

DAY=$1

ROOT_PATH=/build/scripts/es-dump

INDEX="radius-${DAY}"

/bin/es2csv -i "${INDEX}" -u 172.27.11.156  -r -q '{"query": {"match_all": {}}}' -o "${ROOT_PATH}/${INDEX}.csv"

mkdir /logs-data/share/radius-conn-log-realtime-parse/conn-log-$(date +%m)

mv "${ROOT_PATH}/${INDEX}.csv" /logs-data/share/radius-conn-log-realtime-parse/conn-log-$(date +%m)


// crontab

#!/bin/bash

#DAY=`date +%Y-%m-%d`
#HOUR=`date +%H`

DAY=`date -d '1 hour ago' +%Y-%m-%d`

/build/scripts/es-dump/dumb-conn.sh $DAY


///////////////////////////////////////////////////////////////////////////////////////////////////////////
// dumb-inf.sh

#!/bin/bash

DAY=$1

ROOT_PATH=/build/scripts/es-dump

INDEX="inf-${DAY}"

/bin/es2csv -i "${INDEX}" -u 172.27.11.156  -r -q '{"query": {"match_all": {}}}' -o "${ROOT_PATH}/${INDEX}.csv"

mkdir /logs-data/share/radius-conn-log-realtime-parse/conn-log-$(date +%m)

mv "${ROOT_PATH}/${INDEX}.csv" /logs-data/share/inf-log-realtime-parse/inf-log-$(date +%m)


// crontab

#!/bin/bash

#DAY=`date +%Y-%m-%d`
#HOUR=`date +%H`

DAY=`date -d '1 hour ago' +%Y-%m-%d`

/build/scripts/es-dump/dumb-inf.sh $DAY




// load//////////////////////////////


#!/bin/bash

DAY=$1

ROOT_PATH=/build/scripts/es-dump

INDEX="radius_load-${DAY}"

/bin/es2csv -i "${INDEX}" -u 172.27.11.156  -r -q '{"query": {"match_all": {}}}' -o "${ROOT_PATH}/${INDEX}.csv"

mkdir /logs-data/share/radius-load-log-realtime-parse/load-log-$(date +%m)

mv "${ROOT_PATH}/${INDEX}.csv" /logs-data/share/load-log-realtime-parse/load-log-$(date +%m)


// crontab

#!/bin/bash

#DAY=`date +%Y-%m-%d`
#HOUR=`date +%H`

DAY=`date -d '1 hour ago' +%Y-%m-%d`

/build/scripts/es-dump/dumb-load.sh $DAY