#!/bin/sh

dir=$(cd -P -- "$(dirname -- "$0")" && pwd -P)

echo "Executing in $dir with args $1 $2 $3"
echo "Running java -cp $dir/scala-cookbook-0.0.1-SNAPSHOT-shaded.jar io.criticality.app.LoadGenRunner \
-f $1 -r $2 -d $3"

java -cp $dir/scala-cookbook-0.0.1-SNAPSHOT-shaded.jar io.criticality.app.LoadGenRunner  \
-f $1 -r $2 -d $3