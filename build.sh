 #!/bin/bash
 
 # One option argument is version number.
 # When version needs to change, please provide version in n.n.n.n format. e.g.
 # ./build.sh 2.3.4.5

 check_version()
 {
     MAJOR=$(echo $FULL_VERSION | cut -d'.' -f1)
     MINOR=$(echo $FULL_VERSION | cut -d'.' -f2)
     PATCH=$(echo $FULL_VERSION | cut -d'.' -f3) 
     RELEASE=$(echo $FULL_VERSION | cut -d'.' -f4)
     EXTRA=$(echo $FULL_VERSION | cut -d'.' -f5)

     if [ "$MAJOR" == "" ] || [ "$MINOR" == "" ] || [ "$PATCH" == "" ] || [ "$RELEASE" == "" ] || [ "$EXTRA" != "" ];
     then
         echo Invalid version format: $FULL_VERSION. please give it in n.n.n.n format.
         exit 1
     fi
 }

 if [ "$1" != "" ];
 then
    # check version
    FULL_VERSION=$1
    check_version 
    mvn versions:set -DartifactId=redshift-jdbc42 -DnewVersion=$1
    mvn versions:commit
 fi

mvn -P release-artifacts clean install
