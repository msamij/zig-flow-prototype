#!/bin/bash

# Run Maven package
mvn package

# fully qualified class name of main java class.
class_name="com.msamiaj.Main"
# Replace this with the path to your jar file located in target folder (must be an absolute path).
jar_path="/home/muhammadsami/Public/MuhammadSami/Projects/Java/FYP/Zig flow prototype/zigflowprototype/target/zigflowprototype-1.0-SNAPSHOT.jar"

# Check if the Maven build was successful
if [ $? -eq 0 ]; then
    # Run spark-submit with the specified class and JAR file
    spark-submit --packages mysql:mysql-connector-java:8.0.33 --class "$class_name" "$jar_path" exportdata
else
    echo "Maven package failed. Exiting..."
    exit 1
fi
