#! /bin/bash
java -XX:+AggressiveOpts -XX:+UseG1GC -XX:+UseStringDeduplication -Xmx512m -jar eventsim-assembly-2.0.jar $*
