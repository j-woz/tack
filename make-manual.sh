#!/bin/sh

THIS=$( cd $( dirname $0 ) ; /bin/pwd )

asciidoc --attribute stylesheet=${THIS}/manual.css \
         --attribute max-width=800px               \
         -o index.html                             \
         manual.txt
