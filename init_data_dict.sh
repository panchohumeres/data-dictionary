#!/bin/bash

while getopts m: option
do 
 case "${option}" 
 in 
 m) mode_param="${OPTARG}";;
 esac 
done 

echo "mode: ""${mode_param}"


file_name="output.ipynb"
current_time=$(date "+%Y.%m.%d-%H.%M.%S")
new_fileName="${current_time}${file_name}"

papermill Data_dictionary.ipynb outputs/${new_fileName} -p mode "${mode_param}"

sphinx-build -b html source build